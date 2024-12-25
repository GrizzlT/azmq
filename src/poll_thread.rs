use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};

use zmq::{Context, PollEvents, PollItem, Socket};
use event_listener::Event;

use crate::socket_slab::PollSlab;
use crate::{AsyncSocket, Result};

/// Interrupt the polling thread
pub(crate) const INTR_CMD: u8 = 0;
/// Release the lock on the polling thread
pub(crate) const POLL_CMD: u8 = 1;
/// Release the lock on event handling
pub(crate) const TASK_DONE_CMD: u8 = 2;

#[derive(Clone)]
pub struct Handle {
    /// independent => is Send
    waker: Arc<Mutex<Socket>>,
    pub(crate) sockets: Arc<Mutex<PollSlab>>,
    notify: Arc<Event>,
    parking_tickets: Arc<AtomicUsize>,
}

impl Handle {
    pub fn insert(&self, socket: Socket) -> Result<AsyncSocket> {
        let mut sockets = self.park()?;
        let (key, readiness) = sockets.insert(&socket);
        self.unpark(sockets)?;
        Ok(AsyncSocket {
            inner: socket,
            key,
            readiness,
            handle: self.clone(),
        })
    }

    pub fn remove(&self, socket: usize) -> Result<()> {
        let mut sockets = self.park()?;
        sockets.remove(socket);
        self.unpark(sockets)
    }

    pub fn park(&self) -> Result<MutexGuard<'_, PollSlab>> {
        let waker = self.waker.lock().unwrap();
        waker.send(&[INTR_CMD][..], 0)?;
        self.parking_tickets.fetch_add(1usize, Ordering::Release);
        Ok(self.sockets.lock().unwrap())
    }

    pub fn unpark<'a>(&'a self, guard: MutexGuard<'a, PollSlab>) -> Result<()> {
        std::mem::drop(guard);
        let waker = self.waker.lock().unwrap();
        waker.send(&[POLL_CMD][..], 0)
    }

    pub fn task_done(&self) -> Result<()> {
        let waker = self.waker.lock().unwrap();
        waker.send(&[TASK_DONE_CMD][..], 0)
    }

    pub fn event(&self) -> &Event {
        &self.notify
    }
}

pub struct PollThread {
    /// dependency in `sockets` field => is **not** Send
    waker: Socket,
    sockets: Arc<Mutex<PollSlab>>,
    notify: Arc<Event>,
    parking_tickets: Arc<AtomicUsize>,
}

impl PollThread {
    pub fn new(context: &Context, capacity: usize, pair_name: &str) -> Result<(Self, Handle)> {
        let pair1 = context.socket(zmq::SocketType::PAIR)?;
        pair1.set_linger(0)?;
        pair1.bind(pair_name)?;
        let pair2 = context.socket(zmq::SocketType::PAIR)?;
        pair2.set_linger(0)?;
        pair2.connect(pair_name)?;
        let waker_poll_item: PollItem<'static> = unsafe { std::mem::transmute(pair1.as_poll_item(PollEvents::POLLIN)) };

        let sockets = Arc::new(Mutex::new(PollSlab::with_capacity(capacity, waker_poll_item)));
        let notify = Arc::new(Event::new());
        let parking_tickets = Arc::new(AtomicUsize::new(0));

        Ok((Self {
            sockets: sockets.clone(),
            waker: pair1,
            notify: notify.clone(),
            parking_tickets: parking_tickets.clone(),
        }, Handle {
            waker: Arc::new(Mutex::new(pair2)),
            sockets,
            notify,
            parking_tickets,
        }))
    }

    pub fn start(mut self) -> JoinHandle<Result<()>> {
        thread::spawn(move || {
            loop {
                if matches!(self.run(), Err(zmq::Error::ETERM)) {
                    return Err(zmq::Error::ETERM)
                }
            }
        })
    }

    fn run(&mut self) -> Result<()> {
        let mut command = POLL_CMD;
        loop {
            let poll_result: Result<(bool, i32)> = {
                let mut lock = self.sockets.lock().unwrap(); // poisoning is impossible, the mutex is only used inside this crate
                let count = zmq::poll(lock.poll_items(), -1)?;
                let (unparked, count) = if lock.poll_items()[0].get_revents().is_empty() { (false, count) } else { (true, count - 1) };
                if count > 0 {
                    lock.mark_changed();
                }
                Ok((unparked, count))
            };
            match poll_result {
                Err(zmq::Error::ETERM) => return Err(zmq::Error::ETERM),
                Ok((mut unparked, mut count)) => {
                    if count > 0 {
                        self.notify.notify(usize::MAX);
                        while count != 0 {
                            self.waker.recv_into(std::slice::from_mut(&mut command), 0)?;
                            if command == TASK_DONE_CMD {
                                count -= 1;
                            } else if command == INTR_CMD {
                                unparked = true;
                            } else if command == POLL_CMD && self.parking_tickets.fetch_sub(1, Ordering::Acquire) == 1 {
                                unparked = false;
                            }
                        }
                    }
                    if unparked {
                        loop {
                            self.waker.recv_into(std::slice::from_mut(&mut command), 0)?;
                            if command == POLL_CMD && self.parking_tickets.fetch_sub(1, Ordering::Acquire) == 1 {
                                break
                            }
                        }
                    }
                }
                _ => unreachable!(),
            }
        }
    }
}
