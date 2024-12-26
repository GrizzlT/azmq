use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};

use zmq::{Context, PollEvents, PollItem, Socket};
use event_listener::Event;

use crate::socket_slab::PollSlab;
use crate::{AsyncSocket, Result};

/// Schedule the polling thread to park
pub(crate) const INTR_CMD: u8 = 0;
/// Schedule the polling thread to unpark
pub(crate) const POLL_CMD: u8 = 1;
/// Release a permit on event handling
pub(crate) const TASK_DONE_CMD: u8 = 2;

#[derive(Clone)]
pub struct Handle {
    /// Used to send signals to the polling thread
    waker: Arc<Mutex<Socket>>,
    /// Internal slab structure containing polling items
    pub(crate) sockets: Arc<Mutex<PollSlab>>,
    /// Used to listen for polling updates
    notify: Arc<Event>,
    /// How long to keep the polling thread paused
    parking_tickets: Arc<AtomicUsize>,
}

pub(crate) struct HandleGuard<'a> {
    /// Internal mutex guard
    lock: Option<MutexGuard<'a, PollSlab>>,
    /// Used to send signals to the polling thread
    waker: &'a Mutex<Socket>,
}

impl Drop for HandleGuard<'_> {
    fn drop(&mut self) {
        std::mem::drop(self.lock.take());
        // the waker is only used to wake up the polling thread
        // -> no lock contention
        let waker = self.waker.lock().unwrap();
        // signal polling thread to unpark
        waker.send(&[POLL_CMD][..], 0).ok();
    }
}

impl Deref for HandleGuard<'_> {
    type Target = PollSlab;

    fn deref(&self) -> &Self::Target {
        self.lock.as_ref().unwrap().deref()
    }
}

impl DerefMut for HandleGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.lock.as_mut().unwrap().deref_mut()
    }
}

impl Handle {
    pub fn insert(&self, socket: Socket) -> Result<AsyncSocket> {
        let mut sockets = self.park()?;
        let (key, readiness) = sockets.insert(&socket);
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
        Ok(())
    }

    /// Parks the polling thread and acquires the mutex on the internal
    /// polling data structure.
    pub fn park(&self) -> Result<HandleGuard<'_>> {
        // The waker is only used to wake up the polling thread
        {
            let waker = self.waker.lock().unwrap();
            // atomic boolean needs to be incremented before notifying the
            // polling thread -> we use Acquire to be safe
            //
            // no memory is being synced by this atomic boolean however (I think)
            self.parking_tickets.fetch_add(1usize, Ordering::Acquire);
            waker.send(&[INTR_CMD][..], 0)?;
        }
        Ok(HandleGuard {
            // wait for the polling thread to park
            lock: Some(self.sockets.lock().unwrap()),
            waker: &self.waker,
        })
    }

    /// signal event handling completion
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
    /// Receives signals on the polling thread
    waker: Socket,
    /// Internal slab structure containing polling items
    sockets: Arc<Mutex<PollSlab>>,
    /// Used to listen for polling updates
    notify: Arc<Event>,
    /// How long to keep the polling thread paused
    parking_tickets: Arc<AtomicUsize>,
}

impl PollThread {
    pub fn new(context: &Context, capacity: usize, pair_name: &str) -> Result<(Self, Handle)> {
        // We're using inproc sockets for polling interrupts + signals

        let pair1 = context.socket(zmq::SocketType::PAIR)?;
        // avoid indefinite waiting on termination
        pair1.set_linger(0)?;
        pair1.bind(pair_name)?;
        let pair2 = context.socket(zmq::SocketType::PAIR)?;
        // avoid indefinite waiting on termination
        pair2.set_linger(0)?;
        pair2.connect(pair_name)?;
        // We only ever want to listen for signals
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
            // restart polling thread on unknown errors
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
            // Wait for zmq events
            // This is a level-triggered operation -> no interrupt will be lost
            let poll_result: Result<(bool, i32)> = (|| {
                // Acquire the lock on the sockets structure
                let mut lock = self.sockets.lock().unwrap(); // poisoning is impossible, the mutex is only used inside this crate
                let count = zmq::poll(lock.poll_items(), -1)?;
                let (unparked, count) = if lock.poll_items()[0].get_revents().is_empty() { (false, count) } else { (true, count - 1) };
                if count > 0 {
                    lock.mark_changed();
                    // this is within the mutex guard
                    // this makes cancelling the async operation sound
                    // the polling thread cannot be stalled because of this
                }
                Ok((unparked, count))
            })();
            match poll_result {
                Err(zmq::Error::ETERM) => return Err(zmq::Error::ETERM),
                Ok((mut unparked, mut count)) => {
                    if count > 0 {
                        // notify if there were events
                        self.notify.notify(usize::MAX);
                        while count != 0 {
                            // Handle all possible cases
                            self.waker.recv_into(std::slice::from_mut(&mut command), 0)?;
                            if command == TASK_DONE_CMD {
                                // Event handling completed
                                count -= 1;
                            } else if command == INTR_CMD {
                                // Interrupt scheduled -> parking tickets are already incremented
                                // Ensure thread stays parked
                                unparked = true;
                            } else if command == POLL_CMD && self.parking_tickets.fetch_sub(1, Ordering::Release) == 1 {
                                // Check if there are no more parking tickets after decrementing
                                // Use release to be safe -> but I think no memory is synced by
                                // this AtomicUsize.
                                unparked = false;
                            }
                        }
                    }
                    // wait before unparking the thread
                    if unparked {
                        loop {
                            self.waker.recv_into(std::slice::from_mut(&mut command), 0)?;
                            // event completion operations are impossible at this point
                            // interrupt signals can be safely ignored, the thread is already parked
                            if command == POLL_CMD && self.parking_tickets.fetch_sub(1, Ordering::Release) == 1 {
                                // Check if there are no more parking tickets after decrementing
                                // Use release to be safe -> but I think no memory is synced by
                                // this AtomicUsize.
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
