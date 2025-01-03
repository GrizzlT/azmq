use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::{atomic::AtomicBool, Arc};
use std::task::{ready, Context, Poll};

use event_listener::EventListener;
use pin_project_lite::pin_project;
use zmq::{PollEvents, Socket};

use crate::message::Sendable;
use crate::poll_thread::Handle;
use crate::{Multipart, Result};

pub struct AsyncSocket {
    pub(crate) inner: Socket,
    pub(crate) key: usize,
    pub(crate) readiness: Arc<AtomicBool>,
    pub(crate) handle: Handle,
}

impl AsyncSocket {
    pub fn blocking_send<T: Sendable>(&mut self, message: T) -> Result<()> {
        message.send(&self.inner, 0)
    }

    pub fn blocking_recv(&self) -> Result<Multipart> {
        let message = self.inner.recv_msg(0)?;
        let mut multipart = Multipart::new();
        multipart.push_msg(message.into());
        while self.inner.get_rcvmore()? {
            multipart.push_msg(self.inner.recv_msg(0)?.into());
        }
        Ok(multipart)
    }

    /// # Safety
    ///
    /// This is one of the few calls that breaks the [`Send`] property of
    /// zmq [`Socket`]s. We require a mutable borrow here so that we are sure
    /// nothing else can interfere with this socket while we're polling for a
    /// message.
    pub async fn recv(&mut self) -> Result<Multipart> {
        // Fast path
        //
        // No lock is required here, the mutable borrow guarantees uniqueness of this
        // scenario for this socket + not polling yet means it is not registered in
        // the polling thread and thus not accessed outside of this type.
        //
        // This means it is safe to access the socket here.
        let mut multipart = Multipart::new();
        match self.inner.recv_msg(zmq::DONTWAIT) {
            Ok(msg) => {
                multipart.push_msg(msg.into());
                while self.inner.get_rcvmore()? {
                    multipart.push_msg(self.inner.recv_msg(zmq::DONTWAIT)?.into());
                }
                return Ok(multipart)
            },
            Err(zmq::Error::EAGAIN) => {}
            Err(error) => {
                return Err(error)
            },
        }

        // Slow path
        let event = {
            // Now we do need the lock to access the polling structure
            let mut sockets = self.handle.park()?;
            sockets.register_interest(self.key, PollEvents::POLLIN);
            self.handle.event().listen()
        };
        let recv = Recv {
            inner: self,
            event,
        };
        // Start polling
        recv.await
    }
}

pin_project! {
    struct Recv<'a> {
        inner: &'a mut AsyncSocket,
        #[pin]
        event: EventListener,
    }

    impl<'a> PinnedDrop for Recv<'a> {
        fn drop(this: Pin<&mut Self>) {
            let this = this.project();
            // parking is necessary to remove the deregister the socket
            if let Ok(mut lock) = this.inner.handle.park() {
                lock.deregister(this.inner.key);
                // after the mutex is acquired, this atomic boolean will have been updated
                if this.inner.readiness.swap(false, Ordering::Relaxed) {
                    this.inner.handle.task_done().ok();
                }
            }
        }
    }
}

impl Future for Recv<'_> {
    type Output = Result<Multipart>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        loop {
            ready!(this.event.as_mut().poll(cx));

            if this.inner.readiness.swap(false, Ordering::Relaxed) {
                let result: Result<Option<Multipart>> = (|| {
                    // no lock needs to be acquired yet since the event ensures
                    // the polling thread is not polling the sockets
                    let mut multipart = Multipart::new();
                    match this.inner.inner.recv_msg(zmq::DONTWAIT) {
                        Ok(msg) => {
                            multipart.push_msg(msg.into());
                            while this.inner.inner.get_rcvmore()? {
                                multipart.push_msg(this.inner.inner.recv_msg(zmq::DONTWAIT)?.into());
                            }
                            // acquire the lock here to satisfy the rust type system
                            // This should have little contention, only bookkeeping
                            let mut sockets = this.inner.handle.sockets.lock().unwrap();
                            sockets.deregister(this.inner.key);
                            Ok(Some(multipart))
                        }
                        Err(zmq::Error::EAGAIN) => Ok(None),
                        Err(error) => {
                            Err(error)
                        },
                    }
                })();

                // make sure the polling thread is released
                this.inner.handle.task_done()?;
                match result {
                    Ok(Some(multipart)) => {
                        return Poll::Ready(Ok(multipart));
                    }
                    Ok(None) => {}
                    Err(error) => {
                        return Poll::Ready(Err(error));
                    }
                }
            }
            this.event.as_mut().set(this.inner.handle.event().listen());
        }
    }
}

impl Drop for AsyncSocket {
    fn drop(&mut self) {
        if self.inner.get_linger().unwrap_or(-1) == -1 {
            self.inner.set_linger(100).ok();
        }
        self.handle.remove(self.key).ok();
    }
}
