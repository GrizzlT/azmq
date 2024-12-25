use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::{atomic::AtomicBool, Arc};
use std::task::{ready, Context, Poll};

use event_listener::EventListener;
use pin_project_lite::pin_project;
use zmq::{PollEvents, Socket};

use crate::poll_thread::Handle;
use crate::{Multipart, Result};

pub struct AsyncSocket {
    pub(crate) inner: Socket,
    pub(crate) key: usize,
    pub(crate) readiness: Arc<AtomicBool>,
    pub(crate) handle: Handle,
}

impl AsyncSocket {
    pub async fn recv(&mut self) -> Result<Multipart> {
        let recv = {
            // Fast path
            let mut sockets = self.handle.park()?;
            let mut multipart = Multipart::new();
            match self.inner.recv_msg(zmq::DONTWAIT) {
                Ok(msg) => {
                    multipart.push_msg(msg.into());
                    while self.inner.get_rcvmore()? {
                        multipart.push_msg(self.inner.recv_msg(zmq::DONTWAIT)?.into());
                    }
                    self.handle.unpark(sockets)?;
                    return Ok(multipart)
                },
                Err(zmq::Error::EAGAIN) => {}
                Err(error) => {
                    self.handle.unpark(sockets)?;
                    return Err(error)
                },
            }
            sockets.register_interest(self.key, PollEvents::POLLIN);
            self.handle.unpark(sockets)?;
            let event = self.handle.event().listen();
            Recv {
                inner: self,
                event,
            }
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
            if let Ok(mut lock) = this.inner.handle.park() {
                lock.remove(this.inner.key);
                if this.inner.readiness.load(Ordering::Relaxed) {
                    this.inner.handle.task_done().ok();
                }
                this.inner.handle.unpark(lock).ok();
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
                    let mut sockets = this.inner.handle.sockets.lock().unwrap();
                    let mut multipart = Multipart::new();
                    match this.inner.inner.recv_msg(zmq::DONTWAIT) {
                        Ok(msg) => {
                            multipart.push_msg(msg.into());
                            while this.inner.inner.get_rcvmore()? {
                                multipart.push_msg(this.inner.inner.recv_msg(zmq::DONTWAIT)?.into());
                            }
                            sockets.deregister(this.inner.key);
                            Ok(Some(multipart))
                        }
                        Err(zmq::Error::EAGAIN) => Ok(None),
                        Err(error) => Err(error),
                    }
                })();

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
