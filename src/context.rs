use std::thread::JoinHandle;

use zmq::{Context, SocketType};

use crate::{poll_thread::{Handle, PollThread}, AsyncSocket, Result};

pub struct AsyncContext {
    inner: Context,
    handle: Handle,
    poll_thread: JoinHandle<Result<()>>,
}

impl AsyncContext {
    pub fn new() -> Result<Self> {
        let context = Context::new();
        let (poll_thread, handle) = PollThread::new(&context, 1024, "inproc://__azmq_poll_thread_#1")?;
        let poll_thread = poll_thread.start();
        Ok(Self {
            inner: context,
            handle,
            poll_thread,
        })
    }

    pub fn get_io_threads(&self) -> Result<i32> {
        self.inner.get_io_threads()
    }

    pub fn set_io_threads(&self, value: i32) -> Result<()> {
        self.inner.set_io_threads(value)
    }

    // TODO: return `AsyncSocket`
    pub fn socket(&self, socket_type: SocketType) -> Result<AsyncSocket> {
        let socket = self.inner.socket(socket_type)?;
        self.handle.insert(socket)
    }

    pub fn shutdown(mut self) -> Result<()> {
        self.inner.destroy()?;
        self.poll_thread.join().expect("Internal thread shouldn't panic")
    }
}
