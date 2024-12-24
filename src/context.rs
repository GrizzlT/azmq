use zmq::{Context, Socket, SocketType};

use crate::Result;

pub struct AsyncContext {
    inner: Context,
}

impl AsyncContext {
    pub fn new() -> Self {
        Self {
            inner: Context::new(),
        }
    }

    pub fn get_io_threads(&self) -> Result<i32> {
        self.inner.get_io_threads()
    }

    pub fn set_io_threads(&self, value: i32) -> Result<()> {
        // TODO: adjust slab size
        self.inner.set_io_threads(value)
    }

    // TODO: return `AsyncSocket`
    pub fn socket(&self, socket_type: SocketType) -> Result<Socket> {
        // TODO: register socket to poller
        self.inner.socket(socket_type)
    }

    pub fn destroy(&mut self) -> Result<()> {
        self.inner.destroy()
    }
}

impl Default for AsyncContext {
    fn default() -> Self {
        Self::new()
    }
}
