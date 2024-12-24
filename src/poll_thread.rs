use zmq::{Context, Socket};

use crate::socket_slab::PollSlab;

pub struct PollThread {
    waker: Socket,
    sockets: PollSlab,
}

impl PollThread {
    pub fn new(context: &Context, capacity: usize) -> (Self, Socket) {
        // (Self {
        //     sockets: PollSlab::with_capacity(capacity),
        //     waker: todo!(),
        // }, todo!())
        todo!()
    }

    // TODO: return socket handle
    pub fn register(&mut self, socket: Socket) -> usize {
        // self.sockets.insert(socket)
        todo!()
    }
}
