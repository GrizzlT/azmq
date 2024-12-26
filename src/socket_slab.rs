use std::sync::{atomic::AtomicBool, Arc};

use zmq::{PollEvents, PollItem, Socket};

pub struct PollSlab {
    poll_items: Vec<PollItem<'static>>, // Lists all the sockets that are created
    keys: Vec<Entry<usize>>, // Keys to poll_item index
    entry_idx: Vec<(usize, Option<Arc<AtomicBool>>)>, // Mapping from PollItem to key entry in `keys` slab
    scheduled: usize,
    next: usize,
}

unsafe impl Send for PollSlab {}

enum Entry<T> {
    Vacant(usize),
    Occupied(T),
}

impl Entry<usize> {
    pub fn set(&mut self, val: usize) {
        match self {
            Entry::Vacant(ref mut c) => *c = val,
            Entry::Occupied(ref mut c) => *c = val,
        }
    }
}

impl PollSlab {
    #[cfg(test)]
    pub fn new(waker: PollItem<'static>) -> Self {
        Self::with_capacity(0, waker)
    }

    pub fn with_capacity(capacity: usize, waker: PollItem<'static>) -> Self {
        // 1 more capacity for poll waker
        let mut vec = Vec::with_capacity(capacity + 1);
        vec.push(waker);
        let mut entry_idx = Vec::with_capacity(capacity + 1);
        // if usize::MAX gets reached there will be a few more problems ;)
        entry_idx.push((usize::MAX, None));
        Self {
            poll_items: vec,
            keys: Vec::with_capacity(capacity),
            entry_idx,
            scheduled: 1,
            next: 0,
        }
    }

    #[cfg(test)]
    pub fn capacity(&self) -> usize {
        self.poll_items.capacity()
    }

    pub fn len(&self) -> usize {
        self.poll_items.len() - 1
    }

    #[cfg(test)]
    fn memory_in_use(&self) -> usize {
        self.keys.len()
    }

    // pub fn clear(&mut self) {
    //     self.poll_items.clear();
    //     self.keys.clear();
    //     self.entry_idx.clear();
    //     self.next = 0;
    // }

    #[cfg(test)]
    fn get(&self, socket: usize) -> Option<&PollItem<'static>> {
        match self.keys.get(socket) {
            Some(&Entry::Occupied(key)) => Some(&self.poll_items[key]),
            _ => None,
        }
    }

    pub(crate) fn mark_changed(&self) {
        for (i, item) in self.poll_items.iter().skip(1).enumerate() {
            if !item.get_revents().is_empty() {
                self.notify_socket_internal(i);
            }
        }
    }

    pub(crate) fn notify_socket_internal(&self, idx: usize) {
        // No memory is synced by this atomic boolean
        //
        // unwrap() is safe because insert() never creates a `None`
        self.entry_idx[idx].1.as_ref().unwrap().store(true, std::sync::atomic::Ordering::Relaxed);
    }

    /// # Safety
    /// This call essentially makes [`Socket`] no longer [`Send`].
    /// **USE WITH CAUTION**
    ///
    /// The intended use is to synchronize polling with immediate socket
    /// reading/writing. This restores the [`Send`] functionality **manually**.
    ///
    pub(crate) fn insert(&mut self, socket: &Socket) -> (usize, Arc<AtomicBool>) {
        let key = self.next;
        let poll_item: PollItem<'static> = unsafe { std::mem::transmute(socket.as_poll_item(PollEvents::empty())) };
        // The returned key will never be 0
        (key, self.insert_at(key, poll_item))
    }

    fn insert_at(&mut self, key: usize, poll_item: PollItem<'static>) -> Arc<AtomicBool> {
        let boolean = Arc::new(AtomicBool::new(false));
        if key == self.keys.len() {
            self.keys.push(Entry::Occupied(self.len()+1));
            self.poll_items.push(poll_item);
            self.entry_idx.push((key, Some(boolean.clone())));
            self.next = key + 1;
        } else {
            self.next = match self.keys.get(key) {
                Some(&Entry::Vacant(next)) => next,
                _ => unreachable!(),
            };
            self.keys[key] = Entry::Occupied(self.len()+1);
            self.poll_items.push(poll_item);
            self.entry_idx.push((key, Some(boolean.clone())));
        }
        boolean
    }

    // Swap values internally, key mappings stay the same
    fn swap_items(&mut self, entry_id1: usize, entry_id2: usize) {
        let (key1, _) = self.entry_idx[entry_id1];
        let (key2, _) = self.entry_idx[entry_id2];
        self.keys[key1].set(entry_id2);
        self.keys[key2].set(entry_id1);
        self.entry_idx.swap(entry_id1, entry_id2);
        self.poll_items.swap(entry_id1, entry_id2);
    }

    pub(crate) fn remove(&mut self, key: usize) {
        if let Some(entry) = self.keys.get_mut(key) {
            // Swap the entry at the provided value
            let prev = std::mem::replace(entry, Entry::Vacant(self.next));

            match prev {
                Entry::Occupied(idx) => {
                    self.next = key;
                    self.swap_items(idx, self.len());
                    self.poll_items.remove(self.len());
                    self.entry_idx.remove(self.len());
                }
                _ => {
                    // Woops, the entry is actually vacant, restore the state
                    *entry = prev;
                }
            }
        }
    }

    /// Do not use this function to deregister a socket. Empty events are ignored.
    pub fn register_interest(&mut self, socket: usize, events: PollEvents) {
        if events.is_empty() {
            return;
        }

        if let Some(&Entry::Occupied(key)) = self.keys.get(socket) {
            self.poll_items[key].set_events(events);
            if key >= self.scheduled {
                self.swap_items(key, self.scheduled);
                self.scheduled += 1;
            }
        }
    }

    pub fn deregister(&mut self, socket: usize) {
        if socket < self.scheduled {
            self.swap_items(socket, self.scheduled-1);
            self.scheduled -= 1;
        }
    }

    /// # Safety
    /// **ONLY USE THIS IN THE POLLING THREAD**
    ///
    /// You will break everything if you access this without proper synchronization
    /// Just don't touch it, please.
    pub fn poll_items(&mut self) -> &mut [PollItem<'static>] {
        &mut self.poll_items[0..self.scheduled]
    }
}

#[cfg(test)]
mod tests {
    use zmq::Context;

    use super::*;

    #[test]
    fn simple_insert() {
        let ctx = Context::new();
        ctx.set_io_threads(0).unwrap();
        let socket1 = ctx.socket(zmq::SocketType::PAIR).unwrap();
        let socket2 = ctx.socket(zmq::SocketType::PAIR).unwrap();
        let control = ctx.socket(zmq::SocketType::PAIR).unwrap();

        let mut slab = PollSlab::new(unsafe { std::mem::transmute::<zmq::PollItem<'_>, zmq::PollItem<'static>>(control.as_poll_item(PollEvents::POLLIN)) });
        let key = slab.insert(&socket1).0;
        assert!(slab.get(key).unwrap().has_socket(&socket1));
        assert_eq!(0, key);
        let key = slab.insert(&socket2).0;
        assert!(slab.get(key).unwrap().has_socket(&socket2));
        assert_eq!(1, key);
        assert_eq!(2, slab.len());
        assert_eq!(2, slab.memory_in_use());
        assert!(slab.capacity() >= 2);
        assert_eq!(1, slab.poll_items().len());
    }

    #[test]
    fn re_insert() {
        let ctx = Context::new();
        ctx.set_io_threads(0).unwrap();
        let socket1 = ctx.socket(zmq::SocketType::PAIR).unwrap();
        let socket2 = ctx.socket(zmq::SocketType::PAIR).unwrap();
        let control = ctx.socket(zmq::SocketType::PAIR).unwrap();

        let mut slab = PollSlab::new(unsafe { std::mem::transmute::<zmq::PollItem<'_>, zmq::PollItem<'static>>(control.as_poll_item(PollEvents::POLLIN)) });
        let key1 = slab.insert(&socket1).0;
        let key2 = slab.insert(&socket2).0;

        slab.remove(key1);
        assert_eq!(1, slab.len());
        assert_eq!(2, slab.memory_in_use());
        let key = slab.insert(&socket1).0;
        assert!(slab.get(key).unwrap().has_socket(&socket1));
        assert!(slab.get(key2).unwrap().has_socket(&socket2));
        assert_eq!(0, key);
        assert_eq!(2, slab.len());
        assert_eq!(2, slab.memory_in_use());
    }

    #[test]
    fn swap() {
        let ctx = Context::new();
        ctx.set_io_threads(0).unwrap();
        let socket1 = ctx.socket(zmq::SocketType::PAIR).unwrap();
        let socket2 = ctx.socket(zmq::SocketType::PAIR).unwrap();
        let control = ctx.socket(zmq::SocketType::PAIR).unwrap();

        let mut slab = PollSlab::new(unsafe { std::mem::transmute::<zmq::PollItem<'_>, zmq::PollItem<'static>>(control.as_poll_item(PollEvents::POLLIN)) });
        let key1 = slab.insert(&socket1).0;
        let key2 = slab.insert(&socket2).0;

        slab.swap_items(1, 2);

        assert!(slab.get(key1).unwrap().has_socket(&socket1));
        assert!(slab.get(key2).unwrap().has_socket(&socket2));
    }

    #[test]
    fn register() {
        let ctx = Context::new();
        ctx.set_io_threads(0).unwrap();
        let socket1 = ctx.socket(zmq::SocketType::PAIR).unwrap();
        let socket2 = ctx.socket(zmq::SocketType::PAIR).unwrap();
        let control = ctx.socket(zmq::SocketType::PAIR).unwrap();

        let mut slab = PollSlab::new(unsafe { std::mem::transmute::<zmq::PollItem<'_>, zmq::PollItem<'static>>(control.as_poll_item(PollEvents::POLLIN)) });
        let key1 = slab.insert(&socket1).0;
        let key2 = slab.insert(&socket2).0;

        slab.register_interest(key2, PollEvents::POLLIN);
        assert!(slab.get(key1).unwrap().has_socket(&socket1));
        assert!(slab.get(key2).unwrap().has_socket(&socket2));
        let poll_items = slab.poll_items();
        assert!(poll_items[1].has_socket(&socket2));
        assert_eq!(2, poll_items.len());

        slab.register_interest(key1, PollEvents::POLLIN);
        assert!(slab.get(key1).unwrap().has_socket(&socket1));
        assert!(slab.get(key2).unwrap().has_socket(&socket2));
        let poll_items = slab.poll_items();
        assert!(poll_items[2].has_socket(&socket1));
        assert_eq!(3, poll_items.len());
    }

    #[test]
    fn deregister() {
        let ctx = Context::new();
        ctx.set_io_threads(0).unwrap();
        let socket1 = ctx.socket(zmq::SocketType::PAIR).unwrap();
        let socket2 = ctx.socket(zmq::SocketType::PAIR).unwrap();
        let control = ctx.socket(zmq::SocketType::PAIR).unwrap();

        let mut slab = PollSlab::new(unsafe { std::mem::transmute::<zmq::PollItem<'_>, zmq::PollItem<'static>>(control.as_poll_item(PollEvents::POLLIN)) });
        let key1 = slab.insert(&socket1).0;
        let key2 = slab.insert(&socket2).0;

        slab.register_interest(key2, PollEvents::POLLIN);
        slab.register_interest(key1, PollEvents::POLLIN);
        slab.deregister(key2);
        assert!(slab.get(key1).unwrap().has_socket(&socket1));
        assert!(slab.get(key2).unwrap().has_socket(&socket2));
        let poll_items = slab.poll_items();
        assert!(poll_items[1].has_socket(&socket1));
        assert_eq!(2, poll_items.len());
    }
}
