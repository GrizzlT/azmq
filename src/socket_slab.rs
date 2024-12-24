use zmq::{PollEvents, PollItem, Socket};

pub struct PollSlab {
    poll_items: Vec<PollItem<'static>>, // Lists all the sockets that are created
    keys: Vec<Entry<usize>>, // Keys to poll_item index
    entry_idx: Vec<usize>, // Mapping from PollItem to key entry in `entries` slab
    scheduled: usize,
    next: usize,
}

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
    pub fn new(waker: PollItem<'static>) -> Self {
        Self {
            poll_items: vec![waker],
            keys: Vec::new(),
            entry_idx: vec![usize::MAX],
            scheduled: 1,
            next: 0,
        }
    }

    pub fn with_capacity(capacity: usize, waker: PollItem<'static>) -> Self {
        let mut vec = Vec::with_capacity(capacity + 1);
        vec.push(waker);
        let mut entry_idx = Vec::with_capacity(capacity + 1);
        entry_idx.push(usize::MAX);
        Self {
            poll_items: vec,
            keys: Vec::with_capacity(capacity),
            entry_idx,
            scheduled: 1,
            next: 0,
        }
    }

    pub fn capacity(&self) -> usize {
        self.poll_items.capacity()
    }

    pub fn len(&self) -> usize {
        self.poll_items.len()
    }

    fn memory_in_use(&self) -> usize {
        self.keys.len()
    }

    pub fn clear(&mut self) {
        self.poll_items.clear();
        self.keys.clear();
        self.entry_idx.clear();
        self.next = 0;
    }

    #[cfg(test)]
    fn get(&self, socket: usize) -> Option<&PollItem<'static>> {
        match self.keys.get(socket) {
            Some(&Entry::Occupied(key)) => Some(&self.poll_items[key]),
            _ => None,
        }
    }

    /// # Safety
    /// This call essentially makes [`Socket`] no longer [`Send`].
    /// **USE WITH CAUTION**
    ///
    /// The intended use is to synchronize polling with immediate socket
    /// reading/writing. This restores the [`Send`] functionality **manually**.
    pub(crate) fn insert(&mut self, socket: &Socket) -> usize {
        let key = self.next;
        let poll_item: PollItem<'static> = unsafe { std::mem::transmute(socket.as_poll_item(PollEvents::empty())) };
        self.insert_at(key, poll_item);
        key
    }

    fn insert_at(&mut self, key: usize, poll_item: PollItem<'static>) {
        if key == self.keys.len() {
            self.keys.push(Entry::Occupied(self.len()));
            self.poll_items.push(poll_item);
            self.entry_idx.push(key);
            self.next = key + 1;
        } else {
            self.next = match self.keys.get(key) {
                Some(&Entry::Vacant(next)) => next,
                _ => unreachable!(),
            };
            self.keys[key] = Entry::Occupied(self.len());
            self.poll_items.push(poll_item);
            self.entry_idx.push(key);
        }
    }

    fn swap_items(&mut self, entry_id1: usize, entry_id2: usize) {
        let key1 = self.entry_idx[entry_id1];
        let key2 = self.entry_idx[entry_id2];
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
                    self.swap_items(idx, self.len()-1);
                    self.poll_items.remove(self.len()-1);
                    self.entry_idx.remove(self.len()-1);
                }
                _ => {
                    // Woops, the entry is actually vacant, restore the state
                    *entry = prev;
                }
            }
        }
    }

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

    pub fn poll_items(&mut self) -> &mut [PollItem<'static>] {
        &mut self.poll_items[0..self.scheduled]
    }

    pub fn contains(&self, key: usize) -> bool {
        matches!(self.keys.get(key), Some(&Entry::Occupied(_)))
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

        let mut slab = PollSlab::new(unsafe { std::mem::transmute(control.as_poll_item(PollEvents::POLLIN)) });
        let key = slab.insert(&socket1);
        assert!(slab.get(key).unwrap().has_socket(&socket1));
        assert_eq!(0, key);
        let key = slab.insert(&socket2);
        assert!(slab.get(key).unwrap().has_socket(&socket2));
        assert_eq!(1, key);
        assert_eq!(3, slab.len());
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

        let mut slab = PollSlab::new(unsafe { std::mem::transmute(control.as_poll_item(PollEvents::POLLIN)) });
        let key1 = slab.insert(&socket1);
        let key2 = slab.insert(&socket2);

        slab.remove(key1);
        assert_eq!(2, slab.len());
        assert_eq!(2, slab.memory_in_use());
        let key = slab.insert(&socket1);
        assert!(slab.get(key).unwrap().has_socket(&socket1));
        assert!(slab.get(key2).unwrap().has_socket(&socket2));
        assert_eq!(0, key);
        assert_eq!(3, slab.len());
        assert_eq!(2, slab.memory_in_use());
    }

    #[test]
    fn swap() {
        let ctx = Context::new();
        ctx.set_io_threads(0).unwrap();
        let socket1 = ctx.socket(zmq::SocketType::PAIR).unwrap();
        let socket2 = ctx.socket(zmq::SocketType::PAIR).unwrap();
        let control = ctx.socket(zmq::SocketType::PAIR).unwrap();

        let mut slab = PollSlab::new(unsafe { std::mem::transmute(control.as_poll_item(PollEvents::POLLIN)) });
        let key1 = slab.insert(&socket1);
        let key2 = slab.insert(&socket2);

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

        let mut slab = PollSlab::new(unsafe { std::mem::transmute(control.as_poll_item(PollEvents::POLLIN)) });
        let key1 = slab.insert(&socket1);
        let key2 = slab.insert(&socket2);

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

        let mut slab = PollSlab::new(unsafe { std::mem::transmute(control.as_poll_item(PollEvents::POLLIN)) });
        let key1 = slab.insert(&socket1);
        let key2 = slab.insert(&socket2);

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
