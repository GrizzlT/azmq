use std::{ops::{Deref, DerefMut}, slice::SliceIndex};

use zmq::Message as ZmqMessage;

#[derive(Debug)]
pub struct Message {
    inner: ZmqMessage,
}

impl Deref for Message {
    type Target = ZmqMessage;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for Message {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Clone for Message {
    fn clone(&self) -> Self {
        Message {
            inner: ZmqMessage::from(self.deref().deref())
        }
    }
}

impl<T: Into<ZmqMessage>> From<T> for Message {
    fn from(value: T) -> Self {
        Self {
            inner: value.into(),
        }
    }
}

impl std::hash::Hash for Message {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.inner.hash(state);
    }
}

#[derive(Debug, Clone, Hash)]
pub struct Multipart {
    pub parts: Vec<Message>,
}

impl Multipart {
    pub fn new() -> Self {
        Self {
            parts: Vec::new(),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            parts: Vec::with_capacity(capacity),
        }
    }

    pub fn capacity(&self) -> usize {
        self.parts.capacity()
    }

    pub fn len(&self) -> usize {
        self.parts.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get(&self, index: usize) -> Option<&[u8]> {
        self.parts.get(index).map(|m| m.deref().deref())
    }

    pub fn get_msg(&self, index: usize) -> Option<&Message> {
        self.parts.get(index)
    }

    pub fn get_mut(&mut self, index: usize) -> Option<&mut [u8]> {
        self.parts.get_mut(index).map(|m| m.deref_mut().deref_mut())
    }

    pub fn get_mut_msg(&mut self, index: usize) -> Option<&mut Message> {
        self.parts.get_mut(index)
    }

    /// Implicitly copies the bytes
    ///
    /// # Panics
    ///
    /// Panics if `index > len`
    pub fn insert(&mut self, index: usize, bytes: &[u8]) {
        self.parts.insert(index, bytes.into());
    }

    /// # Panics
    ///
    /// Panics if `index > len`
    pub fn insert_msg(&mut self, index: usize, msg: Message) {
        self.parts.insert(index, msg);
    }

    /// Implicitly copies the bytes
    pub fn push(&mut self, bytes: &[u8]) {
        self.parts.push(bytes.into());
    }

    pub fn push_msg(&mut self, msg: Message) {
        self.parts.push(msg);
    }

    pub fn remove(&mut self, index: usize) -> Message {
        self.parts.remove(index)
    }

    pub fn swap_remove(&mut self, index: usize) -> Message {
        self.parts.swap_remove(index)
    }
}

impl Default for Multipart {
    fn default() -> Self {
        Self::new()
    }
}

impl AsRef<[Message]> for Multipart {
    fn as_ref(&self) -> &[Message] {
        self.parts.as_ref()
    }
}

impl AsMut<[Message]> for Multipart {
    fn as_mut(&mut self) -> &mut [Message] {
        self.parts.as_mut()
    }
}

impl std::borrow::Borrow<[Message]> for Multipart {
    fn borrow(&self) -> &[Message] {
        self.as_ref()
    }
}

impl std::borrow::BorrowMut<[Message]> for Multipart {
    fn borrow_mut(&mut self) -> &mut [Message] {
        self.as_mut()
    }
}

impl Deref for Multipart {
    type Target = [Message];

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl DerefMut for Multipart {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut()
    }
}

impl Extend<Message> for Multipart {
    fn extend<T: IntoIterator<Item = Message>>(&mut self, iter: T) {
        self.parts.extend(iter);
    }
}

impl FromIterator<Message> for Multipart {
    fn from_iter<T: IntoIterator<Item = Message>>(iter: T) -> Self {
        Self {
            parts: Vec::from_iter(iter)
        }
    }
}

impl<T: Into<Vec<Message>>> From<T> for Multipart {
    fn from(value: T) -> Self {
        Self {
            parts: value.into()
        }
    }
}

impl<I: SliceIndex<[Message]>> std::ops::Index<I> for Multipart {
    type Output = <I as SliceIndex<[Message]>>::Output;

    fn index(&self, index: I) -> &Self::Output {
        self.parts.index(index)
    }
}

impl<I: SliceIndex<[Message]>> std::ops::IndexMut<I> for Multipart {
    fn index_mut(&mut self, index: I) -> &mut Self::Output {
        self.parts.index_mut(index)
    }
}

impl<'a> IntoIterator for &'a Multipart {
    type Item = &'a Message;

    type IntoIter = std::slice::Iter<'a, Message>;

    fn into_iter(self) -> Self::IntoIter {
        self.parts.iter()
    }
}

impl<'a> IntoIterator for &'a mut Multipart {
    type Item = &'a mut Message;

    type IntoIter = std::slice::IterMut<'a, Message>;

    fn into_iter(self) -> Self::IntoIter {
        self.parts.iter_mut()
    }
}

impl IntoIterator for Multipart {
    type Item = Message;

    type IntoIter = std::vec::IntoIter<Message>;

    fn into_iter(self) -> Self::IntoIter {
        self.parts.into_iter()
    }
}
