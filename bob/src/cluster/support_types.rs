use crate::prelude::*;
use smallvec::SmallVec;


pub(super) trait HashSetExt<T> {
    fn drain_collect(&mut self, count: usize) -> Vec<T>;  
}

impl<T: Eq + core::hash::Hash> HashSetExt<T> for HashSet<T> {
    fn drain_collect(&mut self, count: usize) -> Vec<T> {
        if count >= self.len() {
            // The assumtion is that this branch is the most often taken
            return self.drain().collect();
        }
        if count == 0 {
            return Vec::new();
        }

        let mut result = Vec::with_capacity(self.len());

        result.extend(self.drain());
        self.extend(result.drain(count.min(result.len())..));

        return result;
    }
}

#[derive(Debug)]
pub(super) struct RemoteDeleteError {
    force_alien_nodes: SmallVec<[NodeName; 1]>, // Will have 0 or 1 elements most of the time
    error: Error
}

impl RemoteDeleteError {
    pub(super) fn new(force_alien_nodes: SmallVec<[NodeName; 1]>, error: Error) -> Self {
        Self {
            force_alien_nodes: force_alien_nodes,
            error: error
        }
    }
    #[allow(dead_code)]
    pub(super) fn force_alien_nodes(&self) -> &[String] {
        return &self.force_alien_nodes;
    }
    #[allow(dead_code)]
    pub(super) fn error(&self) -> &Error {
        return &self.error;
    }

    pub(super) fn into_force_alien_nodes(self) -> SmallVec<[NodeName; 1]> {
        return self.force_alien_nodes;
    }
}

impl From<RemoteDeleteError> for Error {
    fn from(w: RemoteDeleteError) -> Error {
        w.error
    }
}
