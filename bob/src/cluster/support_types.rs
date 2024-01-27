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
pub(crate) struct RemotePutResponse {
    affected_replicas: usize
}

impl RemotePutResponse {
    pub(super) fn new(affected_replicas: usize) -> Self {
        Self { affected_replicas }
    }

    pub(super) fn affected_replicas(&self) -> usize {
        self.affected_replicas
    }
}

#[derive(Debug)]
pub(crate) struct RemotePutError {
    affected_replicas: usize,
    #[allow(dead_code)] // We need error for debug print
    error: Error
}

impl RemotePutError {
    pub(super) fn new(affected_replicas: usize, error: Error) -> Self {
        Self { affected_replicas, error }
    }

    pub(crate) fn affected_replicas(&self) -> usize {
        self.affected_replicas
    }
}

#[derive(Debug)]
pub(super) struct RemoteDeleteError {
    force_alien_nodes: SmallVec<[NodeName; 1]>, // Will have 0 or 1 elements most of the time
    error: Error
}

impl RemoteDeleteError {
    pub(super) fn new(force_alien_nodes: SmallVec<[NodeName; 1]>, error: Error) -> Self {
        Self { force_alien_nodes, error }
    }
    #[allow(dead_code)]
    pub(super) fn force_alien_nodes(&self) -> &[NodeName] {
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

pub(crate) struct IndexMap {
    indexes: Vec<usize>,
}

impl IndexMap {
    /// Create empty indexes map
    pub(crate) fn new() -> Self {
        Self { indexes: vec![] }
    }

    /// Create with indexes with `false` values
    pub(crate) fn where_not_exists(data: &[bool]) -> Self {
        Self {
            indexes: data
                .iter()
                .enumerate()
                .filter(|(_, f)| !**f)
                .map(|(i, _)| i)
                .collect(),
        }
    }

    /// Check that indexes are empty
    pub(crate) fn is_empty(&self) -> bool {
        self.indexes.is_empty()
    }

    /// Get number of indexes in the map
    pub(crate) fn len(&self) -> usize {
        self.indexes.len()
    }

    /// Add index
    pub(crate) fn push(&mut self, item: usize) {
        debug_assert!(!self.indexes.contains(&item));
        self.indexes.push(item);
    }

    /// Collect data with allowed indexes
    pub(crate) fn collect<T: Clone>(
        &self,
        data: &[T]
    ) -> Vec<T> {
        let mut result = Vec::with_capacity(self.indexes.len());
        for &i in &self.indexes {
            debug_assert!(i < data.len());
            result.push(data[i].clone());
        }
        result
    }

    /// Update elements in original according to internal indexes mapping
    pub(crate) fn update_existence(&self, original: &mut [bool], mapped: &[bool]) {
        let max = original.len();
        assert!(self.indexes.len() == mapped.len());
        for i in 0..self.indexes.len() {
            let ind = self.indexes[i];
            debug_assert!(ind < max);
            original[ind] |= mapped[i];
        }
    }

    /// Retain only elements that is false in the `original` slice
    pub(crate) fn retain_not_existed(&mut self, original: &[bool]) {
        self.indexes.retain(|&i| {
            debug_assert!(i < original.len());
            original[i] == false
        });
    }
}

pub(crate) struct NodeOutputJoinHandle<Res, Err>(JoinHandle<Result<NodeOutput<Res>, NodeOutput<Err>>>, NodeName);

impl<Res, Err> NodeOutputJoinHandle<Res, Err> {
    pub(super) fn new(handle: JoinHandle<Result<NodeOutput<Res>, NodeOutput<Err>>>, node_name: NodeName) -> Self {
        Self(handle, node_name)
    }
}

impl<Res, Err> Future for NodeOutputJoinHandle<Res, Err> {
    type Output = Result<NodeOutput<Res>, NodeOutput<Err>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        use std::task::Poll;

        match self.0.poll_unpin(cx) {
            Poll::Ready(Ok(r)) => Poll::Ready(r),
            Poll::Ready(Err(e)) => {
                error!("{:?}", e);
                if e.is_panic() {
                    panic!("panic in thread");
                }
                panic!("cancellation is not supported")
            },
            Poll::Pending => std::task::Poll::Pending
        }
    }

}
