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
