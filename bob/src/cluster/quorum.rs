use crate::prelude::*;

use super::{
    operations::{
        delete_at_local_node, delete_at_nodes, exist_on_local_alien, exist_on_local_node,
        exist_on_remote_aliens, exist_on_remote_nodes, lookup_local_alien, lookup_local_node,
        lookup_remote_aliens, lookup_remote_nodes, put_at_least, put_local_all, put_local_node,
        put_sup_nodes, Tasks,
    },
    Cluster,
};

#[derive(Clone)]
pub(crate) struct Quorum {
    backend: Arc<Backend>,
    mapper: Arc<Virtual>,
    quorum: usize,
}

impl Quorum {
    pub(crate) fn new(backend: Arc<Backend>, mapper: Arc<Virtual>, quorum: usize) -> Self {
        Self {
            backend,
            mapper,
            quorum,
        }
    }

    async fn put_at_least(&self, key: BobKey, data: &BobData) -> Result<(), Error> {
        debug!("PUT[{}] ~~~PUT LOCAL NODE FIRST~~~", key);
        let mut local_put_ok = 0_usize;
        let mut remote_ok_count = 0_usize;
        let mut at_least = self.quorum;
        let mut failed_nodes = Vec::new();
        let (vdisk_id, disk_path) = self.mapper.get_operation(key);
        if let Some(path) = disk_path {
            debug!("disk path is present, try put local");
            let res = put_local_node(&self.backend, key, data, vdisk_id, path).await;
            if let Err(e) = res {
                error!("{}", e);
                failed_nodes.push(self.mapper.local_node_name().to_owned());
            } else {
                local_put_ok += 1;
                at_least -= 1;
                debug!("PUT[{}] local node put successful", key);
            }
        } else {
            debug!("skip local put");
        }
        debug!("PUT[{}] need at least {} additional puts", key, at_least);

        debug!("PUT[{}] ~~~PUT TO REMOTE NODES~~~", key);
        let (tasks, errors) = self.put_remote_nodes(key, data, at_least).await;
        let all_count = self.mapper.get_target_nodes_for_key(key).len();
        remote_ok_count += all_count - errors.len() - tasks.len() - local_put_ok;
        failed_nodes.extend(errors.iter().map(|e| e.node_name().to_string()));
        if remote_ok_count + local_put_ok >= self.quorum {
            debug!("PUT[{}] spawn {} background put tasks", key, tasks.len());
            let q = self.clone();
            let data = data.clone();
            tokio::spawn(async move {
                q.background_put(tasks, key, &data, failed_nodes).await
            });
            Ok(())
        } else {
            warn!(
                "PUT[{}] quorum was not reached. ok {}, quorum {}, errors: {:?}",
                key,
                remote_ok_count + local_put_ok,
                self.quorum,
                errors
            );
            if let Err(err) = self.put_aliens(failed_nodes, key, data).await {
                error!("PUT[{}] smth wrong with cluster/node configuration", key);
                error!("PUT[{}] node errors: {:?}", key, errors);
                Err(err)
            } else {
                warn!("PUT[{}] succeed, but some data get into alien", key);
                Ok(())
            }
        }
    }

    async fn delete_on_nodes(&self, key: BobKey) -> Result<(), Error> {
        debug!("DELETE[{}] ~~~DELETE LOCAL NODE FIRST~~~", key);
        let res = delete_at_local_node(&self.backend, key).await;
        let local_delete_ok = if let Err(e) = res {
            error!("{}", e);
            false
        } else {
            debug!("DELETE[{}] local node delete successful", key);
            true
        };

        debug!("DELETE[{}] ~~~DELETE TO REMOTE NODES~~~", key);
        let (errors, remote_count) = self.delete_at_remote_nodes(key).await;
        let remote_ok_count = remote_count - errors.len();
        if errors.len() > 0 || !local_delete_ok {
            warn!(
                "DELETE[{}] was not successful. local done: {}, remote {}, failed {}, errors: {:?}",
                key,
                local_delete_ok,
                remote_ok_count,
                errors.len(),
                errors
            );
            Err(Error::failed("Data was deleted not on all nodes"))
        } else {
            Ok(())
        }
    }

    async fn background_put(
        self,
        mut rest_tasks: Tasks,
        key: BobKey,
        data: &BobData,
        mut failed_nodes: Vec<String>,
    ) {
        debug!("PUT[{}] ~~~BACKGROUND PUT TO REMOTE NODES~~~", key);
        while let Some(join_res) = rest_tasks.next().await {
            match join_res {
                Ok(Ok(output)) => debug!(
                    "PUT[{}] successful background put to: {}",
                    key,
                    output.node_name()
                ),
                Ok(Err(e)) => {
                    error!("{:?}", e);
                    failed_nodes.push(e.node_name().to_string());
                }
                Err(e) => error!("{:?}", e),
            }
        }
        debug!("PUT[{}] ~~~PUT TO REMOTE NODES ALIEN~~~", key);
        if !failed_nodes.is_empty() {
            if let Err(e) = self.put_aliens(failed_nodes, key, &data).await {
                error!("{}", e);
            }
        }
    }

    pub(crate) async fn put_remote_nodes(
        &self,
        key: BobKey,
        data: &BobData,
        at_least: usize,
    ) -> (Tasks, Vec<NodeOutput<Error>>) {
        let local_node = self.mapper.local_node_name();
        let target_nodes = self.mapper.get_target_nodes_for_key(key);
        debug!(
            "PUT[{}] cluster quorum put remote nodes {} total target nodes",
            key,
            target_nodes.len(),
        );
        let target_nodes = target_nodes.iter().filter(|node| node.name() != local_node);
        put_at_least(key, data, target_nodes, at_least, PutOptions::new_local()).await
    }

    pub(crate) async fn delete_at_remote_nodes(
        &self,
        key: BobKey,
    ) -> (Vec<NodeOutput<Error>>, usize) {
        let local_node = self.mapper.local_node_name();
        let target_nodes: Vec<_> = self
            .mapper
            .nodes()
            .values()
            .filter(|n| n.name() != local_node)
            .collect();
        debug!(
            "DELETE[{}] cluster quorum put remote nodes {} total target nodes",
            key,
            target_nodes.len(),
        );
        let count = target_nodes.len();
        (
            delete_at_nodes(
                key,
                target_nodes.into_iter(),
                count,
                DeleteOptions::new_local(),
            )
            .await,
            count,
        )
    }

    pub(crate) async fn put_aliens(
        &self,
        mut failed_nodes: Vec<String>,
        key: BobKey,
        data: &BobData,
    ) -> Result<(), Error> {
        debug!("PUT[{}] ~~~TRY PUT TO REMOTE ALIENS FIRST~~~", key);
        if failed_nodes.is_empty() {
            warn!(
                "PUT[{}] trying to aliens, but there are no failed nodes: unreachable",
                key
            );
            return Err(Error::internal());
        }
        trace!("selection of free nodes available for data writing");
        let sup_nodes = self.mapper.get_support_nodes(key, failed_nodes.len());
        debug!("PUT[{}] sup put nodes: {:?}", key, &sup_nodes);
        let nodes_need_remote_backup: Vec<_> = failed_nodes.drain(..sup_nodes.len()).collect();
        let queries: Vec<_> = sup_nodes
            .into_iter()
            .zip(nodes_need_remote_backup)
            .map(|(node, remote_node)| (node, PutOptions::new_alien(vec![remote_node])))
            .collect();
        debug!("PUT[{}] additional alien requests: {:?}", key, queries);
        if let Err(sup_nodes_errors) = put_sup_nodes(key, data, &queries).await {
            debug!("support nodes errors: {:?}", sup_nodes_errors);
            failed_nodes.extend(
                sup_nodes_errors
                    .iter()
                    .map(|err| err.node_name().to_owned()),
            )
        };
        debug!("need additional local alien copies: {}", failed_nodes.len());
        let vdisk_id = self.mapper.vdisk_id_from_key(key);
        let operation = Operation::new_alien(vdisk_id);
        let local_put = put_local_all(
            &self.backend,
            failed_nodes.clone(),
            key,
            data,
            operation,
        )
        .await;
        if let Err(e) = local_put {
            error!(
                "PUT[{}] local put failed, smth wrong with backend: {:?}",
                key, e
            );
            Err(Error::internal())
        } else {
            Ok(())
        }
    }
}

#[async_trait]
impl Cluster for Quorum {
    async fn put(&self, key: BobKey, data: &BobData) -> Result<(), Error> {
        self.put_at_least(key, data).await
    }

    //todo check no data (no error)
    async fn get(&self, key: BobKey) -> Result<BobData, Error> {
        debug!("GET[{}] ~~~LOOKUP LOCAL NODE~~~", key);
        let (vdisk_id, disk_path) = self.mapper.get_operation(key);
        if let Some(data) = lookup_local_node(&self.backend, key, vdisk_id, disk_path).await {
            return Ok(data);
        }
        debug!("GET[{}] ~~~LOOKUP REMOTE NODES~~~", key);
        if let Some(data) = lookup_remote_nodes(&self.mapper, key).await {
            return Ok(data);
        }
        debug!("GET[{}] ~~~LOOKUP LOCAL NODE ALIEN~~~", key);
        if let Some(data) = lookup_local_alien(&self.backend, key, vdisk_id).await {
            return Ok(data);
        }

        debug!("GET[{}] ~~~LOOKUP REMOTE NODES ALIEN~~~", key);
        if let Some(data) = lookup_remote_aliens(&self.mapper, key).await {
            return Ok(data);
        }
        info!("GET[{}] Key not found", key);
        Err(Error::key_not_found(key))
    }

    async fn exist(&self, keys: &[BobKey]) -> Result<Vec<bool>, Error> {
        let len = keys.len();
        let mut exist = vec![false; len];

        // filter local keys
        let (indices, local_keys) = filter_local_keys(keys, &self.mapper);
        // end
        trace!("local keys {:?} local indices {:?}", local_keys, indices);
        if local_keys.len() > 0 {
            let result = exist_on_local_node(&self.backend, &local_keys).await?;
            for (idx, r) in indices.into_iter().zip(result.into_iter()) {
                exist[idx] |= r;
            }
            trace!("exist after local node {:?}", exist);
        }

        // filter keys that were not found
        let local_alien = filter_not_found(&exist, keys);
        // end
        trace!("local alien keys {:?}", local_alien);
        if local_alien.len() > 0 {
            let result = exist_on_local_alien(&self.backend, &local_alien).await?;
            update_exist(&mut exist, &result);
            trace!("exist after local alien {:?}", exist);
        }

        // filter remote not found keys by nodes
        let remote_keys = filter_remote_not_found_by_nodes(&exist, keys, &self.mapper);
        // end

        trace!("remote keys by nodes {:?}", remote_keys);
        if remote_keys.len() > 0 {
            for (nodes, (keys, indices)) in remote_keys {
                let result = exist_on_remote_nodes(&nodes, &keys).await;
                for res in result.into_iter().flatten() {
                    for (&r, &ind) in res.inner().iter().zip(&indices) {
                        exist[ind] |= r;
                    }
                }
            }
            trace!("exist after remote nodes {:?}", exist);
        }

        // filter remote not found keys
        let remote_alien = filter_not_found(&exist, keys);
        // end
        if remote_alien.len() > 0 {
            // filter remote nodes
            let remote_nodes = filter_remote_nodes(&self.mapper);
            // end
            trace!("remote nodes {:?}", remote_nodes);

            let result = exist_on_remote_aliens(&remote_nodes, &remote_alien).await;
            trace!("alien result {:?}", result);
            for res in result.into_iter() {
                if let Ok(inner) = res {
                    trace!("inner {:?}", inner);
                    update_exist(&mut exist, inner.inner());
                }
            }
        }
        Ok(exist)
    }

    async fn delete(&self, key: BobKey) -> Result<(), Error> {
        self.delete_on_nodes(key).await
    }
}

fn group_by_nodes(
    keys: &[BobKey],
    mapper: &Virtual,
) -> (
    Option<IndexMap>,
    HashMap<Node, IndexMap>,
    HashMap<Node, IndexMap>,
) {
    let mut local = IndexMap::new();
    let mut primary = HashMap::new();
    let mut secondary = HashMap::new();

    let local_node = mapper.local_node_name();

    for (index, &key) in keys.iter().enumerate() {
        let target_nodes = mapper.get_target_nodes_for_key(key);

        if !target_nodes
            .iter()
            .any(|n| n.name() == local_node || primary.contains_key(n))
        {
            if let Some(node) = target_nodes.iter().find(|n| !secondary.contains_key(*n)) {
                primary.insert(node.clone(), IndexMap::new());
            }
        }

        for node in target_nodes {
            if node.name() == local_node {
                local.push(index);
            } else if let Some(map) = primary.get_mut(node) {
                map.push(index)
            } else {
                secondary
                    .entry(node.clone())
                    .or_insert(IndexMap::new())
                    .push(index);
            }
        }
    }

    (
        if local.is_empty() { None } else { Some(local) },
        primary,
        secondary,
    )
}

fn filter_local_keys(keys: &[BobKey], mapper: &Virtual) -> (Vec<usize>, Vec<BobKey>) {
    let mut indices = Vec::new();
    let local_keys = keys
        .iter()
        .enumerate()
        .filter_map(|(idx, &key)| {
            mapper
                .get_target_nodes_for_key(key)
                .iter()
                .find_map(|node| {
                    if node.name() == mapper.local_node_name() {
                        indices.push(idx);
                        Some(key)
                    } else {
                        None
                    }
                })
        })
        .collect::<Vec<BobKey>>();
    (indices, local_keys)
}

fn filter_not_found(exist: &[bool], keys: &[BobKey]) -> Vec<BobKey> {
    let mut not_found_keys = Vec::new();
    for (idx, &r) in exist.iter().enumerate() {
        if r == false {
            not_found_keys.push(keys[idx]);
        }
    }
    not_found_keys
}

fn filter_remote_not_found_by_nodes(
    exist: &[bool],
    keys: &[BobKey],
    mapper: &Virtual,
) -> HashMap<Vec<Node>, (Vec<BobKey>, Vec<usize>)> {
    let mut remote_keys: HashMap<_, (Vec<_>, Vec<_>)> = HashMap::new();
    for (idx, &r) in exist.iter().enumerate() {
        if r == false {
            remote_keys
                .entry(mapper.get_target_nodes_for_key(keys[idx]).to_vec())
                .and_modify(|(closure_keys, indices)| {
                    closure_keys.push(keys[idx]);
                    indices.push(idx);
                })
                .or_insert_with(|| (vec![keys[idx]], vec![idx]));
        }
    }
    remote_keys
}

fn filter_remote_nodes(mapper: &Virtual) -> Vec<Node> {
    let remote_nodes = mapper
        .nodes()
        .into_iter()
        .filter_map(|(_, node)| {
            if node.name() != mapper.local_node_name() {
                Some(node.clone())
            } else {
                None
            }
        })
        .collect::<Vec<Node>>();
    remote_nodes
}

fn update_exist(exist: &mut [bool], result: &[bool]) {
    let mut i = 0;
    for r in exist.iter_mut() {
        if *r == false {
            *r |= result[i];
            i += 1;
        }
    }
}

pub(crate) struct IndexMap {
    indexes: Vec<usize>,
}

impl IndexMap {
    pub(crate) fn new() -> Self {
        Self { indexes: vec![] }
    }

    pub(crate) fn where_not_exists(data: &[bool]) -> Self {
        Self {
            indexes: data
                .iter()
                .enumerate()
                .filter(|(_, f)| **f)
                .map(|(i, _)| i)
                .collect(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.indexes.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.indexes.is_empty()
    }

    pub(crate) fn push(&mut self, item: usize) {
        debug_assert!(!self.indexes.contains(&item));
        self.indexes.push(item);
    }

    pub(crate) fn collect<T>(&self, data: impl IntoIterator<Item = T>) -> Vec<T> {
        data.into_iter()
            .enumerate()
            .filter(|(i, _)| self.indexes.contains(i))
            .map(|(_, i)| i)
            .collect()
    }

    pub(crate) fn update_existence(&self, original: &mut [bool], mapped: &[bool]) {
        let max = original.len();
        self.indexes
            .iter()
            .zip(mapped.iter())
            .filter(|(i, _)| **i < max)
            .for_each(|(i, f)| original[*i] |= f)
    }

    pub(crate) fn retain_not_existed(&mut self, original: &[bool]) {
        self.indexes
            .retain(|&i| i >= original.len() || original[i] == false);
    }
}
