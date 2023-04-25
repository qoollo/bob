use crate::prelude::*;

use super::{
    operations::{
        delete_on_local_aliens, delete_on_local_node, delete_on_remote_nodes,
        delete_on_remote_nodes_with_options, exist_on_local_alien, exist_on_local_node,
        exist_on_remote_aliens, exist_on_remote_nodes, finish_at_least_handles, lookup_local_alien,
        lookup_local_node, lookup_remote_aliens, lookup_remote_nodes, put_at_least, put_local_all,
        put_local_node, put_sup_nodes, Tasks,
    },
    support_types::{HashSetExt, RemoteDeleteError},
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

    // ================== PUT ==================

    async fn put_at_least(&self, key: BobKey, data: &BobData) -> Result<(), Error> {
        let mut local_put_ok = 0_usize;
        let at_least = self.quorum;
        let mut failed_nodes = Vec::new();
        let (vdisk_id, disk_path) = self.mapper.get_operation(key);
        let (tasks, errors) = if let Some(disk_path) = disk_path {
            debug!(
                "PUT[{}] ~~~PUT TO {} REMOTE NODES AND LOCAL NODE~~~",
                key,
                at_least - 1
            );
            let (remote_put, local_put) = tokio::join!(
                self.put_remote_nodes(key, data, at_least - 1),
                put_local_node(&self.backend, key, data, vdisk_id, disk_path),
            );
            let (mut remote_tasks, mut errors) = remote_put;
            if let Err(e) = local_put {
                error!("{}", e);
                failed_nodes.push(self.mapper.local_node_name().to_owned());
                debug!("PUT[{}] local failed, put another remote", key);
                errors.extend(
                    finish_at_least_handles(&mut remote_tasks, 1)
                        .await
                        .into_iter(),
                );
            } else {
                local_put_ok += 1;
                debug!("PUT[{}] local node put successful", key);
            }
            (remote_tasks, errors)
        } else {
            debug!("PUT[{}] ~~~PUT TO {} REMOTE NODES~~~", key, at_least);
            self.put_remote_nodes(key, data, at_least).await
        };
        let all_count = self.mapper.get_target_nodes_for_key(key).len();
        let remote_ok_count = all_count - errors.len() - tasks.len() - local_put_ok;
        failed_nodes.extend(errors.iter().map(|e| e.node_name().to_string()));
        if remote_ok_count + local_put_ok >= self.quorum {
            if tasks.is_empty() && failed_nodes.is_empty() {
                return Ok(());
            }

            debug!("PUT[{}] spawn {} background put tasks", key, tasks.len());
            let q = self.clone();
            let data = data.clone();
            tokio::spawn(async move { q.background_put(tasks, key, &data, failed_nodes).await });
            Ok(())
        } else {
            assert!(
                tasks.is_empty(),
                "All target nodes put are expected to be completed before alien put begins"
            );
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

    async fn background_put(
        self,
        mut rest_tasks: Tasks<Error>,
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

    async fn put_remote_nodes(
        &self,
        key: BobKey,
        data: &BobData,
        at_least: usize,
    ) -> (Tasks<Error>, Vec<NodeOutput<Error>>) {
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

    async fn put_aliens(
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
        let local_put =
            put_local_all(&self.backend, failed_nodes.clone(), key, data, operation).await;
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

    // =============================== DELETE ======================

    async fn delete_on_nodes(&self, key: BobKey, meta: &BobMeta) -> Result<(), Error> {
        debug!("DELETE[{}] ~~~DELETE LOCAL NODE FIRST~~~", key);
        let (vdisk_id, disk_path) = self.mapper.get_operation(key);
        let mut failed_nodes = HashSet::new();
        let mut total = 0;
        if let Some(disk_path) = disk_path {
            total += 1;
            let res = delete_on_local_node(&self.backend, key, meta, vdisk_id, disk_path).await;
            if let Err(e) = res {
                error!("{}", e);
                failed_nodes.insert(self.mapper.local_node_name().to_owned());
            }
        };

        debug!("DELETE[{}] ~~~DELETE TO REMOTE NODES~~~", key);
        let (errors, remote_count) = self.delete_at_remote_nodes(key, meta).await;
        total += remote_count;
        failed_nodes.extend(errors.iter().map(|o| o.node_name().to_string()));

        if !failed_nodes.is_empty() {
            warn!(
                "DELETE[{}] was not successful on target nodes. Operation continue on aliens. total {}, failed {:?}, errors: {:?}",
                key, total, failed_nodes, errors
            );
        }

        if let Err(err) = self.delete_aliens(failed_nodes, key, meta).await {
            error!(
                "DELETE[{}] delete failed. Smth wrong with cluster/node configuration",
                key
            );
            Err(err)
        } else {
            debug!("DELETE[{}] succeed", key);
            Ok(())
        }
    }

    async fn delete_at_remote_nodes(
        &self,
        key: BobKey,
        meta: &BobMeta,
    ) -> (Vec<NodeOutput<RemoteDeleteError>>, usize) {
        let local_node = self.mapper.local_node_name();
        let target_nodes: Vec<_> = self
            .mapper
            .get_target_nodes_for_key(key)
            .iter()
            .filter(|n| n.name() != local_node)
            .collect();
        debug!(
            "DELETE[{}] cluster quorum runs delete on remote nodes. Target nodes count: {}",
            key,
            target_nodes.len(),
        );

        let count = target_nodes.len();
        (
            delete_on_remote_nodes_with_options(
                key,
                meta,
                target_nodes,
                DeleteOptions::new_local(),
            )
            .await,
            count,
        )
    }

    async fn delete_aliens(
        &self,
        mut failed_nodes: HashSet<String>,
        key: BobKey,
        meta: &BobMeta,
    ) -> Result<(), Error> {
        trace!("DELETE[{}] ~~~TRY DELETE AT REMOTE ALIENS~~~", key);

        let mut sup_nodes_set = HashSet::new();
        let local_node_name = self.mapper.local_node_name();

        if failed_nodes.len() > 0 {
            let sup_nodes = self.mapper.get_support_nodes(key, failed_nodes.len());
            for sup_node in sup_nodes.iter() {
                sup_nodes_set.insert(sup_node.name());
            }

            trace!("DELETE[{}] sup delete nodes: {:?}", key, &sup_nodes);
            let nodes_need_remote_backup: Vec<_> = failed_nodes.drain_collect(sup_nodes.len());
            let queries: Vec<_> = sup_nodes
                .into_iter()
                .zip(nodes_need_remote_backup)
                .map(|(node, remote_node)| (node, DeleteOptions::new_alien(vec![remote_node])))
                .collect();

            trace!("DELETE[{}] supported alien requests: {:?}", key, queries);
            if let Err(sup_nodes_errors) =
                delete_on_remote_nodes(key, meta, queries.into_iter()).await
            {
                warn!("delete on support nodes errors: {:?}", sup_nodes_errors);
                failed_nodes.extend(
                    sup_nodes_errors
                        .into_iter()
                        .flat_map(|err| err.into_inner().into_force_alien_nodes().into_iter()),
                )
            };
        }

        // Delete on all nodes of cluster except sup_nodes and local node
        let all_other_nodes_queries: Vec<_> = self
            .mapper
            .nodes()
            .iter()
            .filter(|n| !sup_nodes_set.contains(n.name()) && n.name() != local_node_name)
            .map(|n| (n, DeleteOptions::new_alien(vec![])))
            .collect();

        trace!(
            "DELETE[{}] normal alien deletion requests: {:?}",
            key,
            all_other_nodes_queries
        );
        if let Err(sup_nodes_errors) =
            delete_on_remote_nodes(key, meta, all_other_nodes_queries.into_iter()).await
        {
            debug!("delete on aliens nodes errors: {:?}", sup_nodes_errors);
        };

        // Delete on local node
        let local_delete = delete_on_local_aliens(
            &self.backend,
            key,
            meta,
            self.mapper.get_target_nodes_for_key(key),
            failed_nodes,
            self.mapper.vdisk_id_from_key(key),
        )
        .await;

        if let Err(e) = local_delete {
            error!(
                "DELETE[{}] local delete failed, smth wrong with backend: {:?}",
                key, e
            );
            return Err(Error::internal());
        }
        Ok(())
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
        debug!("GET[{}] Key not found", key);
        Err(Error::key_not_found(key))
    }

    async fn exist(&self, keys: &[BobKey]) -> Result<Vec<bool>, Error> {
        let len = keys.len();
        debug!("EXIST {} keys", len);

        let mut result = vec![false; len];

        let (local, mut primary, mut secondary) = group_by_nodes(keys, &self.mapper);

        if let Some(local) = local {
            if !local.is_empty() {
                let local_keys = local.collect(keys);
                let local_exist = exist_on_local_node(&self.backend, &local_keys).await?;
                local.update_existence(&mut result, &local_exist);
            }
        }

        collect_remote_exists(&mut result, keys, &mut primary).await?;
        collect_remote_exists(&mut result, keys, &mut secondary).await?;

        let alien_index_map = IndexMap::where_not_exists(&result);

        if !alien_index_map.is_empty() {
            let local_alien_result =
                exist_on_local_alien(&self.backend, &alien_index_map.collect(keys)).await?;
            alien_index_map.update_existence(&mut result, &local_alien_result);
        }

        if !alien_index_map.is_empty() {
            let all_remote_nodes: Vec<_> = self
                .mapper
                .nodes()
                .iter()
                .filter(|n| n.name() != self.mapper.local_node_name())
                .cloned()
                .collect();
            let remote_nodes_aliens_exist =
                exist_on_remote_aliens(&all_remote_nodes, &alien_index_map.collect(keys)).await;
            for remote_alien_result in remote_nodes_aliens_exist {
                match remote_alien_result {
                    Ok(remote_alien_result) => {
                        alien_index_map.update_existence(&mut result, remote_alien_result.inner());
                    }
                    Err(e) => {
                        debug!("Failed to check existence in aliens: {:?}", e);
                    }
                }
            }
        }

        Ok(result)
    }

    async fn delete(&self, key: BobKey, meta: &BobMeta) -> Result<(), Error> {
        self.delete_on_nodes(key, meta).await
    }
}

async fn collect_remote_exists(
    result: &mut [bool],
    keys: &[BobKey],
    indexes_by_node: &mut HashMap<Node, IndexMap>,
) -> Result<(), Error> {
    if !indexes_by_node.is_empty() {
        let mut keys_by_node = HashMap::new();
        for (node, node_map) in indexes_by_node.iter_mut() {
            node_map.retain_not_existed(&result);
            if !node_map.is_empty() {
                keys_by_node.insert(node.clone(), node_map.collect(keys));
            }
        }

        if !keys_by_node.is_empty() {
            let nodes: Vec<_> = keys_by_node.keys().cloned().collect();
            let remote_results = exist_on_remote_nodes(&nodes, keys_by_node).await;
            for (remote_result, node) in remote_results.into_iter().zip(nodes) {
                match remote_result {
                    Ok(remote_result) => {
                        indexes_by_node
                            .get(&node)
                            .map(|idx| idx.update_existence(result, remote_result.inner()));
                    }
                    Err(e) => {
                        debug!("Failed to check existence on node {}: {:?}", node.name(), e);
                    }
                }
            }
        }
    }
    Ok(())
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
                .filter(|(_, f)| !**f)
                .map(|(i, _)| i)
                .collect(),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.indexes.is_empty()
    }

    pub(crate) fn push(&mut self, item: usize) {
        debug_assert!(!self.indexes.contains(&item));
        self.indexes.push(item);
    }

    pub(crate) fn collect<'a, T: Clone + 'a>(
        &'a self,
        data: impl IntoIterator<Item = &'a T>,
    ) -> Vec<T> {
        data.into_iter()
            .enumerate()
            .filter(|(i, _)| self.indexes.contains(i))
            .map(|(_, i)| i.clone())
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
