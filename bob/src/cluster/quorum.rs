use crate::prelude::*;

use super::{
    operations::{
        delete_on_local_node, delete_on_local_aliens, delete_on_remote_nodes, delete_on_remote_nodes_with_options,
        group_keys_by_nodes, lookup_local_alien, lookup_local_node, lookup_remote_aliens, finish_at_least_handles,
        lookup_remote_nodes, put_at_least, put_local_all, put_local_node, put_sup_nodes, Tasks,
    },
    support_types::{ RemoteDeleteError, HashSetExt },
    Cluster,
};
use crate::link_manager::LinkManager;

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
        debug!("PUT[{}] ~~~PUT LOCAL NODE FIRST~~~", key);
        let mut local_put_ok = 0_usize;
        let mut remote_ok_count = 0_usize;
        let at_least = self.quorum;
        let mut failed_nodes = Vec::new();
        let (vdisk_id, disk_path) = self.mapper.get_operation(key);
        let local_put_task = disk_path.map(|path| {
            debug!("disk path is present, try put local");
            put_local_node(&self.backend, key, data, vdisk_id, path).boxed()
        });
        debug!(
            "PUT[{}] need at least {} additional puts",
            key,
            at_least - 1
        );
        debug!("PUT[{}] ~~~PUT TO REMOTE NODES~~~", key);
        let (mut tasks, mut errors) = self.put_remote_nodes(key, data, at_least - 1).await;
        if let Some(task) = local_put_task {
            let local_put = task.await;
            if let Err(e) = local_put {
                error!("{}", e);
                failed_nodes.push(self.mapper.local_node_name().to_owned());
                debug!("PUT[{}] local failed, put another remote", key);
            } else {
                local_put_ok += 1;
                debug!("PUT[{}] local node put successful", key);
            }
        };
        if local_put_ok == 0 {
            errors.extend(finish_at_least_handles(&mut tasks, 1).await.into_iter());
        }
        let all_count = self.mapper.get_target_nodes_for_key(key).len();
        remote_ok_count += all_count - errors.len() - tasks.len() - local_put_ok;
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
            error!("DELETE[{}] delete failed. Smth wrong with cluster/node configuration", key);
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
            delete_on_remote_nodes_with_options(key, meta, target_nodes, DeleteOptions::new_local()).await,
            count
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
            if let Err(sup_nodes_errors) = delete_on_remote_nodes(key, meta, queries.into_iter()).await {
                warn!("delete on support nodes errors: {:?}", sup_nodes_errors);
                failed_nodes.extend(
                    sup_nodes_errors
                        .into_iter()
                        .flat_map(|err| err.into_inner().into_force_alien_nodes().into_iter()),
                )
            };
        }


        // Delete on all nodes of cluster except sup_nodes and local node
        let all_other_nodes_queries: Vec<_> = self.mapper.nodes().values()
            .filter(|n| !sup_nodes_set.contains(n.name()) && n.name() != local_node_name)
            .map(|n| (n, DeleteOptions::new_alien(vec![])))
            .collect();
        
        trace!("DELETE[{}] normal alien deletion requests: {:?}", key, all_other_nodes_queries);
        if let Err(sup_nodes_errors) = delete_on_remote_nodes(key, meta, all_other_nodes_queries.into_iter()).await {
            debug!("delete on aliens nodes errors: {:?}", sup_nodes_errors);
        };        


        // Delete on local node
        let local_delete = delete_on_local_aliens(
            &self.backend,
            key,
            meta,
            self.mapper.get_target_nodes_for_key(key),
            failed_nodes,
            self.mapper.vdisk_id_from_key(key)
        ).await;

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
        let keys_by_nodes = group_keys_by_nodes(&self.mapper, keys);
        debug!(
            "EXIST Nodes for fan out: {:?}",
            &keys_by_nodes.keys().flatten().collect::<Vec<_>>()
        );
        let len = keys.len();
        let mut exist = vec![false; len];
        for (nodes, (keys, indexes)) in keys_by_nodes {
            let cluster_results = LinkManager::exist_on_nodes(&nodes, &keys).await;
            for result in cluster_results.into_iter().flatten() {
                for (&r, &ind) in result.inner().iter().zip(&indexes) {
                    exist[ind] |= r;
                }
            }
        }
        Ok(exist)
    }

    async fn delete(&self, key: BobKey, meta: &BobMeta) -> Result<(), Error> {
        self.delete_on_nodes(key, meta).await
    }
}
