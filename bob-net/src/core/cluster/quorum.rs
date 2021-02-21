use crate::prelude::*;

use super::{
    operations::{
        group_keys_by_nodes, lookup_local_alien, lookup_local_node, lookup_remote_aliens,
        lookup_remote_nodes, put_at_least, put_local_all, put_local_node, put_sup_nodes,
    },
    Cluster,
};
use crate::core::link_manager::LinkManager;

type Tasks = FuturesUnordered<JoinHandle<Result<NodeOutput<()>, NodeOutput<Error>>>>;

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

    async fn put_at_least(&self, key: BobKey, data: BobData) -> Result<(), Error> {
        debug!("PUT[{}] ~~~PUT LOCAL NODE FIRST~~~", key);
        let mut local_put_ok = 0_usize;
        let mut remote_ok_count = 0_usize;
        let mut at_least = self.quorum;
        let mut failed_nodes = Vec::new();
        let (vdisk_id, disk_path) = self.mapper.get_operation(key);
        if let Some(path) = disk_path {
            debug!("disk path is present, try put local");
            let res = put_local_node(&self.backend, key, data.clone(), vdisk_id, path).await;
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
        let (tasks, errors) = self.put_remote_nodes(key, data.clone(), at_least).await;
        let all_count = self.mapper.get_target_nodes_for_key(key).len();
        remote_ok_count += all_count - errors.len() - tasks.len() - local_put_ok;
        failed_nodes.extend(errors.iter().map(|e| e.node_name().to_string()));
        if remote_ok_count + local_put_ok >= self.quorum {
            debug!("PUT[{}] spawn {} background put tasks", key, tasks.len());
            let q = self.clone();
            tokio::spawn(q.background_put(tasks, key, data, failed_nodes));
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
        mut rest_tasks: Tasks,
        key: BobKey,
        data: BobData,
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
            if let Err(e) = self.put_aliens(failed_nodes, key, data).await {
                error!("{}", e);
            }
        }
    }

    pub(crate) async fn put_remote_nodes(
        &self,
        key: BobKey,
        data: BobData,
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

    pub(crate) async fn put_aliens(
        &self,
        mut failed_nodes: Vec<String>,
        key: BobKey,
        data: BobData,
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
        if let Err(sup_nodes_errors) = put_sup_nodes(key, data.clone(), &queries).await {
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
            data.clone(),
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
    async fn put(&self, key: BobKey, data: BobData) -> Result<(), Error> {
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
        let keys_by_nodes = group_keys_by_nodes(&self.mapper, keys);
        debug!(
            "EXIST Nodes for fan out: {:?}",
            &keys_by_nodes.keys().flatten().collect::<Vec<_>>()
        );
        let len = keys.len();
        let mut exist = vec![false; len];
        for (nodes, (keys, indexes)) in keys_by_nodes {
            let cluster_results = LinkManager::exist_on_nodes(&nodes, &keys).await;
            for result in cluster_results {
                if let Ok(result) = result {
                    for (&r, &ind) in result.inner().iter().zip(&indexes) {
                        exist[ind] |= r;
                    }
                }
            }
        }
        Ok(exist)
    }
}
