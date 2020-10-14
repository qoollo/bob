use super::prelude::*;
use tokio::task::JoinHandle;

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
        let (vdisk_id, disk_path) = self.mapper.get_operation(key);
        if let Some(path) = disk_path {
            debug!("disk path is present, try put local");
            let res = put_local_node(&self.backend, key, data.clone(), vdisk_id, path).await;
            if let Err(e) = res {
                error!("{}", e);
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
        remote_ok_count += at_least - errors.len();
        let failed_nodes = errors
            .iter()
            .map(|e| e.node_name().to_string())
            .collect::<Vec<_>>();
        let q = self.clone();
        if remote_ok_count + local_put_ok >= self.quorum {
            if tasks.is_empty() {
                Ok(())
            } else {
                debug!("PUT[{}] spawn {} background put tasks", key, tasks.len());
                tokio::spawn(q.background_put(tasks, key, data, failed_nodes));
                Ok(())
            }
        } else {
            warn!(
                "PUT[{}] quorum was not reached. ok {}, quorum {}, errors: {:?}",
                key,
                remote_ok_count + local_put_ok,
                self.quorum,
                errors
            );
            if let Err(err) = self.put_aliens(failed_nodes, key, data).await {
                if let Some(err) = errors.last() {
                    Err(err.inner().clone())
                } else {
                    error!("PUT[{}] smth wrong with cluster/node configuration", key);
                    error!(
                        "PUT[{}] local_put_ok: {}, remote_ok_count: {}, quorum: {},no errors",
                        key, local_put_ok, remote_ok_count, self.quorum
                    );
                    Err(err)
                }
            } else {
                warn!("PUT[{}] succeed, but some data get into alien", key);
                Ok(())
            }
        }
    }

    async fn background_put(
        self,
        mut rest_tasks: FuturesUnordered<JoinHandle<Result<NodeOutput<()>, NodeOutput<Error>>>>,
        key: BobKey,
        data: BobData,
        mut failed_nodes: Vec<String>,
    ) {
        debug!("PUT[{}] ~~~BACKGROUND PUT TO REMOTE NODES~~~", key);
        while let Some(join_res) = rest_tasks.next().await {
            match join_res {
                Ok(res) => match res {
                    Ok(n) => debug!(
                        "PUT[{}] successful background put to: {}",
                        key,
                        n.node_name()
                    ),
                    Err(e) => {
                        error!("{:?}", e);
                        failed_nodes.push(e.node_name().to_string());
                    }
                },
                Err(e) => error!("{:?}", e),
            }
        }
        debug!("PUT[{}] ~~~PUT TO REMOTE NODES ALIEN~~~", key);
        if let Err(e) = self.put_aliens(failed_nodes, key, data).await {
            error!("{}", e);
        }
    }

    pub(crate) async fn put_remote_nodes(
        &self,
        key: BobKey,
        data: BobData,
        at_least: usize,
    ) -> (
        FuturesUnordered<JoinHandle<Result<NodeOutput<()>, NodeOutput<Error>>>>,
        Vec<NodeOutput<Error>>,
    ) {
        let local_node = self.mapper.local_node_name();
        let target_nodes = get_target_nodes(&self.mapper, key)
            .iter()
            .filter(|node| node.name() != local_node);
        put_at_least(key, data, target_nodes, at_least, PutOptions::new_local()).await
    }

    pub(crate) async fn put_aliens(
        &self,
        mut failed_nodes: Vec<String>,
        key: BobKey,
        data: BobData,
    ) -> Result<(), Error> {
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
        if local_put.is_err() {
            debug!(
                "PUT[{}] local put failed, need additional remote alien put",
                key
            );
            failed_nodes.push(self.mapper.local_node_name().to_string());
        }
        let target_nodes = get_target_nodes(&self.mapper, key);
        let target_indexes = target_nodes.iter().map(Node::index);
        let mut sup_nodes = get_support_nodes(&self.mapper, target_indexes, failed_nodes.len())?;
        debug!("PUT[{}] sup put nodes: {:?}", key, &sup_nodes);

        let mut queries = Vec::new();

        if let Err(op) = local_put {
            let item = sup_nodes.remove(sup_nodes.len() - 1);
            queries.push((item, op));
        }

        if !failed_nodes.is_empty() {
            let put_options = PutOptions::new_alien(failed_nodes);
            queries.extend(
                sup_nodes
                    .into_iter()
                    .map(|node| (node, put_options.clone())),
            );
        }

        let mut sup_ok_count = queries.len();
        let mut err = String::new();
        debug!("PUT[{}] additional alien requests: {:?}", key, queries);

        if let Err((sup_ok_count_l, err_l)) = put_sup_nodes(key, data, &queries).await {
            sup_ok_count = sup_ok_count_l;
            err = err_l;
        }
        if err.is_empty() {
            Ok(())
        } else {
            let msg = format!(
                "PUT[{}] failed: ok: {}, quorum: {}, errors: {}",
                key, sup_ok_count, self.quorum, err
            );
            let e = Error::failed(msg);
            Err(e)
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
