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

    async fn put_at_least(self, key: BobKey, data: BobData) -> Result<(), Error> {
        debug!("PUT[{}] ~~~PUT LOCAL NODE FIRST~~~", key);
        let mut ok_count = 0;
        let mut fails_count = 0;
        let (vdisk_id, disk_path) = self.mapper.get_operation(key);
        if let Some(path) = disk_path {
            debug!("disk path is present, try put local");
            let res = put_local_node(&self.backend, key, data.clone(), vdisk_id, path).await;
            match res {
                Ok(()) => {
                    ok_count += 1;
                    debug!("local node put successful");
                }
                Err(e) => {
                    fails_count += 1;
                    error!("{}", e);
                }
            }
        } else {
            debug!("skip local put");
        }
        let at_least = self.quorum - ok_count;
        if at_least > 0 {
            debug!("PUT[{}] ~~~PUT TO REMOTE NODES~~~", key);
            let (tasks, errors) = self.put_remote_nodes(key, data.clone(), at_least).await;
            fails_count += errors.len();
            tokio::spawn(self.background_put(tasks, key, data, fails_count));
        } else {
            debug!("PUT[{}] ~~~SKIP PUT TO REMOTE NODES~~~", key);
        }
        Ok(())
    }

    async fn background_put(
        self,
        mut rest_tasks: FuturesUnordered<JoinHandle<Result<NodeOutput<()>, NodeOutput<Error>>>>,
        key: BobKey,
        data: BobData,
        mut fails_count: usize,
    ) {
        let mut failed_nodes = Vec::new();
        while let Some(join_res) = rest_tasks.next().await {
            match join_res {
                Ok(res) => match res {
                    Ok(_) => {}
                    Err(e) => {
                        error!("{:?}", e);
                        failed_nodes.push(e.node_name().to_string());
                        fails_count += 1;
                    }
                },
                Err(e) => error!("{:?}", e),
            }
        }
        debug!("PUT[{}] ~~~PUT TO REMOTE NODES ALIEN~~~", key);
        if let Err(e) = self.put_aliens(failed_nodes, key, data, fails_count).await {
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
        failed_nodes: Vec<String>,
        key: BobKey,
        data: BobData,
        count: usize,
    ) -> Result<(), Error> {
        let vdisk_id = self.mapper.id_from_key(key);
        let operation = Operation::new_alien(vdisk_id);
        let local_put =
            put_local_all(&self.backend, failed_nodes, key, data.clone(), operation).await;
        let target_nodes = get_target_nodes(&self.mapper, key);
        let target_indexes = target_nodes.iter().map(Node::index);
        let mut sup_nodes = get_support_nodes(&self.mapper, target_indexes, count)?;
        debug!("PUT[{}] sup put nodes: {:?}", key, &sup_nodes);

        let mut queries = Vec::new();

        if let Err(op) = local_put {
            let item = sup_nodes.remove(sup_nodes.len() - 1);
            queries.push((item, op));
        }

        let mut sup_ok_count = queries.len();
        let mut err = String::new();

        if let Err((sup_ok_count_l, err_l)) = put_sup_nodes(key, data, &queries).await {
            sup_ok_count = sup_ok_count_l;
            err = err_l;
        }
        if err.is_empty() {
            Ok(())
        } else {
            let msg = format!(
                "failed: ok: {}, quorum: {}, errors: {}",
                count + sup_ok_count,
                self.quorum,
                err
            );
            let e = Error::failed(msg);
            Err(e)
        }
    }
}

#[async_trait]
impl Cluster for Quorum {
    async fn put(&self, key: BobKey, data: BobData) -> Result<(), Error> {
        self.clone().put_at_least(key, data).await
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
