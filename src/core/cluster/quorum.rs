use super::prelude::*;

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
}

#[async_trait]
impl Cluster for Quorum {
    async fn put(&self, key: BobKey, data: BobData) -> Result<(), Error> {
        debug!("get nodes of the target vdisk");
        let target_nodes = get_target_nodes(&self.mapper, key);
        debug!("PUT[{}]: Nodes for fan out: {:?}", key, &target_nodes);
        debug!("call put on target nodes (on target vdisk)");
        let results = LinkManager::call_nodes(target_nodes.iter(), |conn| {
            Box::pin(conn.put(key, data.clone(), PutOptions::new_local()))
        })
        .await;
        debug!("PUT[{}] rcv {} cluster answers", key, results.len());

        let total_ops = results.len();
        debug!("filter out ok results");
        let errors = results
            .into_iter()
            .filter_map(Result::err)
            .collect::<Vec<_>>();
        let ok_count = total_ops - errors.len();

        debug!("ok: {}/{} quorum: {}", ok_count, total_ops, self.quorum);
        if ok_count == total_ops {
            Ok(())
        } else {
            let mut additional_remote_writes = match ok_count {
                0 => self.quorum, //@TODO take value from config
                value if value < self.quorum => 1,
                _ => 0,
            };

            let vdisk_id = self.mapper.id_from_key(key);
            debug!("get names of the failed nodes");
            let node_names = errors.iter().map(|n| n.node_name().to_owned()).collect();
            debug!("create operation Backend alien, id: {}", vdisk_id);
            let operation = Operation::new_alien(vdisk_id);
            let local_put =
                put_local_all(&self.backend, node_names, key, data.clone(), operation).await;

            if local_put.is_err() {
                debug!("local put failed, add another remote node");
                additional_remote_writes += 1;
            }
            let target_indexes = target_nodes.iter().map(Node::index);
            let mut sup_nodes =
                get_support_nodes(&self.mapper, target_indexes, additional_remote_writes)?;
            debug!("PUT[{}] sup put nodes: {:?}", key, &sup_nodes);

            let mut queries = Vec::new();

            if let Err(op) = local_put {
                let item = sup_nodes.remove(sup_nodes.len() - 1);
                queries.push((item, op));
            }

            if additional_remote_writes > 0 {
                let nodes = errors
                    .into_iter()
                    .map(|res| res.node_name().to_owned())
                    .collect::<Vec<_>>();
                let put_options = PutOptions::new_alien(nodes);

                queries.extend(
                    sup_nodes
                        .into_iter()
                        .map(|node| (node, put_options.clone())),
                );
            }

            let mut sup_ok_count = queries.len();
            let mut err = String::new();

            if let Err((sup_ok_count_l, err_l)) = put_sup_nodes(key, data, &queries).await {
                sup_ok_count = sup_ok_count_l;
                err = err_l;
            }
            if sup_ok_count + ok_count >= self.quorum {
                Ok(())
            } else {
                let msg = format!(
                    "failed: total: {}, ok: {}, quorum: {}, errors: {}",
                    total_ops,
                    ok_count + sup_ok_count,
                    self.quorum,
                    err
                );
                let e = Error::failed(msg);
                Err(e)
            }
        }
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
