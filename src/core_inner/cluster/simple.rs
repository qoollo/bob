use super::prelude::*;

pub struct SimpleQuorumCluster {
    mapper: Arc<VDiskMapper>,
    quorum: u8,
}

impl SimpleQuorumCluster {
    pub fn new(mapper: Arc<VDiskMapper>, config: &NodeConfig) -> Self {
        SimpleQuorumCluster {
            quorum: config.quorum.unwrap(),
            mapper,
        }
    }

    #[inline]
    fn calc_target_nodes(&self, key: BobKey) -> Vec<Node> {
        self.mapper.get_vdisk(key).nodes.clone()
    }
}

impl Cluster for SimpleQuorumCluster {
    fn put_clustered_async(&self, key: BobKey, data: BobData) -> BackendPut {
        let target_nodes = self.calc_target_nodes(key);

        debug!(
            "PUT[{}]: Nodes for fan out: {:?}",
            key,
            print_vec(&target_nodes)
        );

        let reqs = LinkManager::call_nodes(&target_nodes, |mut mock_bob_client| {
            mock_bob_client
                .put(
                    key,
                    &data,
                    PutOptions {
                        remote_nodes: vec![], //TODO check
                        force_node: true,
                        overwrite: false,
                    },
                )
                .0
        })
        .into_iter()
        .collect::<FuturesUnordered<_>>();

        let l_quorum = self.quorum;
        let task = reqs
            .map(move |r| {
                trace!("PUT[{}] Response from cluster {:?}", key, r);
                r // wrap all result kind to process it later
            })
            .collect::<Vec<_>>()
            .map(move |acc| {
                debug!("PUT[{}] cluster ans: {:?}", key, acc);
                let total_ops = acc.iter().count();
                let mut sup = String::new();
                let ok_count = acc
                    .iter()
                    .filter(|&r| {
                        if let Err(e) = r {
                            sup += &e.to_string();
                        }
                        r.is_ok()
                    })
                    .count();
                debug!(
                    "PUT[{}] total reqs: {} succ reqs: {} quorum: {}",
                    key, total_ops, ok_count, l_quorum
                );
                // TODO: send actuall list of vdisk it has been written on
                if ok_count >= l_quorum as usize {
                    Ok(BackendPutResult {})
                } else {
                    Err(backend::Error::Failed(format!(
                        "failed: total: {}, ok: {}, quorum: {}, sup: {}",
                        total_ops, ok_count, l_quorum, sup
                    )))
                }
            })
            .boxed();
        BackendPut(task)
    }

    fn get_clustered_async(&self, key: BobKey) -> BackendGet {
        let target_nodes = self.calc_target_nodes(key);

        debug!(
            "GET[{}]: Nodes for fan out: {:?}",
            key,
            print_vec(&target_nodes)
        );
        let reqs = LinkManager::call_nodes(&target_nodes, |mut conn| {
            conn.get(key, GetOptions::new_normal()).0
        })
        .into_iter()
        .collect::<FuturesUnordered<_>>();

        let task = reqs
            .collect::<Vec<_>>()
            .map(move |acc| {
                let sup = acc
                    .iter()
                    .filter_map(|r| r.as_ref().err())
                    .map(|e| e.to_string())
                    .collect();

                let r = acc.into_iter().find(|r| r.is_ok());
                if let Some(answer) = r {
                    match answer {
                        Ok(ClusterResult { result: i, .. }) => Ok(i),
                        Err(ClusterResult { result: i, .. }) => Err(i),
                    }
                } else {
                    debug!("GET[{}] no success result", key);
                    Err(BackendError::Failed(sup))
                }
            })
            .boxed();
        BackendGet(task)
    }
}
