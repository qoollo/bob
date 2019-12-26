use super::prelude::*;

pub struct Quorum {
    mapper: Arc<VDiskMapper>,
    quorum: u8,
}

impl Quorum {
    pub fn new(mapper: Arc<VDiskMapper>, config: &NodeConfig) -> Self {
        Self {
            quorum: config.quorum.expect("get quorum config"),
            mapper,
        }
    }

    #[inline]
    fn calc_target_nodes(&self, key: BobKey) -> Vec<Node> {
        self.mapper.get_vdisk(key).nodes.clone()
    }
}

impl Cluster for Quorum {
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

        let task = async move {
            let results = reqs.collect::<Vec<_>>().await;
            let err_results = results
                .iter()
                .filter_map(|r| r.as_ref().err())
                .collect::<Vec<_>>();
            let errors = err_results // @TODO refactoring of the error logs
                .iter()
                .map(ToString::to_string)
                .collect::<String>();
            trace!("GET[{}] errors: {}", key, errors);
            let mut ok_results = results
                .into_iter()
                .filter_map(|r| r.ok())
                .collect::<Vec<_>>();
            if ok_results.is_empty() {
                Err(BackendError::KeyNotFound(key))
            } else {
                let cluster_result = ok_results.remove(0);
                Ok(cluster_result.result)
            }
        }
        .boxed();
        BackendGet(task)
    }
}
