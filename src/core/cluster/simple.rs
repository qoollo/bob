use super::prelude::*;
use crate::core::backend::Exist;
use crate::core::bob_client::ExistResult;

pub(crate) struct Quorum {
    mapper: Arc<VDiskMapper>,
    quorum: u8,
}

impl Quorum {
    pub(crate) fn new(mapper: Arc<VDiskMapper>, config: &NodeConfig) -> Self {
        Self {
            quorum: config.quorum.expect("get quorum config"),
            mapper,
        }
    }

    #[inline]
    fn get_target_nodes(&self, key: BobKey) -> Vec<Node> {
        self.mapper.get_vdisk(key).nodes().to_vec()
    }

    fn group_keys_by_nodes(
        &self,
        keys: &[BobKey],
    ) -> HashMap<Vec<Node>, (Vec<BobKey>, Vec<usize>)> {
        let mut keys_by_nodes: HashMap<_, (Vec<_>, Vec<_>)> = HashMap::new();
        for (ind, &key) in keys.iter().enumerate() {
            keys_by_nodes
                .entry(self.get_target_nodes(key))
                .and_modify(|(keys, indexes)| {
                    keys.push(key);
                    indexes.push(ind);
                })
                .or_insert_with(|| (vec![key], vec![ind]));
        }
        keys_by_nodes
    }
}

impl Cluster for Quorum {
    fn put_clustered_async(&self, key: BobKey, data: BobData) -> BackendPut {
        let target_nodes = self.get_target_nodes(key);

        debug!("PUT[{}]: Nodes for fan out: {:?}", key, &target_nodes);

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
        });

        let l_quorum = self.quorum as usize;
        let task = async move {
            let results = reqs
                .inspect(|r| debug!("PUT[{}] cluster ans: {:?}", key, r))
                .collect::<Vec<_>>()
                .await;
            let total_count = results.len();
            let errors = results.iter().filter(|r| r.is_err()).collect::<Vec<_>>();
            let ok_count = total_count - errors.len();
            debug!(
                "PUT[{}] total requests: {} ok: {} quorum: {}",
                key, total_count, ok_count, l_quorum
            );
            // TODO: send actuall list of vdisk it has been written on
            if ok_count >= l_quorum {
                Ok(BackendPutResult {})
            } else {
                Err(backend::Error::Failed(format!(
                    "failed: total requests: {}, ok: {}, quorum: {}, errors: {:?}",
                    total_count, ok_count, l_quorum, errors
                )))
            }
        };
        BackendPut(task.boxed())
    }

    fn get_clustered_async(&self, key: BobKey) -> BackendGet {
        let target_nodes = self.get_target_nodes(key);

        debug!("GET[{}]: Nodes for fan out: {:?}", key, &target_nodes);
        let reqs = LinkManager::call_nodes(&target_nodes, |mut conn| {
            conn.get(key, GetOptions::new_normal()).0
        });

        let task = async move {
            let results = reqs
                .inspect(|r| debug!("GET[{}] cluster ans: {:?}", key, r))
                .collect::<Vec<_>>()
                .await;
            let ok_results = results
                .iter()
                .filter_map(|r| r.as_ref().ok())
                .collect::<Vec<_>>();

            if let Some(cluster_result) = ok_results.get(0) {
                Ok(cluster_result.inner().clone())
            } else {
                Err(BackendError::KeyNotFound(key))
            }
        };
        BackendGet(task.boxed())
    }

    fn exist_clustered_async(&self, keys: &[BobKey]) -> Exist {
        let keys_by_nodes = self.group_keys_by_nodes(keys);
        debug!(
            "EXIST Nodes for fan out: {:?}",
            &keys_by_nodes.keys().flat_map(|v| v).collect::<Vec<_>>()
        );
        let len = keys.len();
        Exist(
            async move {
                let mut exist = vec![false; len];
                for (nodes, (keys, indexes)) in keys_by_nodes {
                    let res: Vec<ExistResult> = LinkManager::exist_on_nodes(&nodes, keys).await;
                    for result in res {
                        if let Ok(result) = result {
                            for (&r, &ind) in result.inner().exist.iter().zip(&indexes) {
                                exist[ind] |= r;
                            }
                        }
                    }
                }
                Ok(BackendExistResult { exist })
            }
            .boxed(),
        )
    }
}
