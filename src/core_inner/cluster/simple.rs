use super::prelude::*;
use crate::core_inner::backend::Exist;
use crate::core_inner::bob_client::ExistResult;

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
    fn get_target_nodes(&self, key: BobKey) -> Vec<Node> {
        self.mapper.get_vdisk(key).nodes.clone()
    }
}

impl Cluster for Quorum {
    fn put_clustered_async(&self, key: BobKey, data: BobData) -> BackendPut {
        let target_nodes = self.get_target_nodes(key);

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

        debug!(
            "GET[{}]: Nodes for fan out: {:?}",
            key,
            print_vec(&target_nodes)
        );
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
                Ok(cluster_result.result.clone())
            } else {
                Err(BackendError::KeyNotFound(key))
            }
        };
        BackendGet(task.boxed())
    }

    fn exist_clustered_async(&self, keys: &[BobKey]) -> Exist {
        let mut keys_by_nodes: HashMap<_, Vec<_>> = HashMap::new();
        for &key in keys {
            keys_by_nodes
                .entry(self.get_target_nodes(key))
                .and_modify(|v| v.push(key))
                .or_insert_with(|| vec![key]);
        }
        debug!(
            "EXIST Nodes for fan out: {:?}",
            print_vec(&keys_by_nodes.keys().flat_map(|v| v).collect::<Vec<_>>())
        );
        let keys = keys.to_vec();
        Exist(
            async move {
                let mut exist = Vec::with_capacity(keys.len());
                exist.extend((0..keys.len()).map(|_| false));
                for (nodes, current_keys) in keys_by_nodes {
                    let indexes = current_keys
                        .iter()
                        .map(|k| keys.iter().position(|k1| k.eq(k1)).unwrap())
                        .collect::<Vec<_>>();
                    let res: Vec<ExistResult> = LinkManager::call_nodes(&nodes, |mut conn| {
                        conn.exist(current_keys.clone(), GetOptions::new_all()).0
                    })
                    .collect()
                    .await;
                    for result in res {
                        if let Ok(result) = result {
                            for (ind, r) in result.result.exist.iter().enumerate() {
                                exist[indexes[ind]] |= *r;
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
