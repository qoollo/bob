use crate::prelude::*;

use super::Cluster;
use crate::link_manager::LinkManager;

pub(crate) struct Quorum {
    mapper: Arc<Virtual>,
    quorum: usize,
}

impl Quorum {
    pub(crate) fn new(mapper: Arc<Virtual>, quorum: usize) -> Self {
        Self { mapper, quorum }
    }

    #[inline]
    fn get_target_nodes(&self, key: BobKey) -> Vec<Node> {
        self.mapper
            .get_vdisk_for_key(key)
            .expect("get vdisk for key failed")
            .nodes()
            .to_vec()
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

    async fn perform_on_nodes<F, T>(
        &self,
        key: BobKey,
        metrics_prefix: &str,
        client_fun: F,
    ) -> Result<(), Error>
    where
        F: FnMut(&'_ BobClient) -> crate::link_manager::ClusterCallFuture<'_, T> + Send + Clone,
        T: Send + Debug,
    {
        let target_nodes = self.get_target_nodes(key);

        debug!(
            "{}[{}]: Nodes for fan out: {:?}",
            metrics_prefix, key, &target_nodes
        );

        let l_quorum = self.quorum as usize;
        let reqs = LinkManager::call_nodes(target_nodes.iter(), client_fun);
        let results = reqs.await;
        let total_count = results.len();
        let errors = results.iter().filter(|r| r.is_err()).collect::<Vec<_>>();
        let ok_count = total_count - errors.len();
        debug!(
            "{}[{}] total requests: {} ok: {} quorum: {}",
            metrics_prefix, key, total_count, ok_count, l_quorum
        );
        // TODO: send actuall list of vdisk it has been written on
        if ok_count >= l_quorum {
            Ok(())
        } else {
            Err(Error::failed(format!(
                "failed: total requests: {}, ok: {}, quorum: {}, errors: {:?}",
                total_count, ok_count, l_quorum, errors
            )))
        }
    }
}

#[async_trait]
impl Cluster for Quorum {
    async fn put(&self, key: BobKey, data: BobData) -> Result<(), Error> {
        self.perform_on_nodes(key, "PUT", |c| {
            Box::pin(c.put(
                key,
                data.clone(),
                PutOptions {
                    remote_nodes: vec![], //TODO check
                    force_node: true,
                    overwrite: false,
                },
            ))
        })
        .await
    }

    async fn get(&self, key: BobKey) -> Result<BobData, Error> {
        let target_nodes = self.get_target_nodes(key);
        debug!("GET[{}]: Nodes for fan out: {:?}", key, &target_nodes);
        let reqs = LinkManager::call_nodes(target_nodes.iter(), |conn| {
            conn.get(key, GetOptions::new_local()).boxed()
        });
        let results = reqs.await;
        let ok_results = results
            .iter()
            .filter_map(|r| r.as_ref().ok())
            .collect::<Vec<_>>();

        ok_results
            .get(0)
            .map_or(Err(Error::key_not_found(key)), |res| {
                Ok(res.inner().clone())
            })
    }

    async fn exist(&self, keys: &[BobKey]) -> Result<Vec<bool>, Error> {
        let keys_by_nodes = self.group_keys_by_nodes(keys);
        debug!(
            "EXIST Nodes for fan out: {:?}",
            &keys_by_nodes.keys().flatten().collect::<Vec<_>>()
        );
        let len = keys.len();
        let mut exist = vec![false; len];
        for (nodes, (keys, indexes)) in keys_by_nodes {
            let res: Vec<_> = LinkManager::exist_on_nodes(&nodes, &keys).await;
            for result in res.into_iter().flatten() {
                for (&r, &ind) in result.inner().iter().zip(&indexes) {
                    exist[ind] |= r;
                }
            }
        }
        Ok(exist)
    }

    async fn delete(&self, key: BobKey) -> Result<(), Error> {
        self.perform_on_nodes(key, "DELETE", move |c| Box::pin(c.delete(key)))
            .await
    }
}
