use super::prelude::*;

pub(crate) struct LinkManager {
    repo: Arc<Vec<Node>>,
    check_interval: Duration,
}

pub(crate) type ClusterCallType<T> = Result<ClusterResult<T>, ClusterResult<BackendError>>;
pub(crate) type ClusterCallFuture<T> = Pin<Box<dyn Future<Output = ClusterCallType<T>> + Send>>;

impl LinkManager {
    pub(crate) fn new(nodes: &[Node], check_interval: Duration) -> LinkManager {
        LinkManager {
            repo: Arc::new(nodes.to_vec()),
            check_interval,
        }
    }

    pub(crate) async fn get_checker_future(&self, client_factory: Factory) -> Result<(), ()> {
        let local_repo = self.repo.clone();
        interval(self.check_interval)
            .map(move |_| {
                local_repo.iter().for_each(|v| {
                    let q = v.clone().check(client_factory.clone()).map(|_| {});
                    tokio::spawn(q);
                });
            })
            .collect::<Vec<_>>()
            .boxed()
            .await;
        Ok(())
    }

    pub(crate) fn call_nodes<F, T>(
        nodes: &[Node],
        mut f: F,
    ) -> FuturesUnordered<ClusterCallFuture<T>>
    where
        F: FnMut(BobClient) -> ClusterCallFuture<T> + Send,
        T: 'static + Send,
    {
        nodes
            .iter()
            .map(move |nl| {
                let client = nl.get_connection();
                match client {
                    Some(conn) => f(conn).boxed(),
                    None => future::err(ClusterResult {
                        result: BackendError::Failed(format!("No active connection {:?}", nl)),
                        node: nl.clone(),
                    })
                    .boxed(),
                }
            })
            .collect()
    }

    pub(crate) fn call_node<F, T>(node: &Node, mut f: F) -> ClusterCallFuture<T>
    where
        F: FnMut(BobClient) -> ClusterCallFuture<T>,
        T: 'static + Send,
    {
        match node.get_connection() {
            Some(conn) => f(conn).boxed(),
            None => future::err(ClusterResult {
                result: BackendError::Failed(format!("No active connection {:?}", node)),
                node: node.clone(),
            })
            .boxed(),
        }
    }

    pub(crate) async fn exist_on_nodes(
        nodes: &[Node],
        keys: Vec<BobKey>,
    ) -> Vec<Result<ClusterResult<BackendExistResult>, ClusterResult<BackendError>>> {
        Self::call_nodes(&nodes, |mut conn| {
            conn.exist(keys.clone(), GetOptions::new_all()).0
        })
        .collect()
        .await
    }
}
