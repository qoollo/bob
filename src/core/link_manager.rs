use super::prelude::*;

#[derive(Debug)]
pub(crate) struct LinkManager {
    repo: Arc<Vec<Node>>,
    check_interval: Duration,
}

pub(crate) type ClusterCallType<T> = Result<NodeOutput<T>, NodeOutput<BackendError>>;
pub(crate) type ClusterCallFuture<T> = Pin<Box<dyn Future<Output = ClusterCallType<T>> + Send>>;

impl LinkManager {
    pub(crate) fn new(nodes: &[Node], check_interval: Duration) -> LinkManager {
        LinkManager {
            repo: Arc::new(nodes.to_vec()),
            check_interval,
        }
    }

    pub(crate) fn spawn_checker(&self, client_factory: Factory) {
        let local_repo = self.repo.clone();
        let mut interval = interval(self.check_interval);
        let task = async move {
            loop {
                interval.tick().await;
                for node in local_repo.iter() {
                    node.check(client_factory.clone()).await.expect("check");
                }
            }
        };
        tokio::spawn(task);
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
                    None => future::err(NodeOutput::new(
                        nl.name().to_owned(),
                        BackendError::Failed(format!("No active connection {:?}", nl)),
                    ))
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
            None => future::err(NodeOutput::new(
                node.name().to_owned(),
                BackendError::Failed(format!("No active connection {:?}", node)),
            ))
            .boxed(),
        }
    }

    pub(crate) async fn exist_on_nodes(
        nodes: &[Node],
        keys: Vec<BobKey>,
    ) -> Vec<Result<NodeOutput<BackendExistResult>, NodeOutput<BackendError>>> {
        Self::call_nodes(&nodes, |mut conn| {
            conn.exist(keys.clone(), GetOptions::new_all()).0
        })
        .collect()
        .await
    }
}
