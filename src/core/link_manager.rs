use super::prelude::*;

#[derive(Debug)]
pub(crate) struct LinkManager {
    nodes: Arc<Vec<Node>>,
    check_interval: Duration,
}

pub(crate) type ClusterCallType<T> = Result<NodeOutput<T>, NodeOutput<BackendError>>;
pub(crate) type ClusterCallFuture<T> = Pin<Box<dyn Future<Output = ClusterCallType<T>> + Send>>;

impl LinkManager {
    pub(crate) fn new(nodes: Vec<Node>, check_interval: Duration) -> LinkManager {
        LinkManager {
            nodes: Arc::new(nodes),
            check_interval,
        }
    }

    pub(crate) fn spawn_checker(&self, client_factory: Factory) {
        let nodes = self.nodes.clone();
        let mut interval = interval(self.check_interval);
        let task = async move {
            loop {
                interval.tick().await;
                for node in nodes.iter() {
                    node.check(&client_factory).await.expect("check");
                }
            }
        };
        tokio::spawn(task);
    }

    pub(crate) fn call_nodes<F, T>(nodes: &[Node], f: F) -> FuturesUnordered<ClusterCallFuture<T>>
    where
        F: FnMut(BobClient) -> ClusterCallFuture<T> + Send + Clone,
        T: 'static + Send,
    {
        nodes
            .iter()
            .map(|node| Self::call_node(node, f.clone()))
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
        let mut results = Vec::new();
        for node in nodes {
            let client = node.get_connection();
            if let Some(client) = client {
                let res = client.exist(keys.clone(), GetOptions::new_all()).0.await;
                results.push(res);
            }
        }
        results
    }
}
