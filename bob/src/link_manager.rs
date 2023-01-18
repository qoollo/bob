use tokio::sync::mpsc::{channel, Receiver, Sender};
use std::sync::RwLock;

use crate::prelude::*;

#[derive(Debug)]
pub(crate) struct LinkManager {
    nodes: Arc<[Node]>,
    check_interval: Duration,
    node_check_queue: Arc<RwLock<Option<Sender<String>>>>,
}

pub(crate) type ClusterCallOutput<T> = Result<NodeOutput<T>, NodeOutput<Error>>;
pub(crate) type ClusterCallFuture<'a, T> =
    Pin<Box<dyn Future<Output = ClusterCallOutput<T>> + Send + 'a>>;

impl LinkManager {
    pub(crate) fn new(nodes: &[Node], check_interval: Duration) -> LinkManager {
        LinkManager {
            nodes: Arc::from(nodes),
            check_interval,
            node_check_queue: Arc::new(RwLock::new(None)),
        }
    }

    async fn checker_task(factory: Factory, nodes: Arc<[Node]>, period: Duration) {
        let mut interval = interval(period);
        loop {
            interval.tick().await;
            let mut err_cnt = 0;
            let mut status = String::from("Node status: ");
            for node in nodes.iter() {
                if let Err(e) = node.check(&factory).await {
                    error!(
                        "No connection to {}:[{}] - {}",
                        node.name(),
                        node.address(),
                        e
                    );
                    status += &format!("[-]{:<10} ", node.name());
                    err_cnt += 1;
                } else {
                    status += &format!("[+]{:<10} ", node.name());
                }
            }
            info!("{}", status);
            let cnt = nodes.len() - err_cnt;
            gauge!(AVAILABLE_NODES_COUNT, cnt as f64);
        }
    }

    async fn priority_nodes_checker(
        nodes: Arc<[Node]>,
        factory: Factory,
        mut node_check_queue: Receiver<String>,
    ) {
        while let Some(name) = node_check_queue.recv().await {
            if let Some(node) = nodes.iter().find(|n| n.name() == name) {
                if !node.connection_is_set() {
                    if let Err(e) = node.check(&factory).await {
                        error!(
                            "No connection to {}:[{}] - {}",
                            node.name(),
                            node.address(),
                            e
                        );
                    } else {
                        debug!("Create connection in response to ping from {}", node.name());
                    }
                }
            }
        }
    }

    pub(crate) async fn spawn_checker(&self, factory: Factory) {
        let (sender, receiver) = channel(self.nodes.len() * 2);
        self.node_check_queue.write().expect("rwlock").replace(sender);
        tokio::spawn(Self::checker_task(
            factory.clone(),
            self.nodes.clone(),
            self.check_interval,
        ));
        tokio::spawn(Self::priority_nodes_checker(
            self.nodes.clone(),
            factory,
            receiver,
        ));
    }

    pub(crate) async fn call_nodes<F, T>(
        nodes: impl Iterator<Item = &Node>,
        f: F,
    ) -> Vec<ClusterCallOutput<T>>
    where
        F: FnMut(&'_ BobClient) -> ClusterCallFuture<'_, T> + Send + Clone,
        T: Send,
    {
        let futures: FuturesUnordered<_> =
            nodes.map(|node| Self::call_node(node, f.clone())).collect();
        futures.collect().await
    }

    pub(crate) async fn call_node<F, T>(node: &Node, f: F) -> ClusterCallOutput<T>
    where
        F: FnOnce(&'_ BobClient) -> ClusterCallFuture<'_, T> + Send + Clone,
        T: Send,
    {
        match node.get_connection() {
            Some(conn) => f(&conn).await,
            None => Err(NodeOutput::new(
                node.name().to_owned(),
                Error::failed(format!("No active connection {:?}", node)),
            )),
        }
    }

    pub(crate) async fn exist_on_nodes(
        nodes: &[Node],
        keys: &[BobKey],
    ) -> Vec<Result<NodeOutput<Vec<bool>>, NodeOutput<Error>>> {
        Self::call_nodes(nodes.iter(), |client| {
            Box::pin(client.exist(keys.to_vec(), GetOptions::new_all()))
        })
        .await
    }

    pub(crate) fn update_node_connection(&self, node_name: &str) {
        if let Some(node) = self.nodes.iter().find(|n| n.name() == node_name) {
            if let Some(queue) = self.node_check_queue.read().expect("rwlock").as_ref() {
                if !node.connection_is_set() {
                    if let Err(e) = queue.try_send(node_name.to_string()) {
                        warn!("error while updating node {} status: {}", node_name, e);
                    }
                }
            }
        }
    }
}
