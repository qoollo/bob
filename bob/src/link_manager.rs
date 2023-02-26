use tokio::sync::mpsc::{channel, Receiver, Sender, error::TrySendError};
use std::sync::RwLock;

use crate::prelude::*;
use std::time::Instant;

const FAST_PING_PERIOD_MS: u64 = 100;
const FAST_PING_DURATION_SEC: u64 = 60;

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
        let start = Instant::now();
        let fast_log_iteration_div = 
            (period.as_millis() as usize / FAST_PING_PERIOD_MS as usize).max(1);
        Self::checker(
            &factory,
            &nodes,
            Duration::from_millis(FAST_PING_PERIOD_MS).min(period),
            || start.elapsed().as_secs() > FAST_PING_DURATION_SEC,
            fast_log_iteration_div,
        )
        .await;
        Self::checker(&factory, &nodes, period, || false, 1).await;
    }

    async fn checker(
        factory: &Factory,
        nodes: &[Node],
        period: Duration,
        should_stop: impl Fn() -> bool,
        log_iteration_div: usize,
    ) {
        let mut interval = interval(period);
        let mut i: usize = 1;
        while !should_stop() {
            i = i.wrapping_add(1) % log_iteration_div;
            let log_in_this_iter = i == 0;
            interval.tick().await;
            let mut err_cnt = 0;
            let mut status = String::from("Node status: ");
            for node in nodes.iter() {
                if let Err(e) = node.check(&factory).await {
                    if log_in_this_iter {
                        error!(
                            "No connection to {}:[{}] - {}",
                            node.name(),
                            node.address(),
                            e
                        );
                        status += &format!("[-]{:<10} ", node.name());
                    }
                    err_cnt += 1;
                } else {
                    if log_in_this_iter {
                        status += &format!("[+]{:<10} ", node.name());
                    }
                }
            }
            if log_in_this_iter {
                info!("{}", status);
            }
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
            if let Some(node) = nodes.iter().find(|n| n.name() == &name) {
                if !node.connection_available() {
                    if let Err(err) = node.check(&factory).await {
                        error!(
                            "Unable to connect to {}:[{}] after the ping was received from it : {}",
                            node.name(),
                            node.address(),
                            err
                        );
                    } else {
                        debug!("Create connection in response to ping from {}", node.name());
                    }
                }
            }
        }
    }

    pub(crate) fn spawn_checker(&self, factory: Factory) {
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
            if !node.connection_available() {
                if let Some(queue) = self.node_check_queue.read().expect("rwlock").as_ref() {
                    if let Err(e) = queue.try_send(node_name.to_string()) {
                        match e {
                            TrySendError::Full(node_name) => warn!("Too many pings received from nodes. Queue overflowed on node: {}", node_name),
                            TrySendError::Closed(_) => error!("Reconnection channel in link_manager closed"),
                        }
                    }
                }
            }
        }
    }
}
