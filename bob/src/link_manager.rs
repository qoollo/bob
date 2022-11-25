use crate::prelude::*;

use termion::color;

const FAST_PING_PERIOD_MS: u64 = 100;
const FAST_PING_DURATION_SEC: u64 = 60;

#[derive(Debug)]
pub(crate) struct LinkManager {
    nodes: Arc<[Node]>,
    check_interval: Duration,
}

pub(crate) type ClusterCallOutput<T> = Result<NodeOutput<T>, NodeOutput<Error>>;
pub(crate) type ClusterCallFuture<'a, T> =
    Pin<Box<dyn Future<Output = ClusterCallOutput<T>> + Send + 'a>>;

impl LinkManager {
    pub(crate) fn new(nodes: &[Node], check_interval: Duration) -> LinkManager {
        LinkManager {
            nodes: Arc::from(nodes),
            check_interval,
        }
    }

    async fn checker_task(factory: Factory, nodes: Arc<[Node]>, period: Duration) {
        let now = coarsetime::Clock::now_since_epoch().as_secs();
        Self::checker(
            &factory,
            &nodes,
            Duration::from_millis(FAST_PING_PERIOD_MS),
            || {
                let ts = coarsetime::Clock::now_since_epoch().as_secs();
                ts > now && ts - now > FAST_PING_DURATION_SEC
            },
            period.as_millis() as usize / FAST_PING_PERIOD_MS as usize,
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
        let mut i = 1;
        while !should_stop() {
            let log = i % log_iteration_div == 0;
            if log {
                i = 1;
            } else {
                i += 1;
            }
            interval.tick().await;
            let mut err_cnt = 0;
            let mut status = String::from("Node status: ");
            for node in nodes.iter() {
                if let Err(e) = node.check(&factory).await {
                    if log {
                        error!(
                            "No connection to {}:[{}] - {}",
                            node.name(),
                            node.address(),
                            e
                        );
                        status += &format!("{}{:<10} ", color::Fg(color::Red), node.name());
                    }
                    err_cnt += 1;
                } else {
                    if log {
                        status += &format!("{}{:<10} ", color::Fg(color::Green), node.name());
                    }
                }
            }
            if log {
                info!("{}{}", status, color::Fg(color::Reset));
                let cnt = nodes.len() - err_cnt;
                gauge!(AVAILABLE_NODES_COUNT, cnt as f64);
            }
        }
    }

    pub(crate) fn spawn_checker(&self, factory: Factory) {
        let nodes = self.nodes.clone();
        tokio::spawn(Self::checker_task(factory, nodes, self.check_interval));
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
}
