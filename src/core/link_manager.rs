use crate::core::{
    backend::Error,
    bob_client::{BobClient, BobClientFactory},
    data::{ClusterResult, Node},
};
use futures::{future::Future, stream::Stream};
use std::{
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use futures03::{
    compat::Future01CompatExt,
    future::{err, FutureExt as OtherFutureExt, TryFutureExt},
    task::{Spawn, SpawnExt},
    Future as Future03,
};

use tokio_timer::Interval;

pub struct LinkManager {
    repo: Arc<Vec<Node>>,
    check_interval: Duration,
}

pub type ClusterCallType<T> = Result<ClusterResult<T>, ClusterResult<Error>>;
pub type ClusterCallFuture<T> =
    Pin<Box<dyn Future03<Output = ClusterCallType<T>> + 'static + Send>>;

impl LinkManager {
    pub fn new(nodes: &Vec<Node>, check_interval: Duration) -> LinkManager {
        LinkManager {
            repo: Arc::new(nodes.to_vec()),
            check_interval,
        }
    }

    pub async fn get_checker_future<S>(
        &self,
        client_factory: BobClientFactory,
        spawner: S,
    ) -> Result<(), ()>
    where
        S: Spawn + Clone + Send + 'static + Unpin + Sync,
    {
        let local_repo = self.repo.clone();
        Interval::new_interval(self.check_interval)
            .for_each(move |_| {
                local_repo.iter().for_each(|v| {
                    let q = v.clone().check(client_factory.clone());
                    let _ = spawner
                        .clone()
                        .spawn(q.map(|_r| {}))
                        .map_err(|e| panic!("can't run timer task {:?}", e));
                });

                Ok(())
            })
            .map_err(|e| panic!("can't make to work timer {:?}", e))
            .compat()
            .boxed()
            .await
    }

    pub fn call_nodes<F: Send, T: 'static + Send>(
        &self,
        nodes: &[Node],
        mut f: F,
    ) -> Vec<ClusterCallFuture<T>>
    where
        F: FnMut(&mut BobClient) -> ClusterCallFuture<T>,
    {
        nodes
            .iter()
            .map(move |nl| {
                let nl_clone = nl.clone();
                match &mut nl.get_connection() {
                    Some(conn) => f(conn)
                        .boxed()
                        .map_err(move |e| {
                            if e.result.is_service() {
                                nl_clone.clear_connection();
                            }
                            e
                        })
                        .boxed(),
                    None => err(ClusterResult {
                        result: Error::Failed(format!("No active connection {:?}", nl)),
                        node: nl.clone(),
                    })
                    .boxed(),
                }
            })
            .collect()
    }

    pub fn call_nodes_direct<F: Send, T: 'static + Send>(
        &self,
        requests: (&Node, F)
    ) -> Vec<ClusterCallFuture<T>>
    where
        F: FnMut(&mut BobClient) -> ClusterCallFuture<T>,
    {
        unimplemented!();
        // let t: Vec<_> = nodes
        //     .iter()
        //     .map(move |nl| {
        //         let nl_clone = nl.clone();
        //         match &mut nl.get_connection() {
        //             Some(conn) => f(conn)
        //                 .boxed()
        //                 .map_err(move |e| {
        //                     if e.result.is_service() {
        //                         nl_clone.clear_connection();
        //                     }
        //                     e
        //                 })
        //                 .boxed(),
        //             None => err(ClusterResult {
        //                 result: Error::Failed(format!("No active connection {:?}", nl)),
        //                 node: nl.clone(),
        //             })
        //             .boxed(),
        //         }
        //     })
        //     .collect();
        // t
    }
}
