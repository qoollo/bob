use crate::link_manager::LinkManager;
use crate::prelude::*;
use super::support_types::{RemoteDeleteError, RemotePutResponse, RemotePutError};

pub(crate) type Tasks<Res, Err> = FuturesUnordered<JoinHandle<Result<NodeOutput<Res>, NodeOutput<Err>>>>;

// ======================= Helpers =================

pub(crate) trait AffectedReplicasProvider {
    fn get_affected_replicas_count(&self) -> usize;
}

impl AffectedReplicasProvider for () {
    fn get_affected_replicas_count(&self) -> usize {
        1
    }
}

impl AffectedReplicasProvider for RemotePutResponse {
    fn get_affected_replicas_count(&self) -> usize {
        self.affected_replicas()
    }
}

fn process_result<TRes: AffectedReplicasProvider, TErr: Debug>(
    join_res: Result<Result<NodeOutput<TRes>, NodeOutput<TErr>>, JoinError>,
    oks: &mut Vec<NodeOutput<TRes>>,
    errors: &mut Vec<NodeOutput<TErr>>
) -> usize {
    debug!("handle returned");
    match join_res {
        Ok(res) => match res {
            Ok(r) =>
            {
                let affected_replicas = r.inner().get_affected_replicas_count();
                oks.push(r);
                return affected_replicas;
            }
            Err(e) => {
                debug!("{:?}", e);
                errors.push(e);
            }
        },
        Err(e) => {
            error!("{:?}", e);
        }
    }
    0
}

pub(crate) async fn finish_at_least_handles<TRes: AffectedReplicasProvider, TErr: Debug>(
    handles: &mut Tasks<TRes, TErr>,
    at_least: usize,
) -> (Vec<NodeOutput<TRes>>, Vec<NodeOutput<TErr>>) {
    let mut ok_count = 0;
    let mut errors = Vec::new();
    let mut oks = Vec::with_capacity(at_least.min(handles.len()));
    while ok_count < at_least {
        if let Some(join_res) = handles.next().await {
            ok_count += process_result(join_res, &mut oks, &mut errors);
        } else {
            break;
        }
    }
    trace!("ok_count/at_least: {}/{}", ok_count, at_least);
    (oks, errors)
}

async fn call_at_least<TOp, TRes: AffectedReplicasProvider, TErr: Debug>(
    target_nodes: impl Iterator<Item = TOp>,
    at_least: usize,
    f: impl Fn(TOp) -> JoinHandle<Result<NodeOutput<TRes>, NodeOutput<TErr>>>
) -> (FuturesUnordered<JoinHandle<Result<NodeOutput<TRes>, NodeOutput<TErr>>>>, Vec<NodeOutput<TRes>>, Vec<NodeOutput<TErr>>) {
    let mut handles: FuturesUnordered<_> = target_nodes.map(|op| f(op)).collect();
    trace!("total handles count: {}", handles.len());
    let (oks, errors) = finish_at_least_handles(&mut handles, at_least).await;
    trace!("remains: {}, oks: {}, errors: {}", handles.len(), oks.len(), errors.len());
    (handles, oks, errors)
}


async fn finish_all_handles<TErr: Debug>(
    handles: &mut FuturesUnordered<JoinHandle<Result<NodeOutput<()>, NodeOutput<TErr>>>>
) -> Vec<NodeOutput<TErr>> {
    let mut ok_count = 0;
    let mut total_count = 0;
    let mut oks = Vec::with_capacity(handles.len());
    let mut errors = Vec::new();
    while let Some(join_res) = handles.next().await {
        total_count += 1;
        ok_count += process_result(join_res, &mut oks, &mut errors);
    }
    trace!("ok_count/total: {}/{}", ok_count, total_count);
    errors
}

async fn call_all<TOp, TErr: Debug>(
    operations: impl Iterator<Item = TOp>,
    f: impl Fn(TOp) -> JoinHandle<Result<NodeOutput<()>, NodeOutput<TErr>>>,
) -> (usize, Vec<NodeOutput<TErr>>) {
    let mut handles: FuturesUnordered<_> = operations.map(|op| f(op)).collect();
    let handles_len = handles.len();
    if handles_len == 0 {
        return (0, Vec::new());
    }
    trace!("total handles count: {}", handles_len);
    let errors = finish_all_handles(&mut handles).await;
    trace!("errors/total: {}/{}", errors.len(), handles_len);
    (handles_len, errors)
}

// ======================== GET ==========================

pub(crate) async fn get_any(
    key: BobKey,
    target_nodes: impl Iterator<Item = &Node>,
    options: GetOptions,
) -> Option<NodeOutput<BobData>> {
    let requests: FuturesUnordered<_> = target_nodes
        .map(|node| LinkManager::call_node(node, |conn| conn.get(key, options.clone()).boxed()))
        .collect();
    requests
        .filter_map(|res| future::ready(res.ok()))
        .next()
        .await
}

pub(crate) async fn lookup_local_alien(
    backend: &Backend,
    key: BobKey,
    vdisk_id: VDiskId,
) -> Option<BobData> {
    let op = Operation::new_alien(vdisk_id);
    match backend.get_local(key, op).await {
        Ok(data) => {
            debug!("GET[{}] key found in local node alien", key);
            return Some(data);
        }
        Err(e) if e.is_key_not_found() => debug!("GET[{}] not found in local alien", key),
        Err(e) => error!("local node backend returned error: {}", e),
    };
    None
}

pub(crate) async fn lookup_local_node(
    backend: &Backend,
    key: BobKey,
    vdisk_id: VDiskId,
    disk_path: DiskPath,
) -> Option<BobData> {
    debug!("local node has vdisk replica, check local");
    let op = Operation::new_local(vdisk_id, disk_path);
    match backend.get_local(key, op).await {
        Ok(data) => {
            debug!("GET[{}] key found in local node", key);
            return Some(data);
        }
        Err(e) if e.is_key_not_found() => debug!("GET[{}] not found in local node", key),
        Err(e) => error!("local node backend returned error: {}", e),
    }
    None
}

pub(crate) async fn lookup_remote_aliens(mapper: &Virtual, key: BobKey) -> Option<BobData> {
    let local_node = mapper.local_node_name();
    let target_nodes = mapper
        .nodes()
        .iter()
        .filter(|node| node.name() != local_node);
    let result = get_any(key, target_nodes, GetOptions::new_alien()).await;
    if let Some(answer) = result {
        debug!(
            "GET[{}] take data from node: {}, timestamp: {}",
            key,
            answer.node_name(),
            answer.timestamp()
        );
        Some(answer.into_inner())
    } else {
        debug!("GET[{}] data not found on any node in alien dir", key);
        None
    }
}

pub(crate) async fn lookup_remote_nodes(mapper: &Virtual, key: BobKey) -> Option<BobData> {
    let local_node = mapper.local_node_name();
    let target_nodes = mapper
        .get_target_nodes_for_key(key)
        .iter()
        .filter(|node| node.name() != local_node);
    let result = get_any(key, target_nodes, GetOptions::new_local()).await;
    if let Some(answer) = result {
        debug!(
            "GET[{}] take data from node: {}, timestamp: {}",
            key,
            answer.node_name(),
            answer.timestamp()
        );
        Some(answer.into_inner())
    } else {
        debug!("GET[{}] data not found on any node in regular dir", key);
        None
    }
}

// ==================== PUT ======================

fn call_node_put(
    key: BobKey,
    data: BobData,
    node: Node,
    options: BobPutOptions,
    affected_replicas: usize
) -> JoinHandle<Result<NodeOutput<RemotePutResponse>, NodeOutput<RemotePutError>>> {
    debug!("PUT[{}] put to {}", key, node.name());
    let task = async move {
        let grpc_options = options.to_grpc();
        let call_result = LinkManager::call_node(&node, |conn| conn.put(key, data, grpc_options).boxed()).await;
        call_result
            .map(|o| o.map(|_| RemotePutResponse::new(affected_replicas)))
            .map_err(|o| o.map(|e| RemotePutError::new(affected_replicas, e)))
    };
    tokio::spawn(task)
}


pub(crate) async fn put_at_least(
    key: BobKey,
    data: &BobData,
    target_nodes: impl Iterator<Item = &Node>,
    at_least: usize,
    options: BobPutOptions,
    affected_replicas_by_node: &HashMap<NodeName, usize>
) -> (Tasks<RemotePutResponse, RemotePutError>, Vec<NodeOutput<RemotePutResponse>>, Vec<NodeOutput<RemotePutError>>) {
    call_at_least(target_nodes, at_least, |n| {
        call_node_put(key, data.clone(), n.clone(), options.clone(),
                      *affected_replicas_by_node.get(n.name()).unwrap_or(&1))
    })
    .await
}


pub(crate) async fn put_local_all(
    backend: &Backend,
    node_names: Vec<NodeName>,
    key: BobKey,
    data: &BobData,
    operation: Operation,
) -> Result<(), BobPutOptions> {
    let mut add_nodes = vec![];
    for node_name in node_names {
        let mut op = operation.clone();
        op.set_remote_node_name(node_name.clone());
        debug!("PUT[{}] put to local alien: {:?}", key, node_name);

        if let Err(e) = backend.put_local(key, data, op).await {
            debug!("PUT[{}] local support put result: {:?}", key, e);
            add_nodes.push(node_name);
        }
    }

    if add_nodes.is_empty() {
        Ok(())
    } else {
        Err(BobPutOptions::new_alien(add_nodes))
    }
}


pub(crate) async fn put_sup_nodes(
    key: BobKey,
    data: &BobData,
    requests: impl Iterator<Item = (&Node, BobPutOptions)>
) -> Result<(), Vec<NodeOutput<Error>>> {
    let mut ret = vec![];
    for (node, options) in requests {
        let result = LinkManager::call_node(node, |client| {
            Box::pin(client.put(key, data.clone(), options.to_grpc()))
        })
        .await;
        debug!("{:?}", result);
        if let Err(e) = result {
            let target_node = options.remote_nodes()[0].clone(); // TODO: should return all nodes
            ret.push(NodeOutput::new(target_node, e.into_inner()));
        }
    }

    if ret.is_empty() {
        Ok(())
    } else {
        Err(ret)
    }
}

pub(crate) async fn put_local_node_all(
    backend: &Backend,
    key: BobKey,
    data: &BobData,
    vdisk_id: VDiskId,
    disk_paths: smallvec::SmallVec<[DiskPath; 1]>
) -> usize {
    let mut successes = 0;
    for path in disk_paths {
        let res = put_local_node(&backend, key, data, vdisk_id, path).await;
        if let Err(e) = res {
            error!("{}", e);
        } else {
            successes += 1;
            debug!("PUT[{}] local node put successful", key);
        }
    }
    successes
}

pub(crate) async fn put_local_node(
    backend: &Backend,
    key: BobKey,
    data: &BobData,
    vdisk_id: VDiskId,
    disk_path: DiskPath,
) -> Result<(), Error> {
    debug!("local node has vdisk replica, put local");
    let op = Operation::new_local(vdisk_id, disk_path);
    backend.put_local(key, data, op).await
}

// =================== EXIST ==================

pub(crate) async fn exist_on_local_node(
    backend: &Backend,
    keys: &[BobKey],
) -> Result<Vec<bool>, Error> {
    Ok(backend
        .exist(keys, &BobGetOptions::new_local())
        .await?)
}

pub(crate) async fn exist_on_local_alien(
    backend: &Backend,
    keys: &[BobKey],
) -> Result<Vec<bool>, Error> {
    Ok(backend
        .exist(keys, &BobGetOptions::new_alien())
        .await?)
}

pub(crate) async fn exist_on_remote_nodes(
    keys_by_node: &HashMap<NodeName, (Node, Vec<BobKey>)>,
) -> Vec<Result<NodeOutput<Vec<bool>>, NodeOutput<Error>>> {
    LinkManager::call_nodes(keys_by_node.values().map(|(n, _)| n), |client| {
        Box::pin(client.exist(
            keys_by_node.get(client.target_node_name()).expect("map is based on nodes from values").1.clone(),
            GetOptions::new_local(),
        ))
    })
    .await
}

pub(crate) async fn exist_on_remote_aliens(
    nodes: &[&Node],
    keys: &[BobKey],
) -> Vec<Result<NodeOutput<Vec<bool>>, NodeOutput<Error>>> {
    LinkManager::call_nodes(nodes.iter().map(|n| *n), |client| {
        Box::pin(client.exist(keys.to_vec(), GetOptions::new_alien()))
    })
    .await
}

// =================== DELETE =================

pub(crate) async fn delete_on_local_node(
    backend: &Backend,
    key: BobKey,
    meta: &BobMeta,
    vdisk_id: VDiskId,
    disk_path: DiskPath,
) -> Result<(), Error> {
    trace!("local node has vdisk replica, delete local");
    let op = Operation::new_local(vdisk_id, disk_path);
    backend.delete_local(key, meta, op, true).await?;
    Ok(())
}

pub(crate) async fn delete_on_local_aliens(
    backend: &Backend,
    key: BobKey,
    meta: &BobMeta,
    all_nodes_for_key: &[Node],
    force_nodes: HashSet<NodeName>,
    vdisk_id: VDiskId,
) -> Result<(), Vec<NodeName>> {
    let mut fully_failed_nodes = vec![];

    for node in all_nodes_for_key {
        let mut op = Operation::new_alien(vdisk_id);
        let node_name = node.name().clone();
        trace!("DELETE[{}] delete to local alien: {:?}", key, node_name);
        let force_delete = force_nodes.contains(&node_name);
        op.set_remote_node_name(node_name);
        if let Err(e) = backend.delete_local(key, meta, op, force_delete).await {
            trace!("DELETE[{}] local alien delete result: {:?}", key, e);
            if force_delete {
                fully_failed_nodes.push(node.name().clone());
            }
        }
    }

    if fully_failed_nodes.is_empty() {
        Ok(())
    } else {
        Err(fully_failed_nodes)
    }
}


fn call_node_delete(
    key: BobKey,
    meta: BobMeta,
    options: BobDeleteOptions,
    node: Node,
) -> JoinHandle<Result<NodeOutput<()>, NodeOutput<RemoteDeleteError>>> {
    trace!("DELETE[{}] delete to {}", key, node.name());
    let task = async move {
        let force_alien_nodes_copy = options.force_delete_nodes().iter().cloned().collect();
        let grpc_options = options.to_grpc();
        let call_result = LinkManager::call_node(&node, |conn| conn.delete(key, meta, grpc_options).boxed()).await;
        call_result.map_err(|err| err.map(|inner| RemoteDeleteError::new(force_alien_nodes_copy, inner)))
    };
    tokio::spawn(task)
}


pub(super) async fn delete_on_remote_nodes(
    key: BobKey,
    meta: &BobMeta,
    requests: impl Iterator<Item = (&Node, BobDeleteOptions)>,
) -> Result<(), Vec<NodeOutput<RemoteDeleteError>>> {

    let (_, errors) = call_all(requests, |(node, options)| { call_node_delete(key, meta.clone(), options, node.clone()) }).await;

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

pub(super) async fn delete_on_remote_nodes_with_options(
    key: BobKey,
    meta: &BobMeta,
    target_nodes: Vec<&Node>,
    options: BobDeleteOptions
) -> Vec<NodeOutput<RemoteDeleteError>> {
    if target_nodes.is_empty() {
        return Vec::new();
    }

    let remote_delete_result = delete_on_remote_nodes(key, meta, 
        target_nodes.into_iter().map(|n| (n, options.clone()))
    ).await;

    if let Err(errors) = remote_delete_result {
        return errors;
    }

    return Vec::new();
}
