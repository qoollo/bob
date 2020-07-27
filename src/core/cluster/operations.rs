use super::prelude::*;
use tokio::task::JoinHandle;

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

pub(crate) async fn put_at_least(
    key: BobKey,
    data: BobData,
    target_nodes: impl Iterator<Item = &Node>,
    at_least: usize,
    options: PutOptions,
) -> (
    FuturesUnordered<JoinHandle<Result<NodeOutput<()>, NodeOutput<Error>>>>,
    Vec<NodeOutput<Error>>,
) {
    let mut handles: FuturesUnordered<_> = target_nodes
        .map(|node| {
            debug!("PUT[{}] put to {}", key, node.name());
            let node = node.clone();
            let data = data.clone();
            let options = options.clone();
            tokio::spawn(async move {
                LinkManager::call_node(&node, |conn| {
                    conn.put(key, data.clone(), options.clone()).boxed()
                })
                .await
            })
        })
        .collect();
    debug!("total handles count: {}", handles.len());
    let mut ok_count = 0;
    let mut errors = Vec::new();
    while ok_count < at_least {
        if let Some(join_res) = handles.next().await {
            debug!("handle returned");
            match join_res {
                Ok(res) => match res {
                    Ok(_) => ok_count += 1,
                    Err(e) => {
                        error!("{:?}", e);
                        errors.push(e);
                    }
                },
                Err(e) => {
                    error!("{:?}", e);
                }
            }
            if ok_count == at_least {
                break;
            }
        } else {
            break;
        }
    }
    debug!("ok_count/at_least: {}/{}", ok_count, at_least);
    debug!("remains: {}, errors: {}", handles.len(), errors.len());
    (handles, errors)
}

pub(crate) fn get_support_nodes<'a>(
    mapper: &'a Virtual,
    mut target_indexes: impl Iterator<Item = u16>,
    count: usize,
) -> Result<Vec<&'a Node>, Error> {
    let (len, _) = target_indexes.size_hint();
    debug!("iterator size lower bound: {}", len);
    trace!("nodes available: {}", mapper.nodes().len());
    if mapper.nodes().len() < len + count {
        let msg = "cannot find enough support nodes".to_owned();
        error!("{}", msg);
        Err(Error::failed(msg))
    } else {
        let sup = mapper
            .nodes()
            .iter()
            .filter(|node| target_indexes.all(|i| i != node.index()))
            .take(count)
            .collect();
        Ok(sup)
    }
}

#[inline]
pub(crate) fn get_target_nodes(mapper: &Virtual, key: BobKey) -> &[Node] {
    mapper.get_vdisk_for_key(key).nodes()
}

pub(crate) fn group_keys_by_nodes(
    mapper: &Virtual,
    keys: &[BobKey],
) -> HashMap<Vec<Node>, (Vec<BobKey>, Vec<usize>)> {
    let mut keys_by_nodes: HashMap<_, (Vec<_>, Vec<_>)> = HashMap::new();
    for (ind, &key) in keys.iter().enumerate() {
        keys_by_nodes
            .entry(get_target_nodes(mapper, key).to_vec())
            .and_modify(|(keys, indexes)| {
                keys.push(key);
                indexes.push(ind);
            })
            .or_insert_with(|| (vec![key], vec![ind]));
    }
    keys_by_nodes
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
    disk_path: Option<DiskPath>,
) -> Option<BobData> {
    if let Some(path) = disk_path {
        debug!("local node has vdisk replica, check local");
        let op = Operation::new_local(vdisk_id, path);
        match backend.get_local(key, op).await {
            Ok(data) => {
                debug!("GET[{}] key found in local node", key);
                return Some(data);
            }
            Err(e) if e.is_key_not_found() => debug!("GET[{}] not found in local node", key),
            Err(e) => error!("local node backend returned error: {}", e),
        }
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
    let target_nodes = get_target_nodes(mapper, key)
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

pub(crate) async fn put_local_all(
    backend: &Backend,
    node_names: Vec<String>,
    key: BobKey,
    data: BobData,
    operation: Operation,
) -> Result<(), PutOptions> {
    let mut add_nodes = vec![];
    for node_name in node_names {
        let mut op = operation.clone();
        op.set_remote_folder(node_name.clone());
        debug!("PUT[{}] put to local alien: {:?}", key, node_name);

        if let Err(e) = backend.put_local(key, data.clone(), op).await {
            debug!("PUT[{}] local support put result: {:?}", key, e);
            add_nodes.push(node_name);
        }
    }

    if add_nodes.is_empty() {
        Ok(())
    } else {
        Err(PutOptions::new_alien(add_nodes))
    }
}

pub(crate) async fn put_sup_nodes(
    key: BobKey,
    data: BobData,
    requests: &[(&Node, PutOptions)],
) -> Result<(), (usize, String)> {
    let mut ret = vec![];
    for (node, options) in requests {
        let result = LinkManager::call_node(&node, |client| {
            Box::pin(client.put(key, data.clone(), options.clone()))
        })
        .await;
        debug!("{:?}", result);
        if let Err(e) = result {
            ret.push(e);
        }
    }

    if ret.is_empty() {
        Ok(())
    } else {
        let msg = ret.iter().map(|x| format!("{:?}\n", x)).collect();
        Err((requests.len() - ret.len(), msg))
    }
}

pub(crate) async fn put_local_node(
    backend: &Backend,
    key: BobKey,
    data: BobData,
    vdisk_id: VDiskId,
    disk_path: DiskPath,
) -> Result<(), Error> {
    debug!("local node has vdisk replica, put local");
    let op = Operation::new_local(vdisk_id, disk_path);
    backend.put_local(key, data, op).await
}
