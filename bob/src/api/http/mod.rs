use crate::{
    api::http::metric_models::MetricsSnapshotModel, build_info::BuildInfo,
    server::Server as BobServer,
};
use axum::{
    body::{self, BoxBody},
    extract::{BodyStream, Extension, Path as AxumPath},
    response::IntoResponse,
    routing::{delete, get, post},
    AddExtensionLayer, Json, Router, Server,
};
use bob_backend::pearl::{Group as PearlGroup, Holder};
use bob_common::{
    data::{BobData, BobKey, BobMeta, BobOptions, VDisk as DataVDisk, BOB_KEY_SIZE},
    error::Error as BobError,
    node::Disk as NodeDisk,
};
use bytes::Bytes;
use futures::{future::BoxFuture, FutureExt};
use http::{header::CONTENT_TYPE, HeaderMap, Response, StatusCode};
use hyper::Body;
use std::net::{IpAddr, SocketAddr};
use std::{
    io::{Cursor, Error as IoError, ErrorKind},
    path::{Path, PathBuf},
    str::FromStr,
};
use tokio::fs::{read_dir, ReadDir};

mod metric_models;
mod s3;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Action {
    Attach,
    Detach,
}

#[derive(Debug, Serialize)]
pub(crate) struct Node {
    name: String,
    address: String,
    vdisks: Vec<VDisk>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct VDisk {
    id: u32,
    replicas: Vec<Replica>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct Replica {
    node: String,
    disk: String,
    path: String,
}
#[derive(Debug, Serialize, Clone)]
pub(crate) struct VDiskPartitions {
    vdisk_id: u32,
    node_name: String,
    disk_name: String,
    partitions: Vec<String>,
}

#[derive(Debug, Serialize, Clone)]
pub(crate) struct Partition {
    vdisk_id: u32,
    node_name: String,
    disk_name: String,
    timestamp: u64,
    records_count: usize,
}

#[derive(Debug)]
pub(crate) struct StatusExt {
    status: StatusCode,
    ok: bool,
    msg: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct Dir {
    name: String,
    path: String,
    children: Vec<Dir>,
}

#[derive(Debug, Serialize)]
pub(crate) struct DistrFunc {
    func: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct DiskState {
    name: String,
    path: String,
    is_active: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct DataKey(BobKey);

#[derive(Debug, Serialize)]
pub(crate) struct Version {
    version: String,
    build_time: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct VersionInfo {
    bob_version: Version,
    pearl_version: Version,
}

#[derive(Debug, Serialize)]
pub(crate) struct NodeConfiguration {
    blob_file_name_prefix: String,
}

pub(crate) fn spawn(bob: BobServer, address: IpAddr, port: u16) {
    let router = Router::new()
        .route("/status", get(status))
        .route("/metrics", get(metrics))
        .route("/version", get(version))
        .route("/nodes", get(nodes))
        .route("/disks/list", get(disks_list))
        .route("/metadata/distrfunc", get(distribution_function))
        .route("/configuration", get(get_node_configuration))
        .route("/disks/:disk_name/stop", post(stop_all_disk_controllers))
        .route("/disks/:disk_name/start", post(start_all_disk_controllers))
        .route("/vdisks", get(vdisks))
        .route("/blobs/outdated", delete(finalize_outdated_blobs))
        .route("/vdisks/:vdisk_id", get(vdisk_by_id))
        .route("/vdisks/:vdisk_id/records/count", get(vdisk_records_count))
        .route("/vdisks/:vdisk_id/partitions", get(partitions))
        .route(
            "/vdisks/:vdisk_id/partitions/:partition_id",
            get(partition_by_id),
        )
        .route(
            "/vdisks/:vdisk_id/partitions/by_timestamp/:timestamp/:action",
            post(change_partition_state),
        )
        .route("/vdisks/:vdisk_id/remount", post(remount_vdisks_group))
        .route(
            "/vdisks/:vdisk_id/partitions/by_timestamp/:timestamp",
            delete(delete_partition),
        )
        .route("/alien", get(alien))
        .route("/alien/detach", post(detach_alien_partitions))
        .route("/alien/dir", get(get_alien_directory))
        .route(
            "/vdisks/:vdisk_id/replicas/local/dirs",
            get(get_local_replica_directories),
        )
        .route("/data/:key", get(get_data))
        .route("/data/:key", post(put_data))
        // .route("/s3", s3::routes())
        .layer(AddExtensionLayer::new(bob));

    let task = Server::bind(&SocketAddr::new(address, port)).serve(router.into_make_service());

    tokio::spawn(task);

    info!("API server started");
}

fn data_vdisk_to_scheme(disk: &DataVDisk) -> VDisk {
    VDisk {
        id: disk.id(),
        replicas: collect_replicas_info(disk.replicas()),
    }
}

fn collect_disks_info(bob: &BobServer) -> Vec<VDisk> {
    let mapper = bob.grinder().backend().mapper();
    mapper.vdisks().values().map(data_vdisk_to_scheme).collect()
}

#[inline]
fn get_vdisk_by_id(bob: &BobServer, id: u32) -> Option<VDisk> {
    find_vdisk(bob, id).map(data_vdisk_to_scheme)
}

fn find_vdisk(bob: &BobServer, id: u32) -> Option<&DataVDisk> {
    let mapper = bob.grinder().backend().mapper();
    mapper.get_vdisk(id)
}

fn collect_replicas_info(replicas: &[NodeDisk]) -> Vec<Replica> {
    replicas
        .iter()
        .map(|r| Replica {
            path: r.disk_path().to_owned(),
            disk: r.disk_name().to_owned(),
            node: r.node_name().to_owned(),
        })
        .collect()
}

fn not_acceptable_backend() -> StatusExt {
    let status = StatusExt::new(
        StatusCode::NOT_ACCEPTABLE,
        false,
        "only pearl backend supports partitions".into(),
    );
    warn!("{:?}", status);
    status
}

// !notice: only finds normal group
async fn find_group(bob: &BobServer, vdisk_id: u32) -> Result<PearlGroup, StatusExt> {
    let backend = bob.grinder().backend().inner();
    debug!("get backend: OK");
    let (dcs, _) = backend
        .disk_controllers()
        .ok_or_else(not_acceptable_backend)?;
    debug!("get vdisks groups: OK");
    let needed_dc = dcs
        .iter()
        .find(|dc| dc.vdisks().iter().any(|&vd| vd == vdisk_id))
        .ok_or_else(|| {
            let err = format!("Disk Controller with vdisk #{} not found", vdisk_id);
            warn!("{}", err);
            StatusExt::new(StatusCode::NOT_FOUND, false, err)
        })?;
    needed_dc.vdisk_group(vdisk_id).await.map_err(|_| {
        let err = format!("Disk Controller with vdisk #{} not found", vdisk_id);
        warn!("{}", err);
        StatusExt::new(StatusCode::NOT_FOUND, false, err)
    })
}

async fn status(Extension(bob): Extension<BobServer>) -> Json<Node> {
    let mapper = bob.grinder().backend().mapper();
    let name = mapper.local_node_name().to_owned();
    let address = mapper.local_node_address().to_owned();
    let vdisks = collect_disks_info(&bob);
    let node = Node {
        name,
        address,
        vdisks,
    };
    Json(node)
}

async fn metrics(Extension(bob): Extension<BobServer>) -> Json<MetricsSnapshotModel> {
    let snapshot = bob.metrics().read().await.clone();
    Json(snapshot.into())
}

async fn version() -> Json<VersionInfo> {
    let build_info = BuildInfo::new();
    let version_info = VersionInfo {
        bob_version: Version {
            version: build_info.version().to_string(),
            build_time: build_info.build_time().to_string(),
        },
        pearl_version: Version {
            version: build_info.pearl_version().to_string(),
            build_time: build_info.pearl_build_time().to_string(),
        },
    };
    Json(version_info)
}

async fn nodes(Extension(bob): Extension<&BobServer>) -> Json<Vec<Node>> {
    let mapper = bob.grinder().backend().mapper();
    let mut nodes = vec![];
    let vdisks = collect_disks_info(bob);
    for node in mapper.nodes().values() {
        let vdisks: Vec<VDisk> = vdisks
            .iter()
            .filter_map(|vd| {
                if vd.replicas.iter().any(|r| r.node == node.name()) {
                    let mut vd = vd.clone();
                    for i in 0..vd.replicas.len() {
                        if vd.replicas[i].node != node.name() {
                            vd.replicas.remove(i);
                        }
                    }
                    Some(vd)
                } else {
                    None
                }
            })
            .collect();

        let node = Node {
            name: node.name().to_string(),
            address: node.address().to_string(),
            vdisks,
        };

        nodes.push(node);
    }
    Json(nodes)
}

async fn disks_list(
    Extension(bob): Extension<BobServer>,
) -> Result<Json<Vec<DiskState>>, StatusExt> {
    let backend = bob.grinder().backend().inner();
    let (dcs, alien_disk_controller) = backend
        .disk_controllers()
        .ok_or_else(not_acceptable_backend)?;

    let mut disks = Vec::new();
    for dc in dcs.iter().chain(std::iter::once(&alien_disk_controller)) {
        let disk_path = dc.disk();
        disks.push(DiskState {
            name: disk_path.name().to_owned(),
            path: disk_path.path().to_owned(),
            is_active: dc.is_ready().await,
        });
    }

    Ok(Json(disks))
}

async fn distribution_function(Extension(bob): Extension<BobServer>) -> Json<DistrFunc> {
    let mapper = bob.grinder().backend().mapper();
    Json(DistrFunc {
        func: format!("{:?}", mapper.distribution_func()),
    })
}

async fn get_node_configuration(Extension(bob): Extension<BobServer>) -> Json<NodeConfiguration> {
    let grinder = bob.grinder();
    let config = grinder.node_config();
    Json(NodeConfiguration {
        blob_file_name_prefix: config.pearl().blob_file_name_prefix().to_owned(),
    })
}

// #[post("/disks/<disk_name>/stop")]
async fn stop_all_disk_controllers(
    Extension(bob): Extension<BobServer>,
    AxumPath(disk_name): AxumPath<String>,
) -> Result<StatusExt, StatusExt> {
    use futures::stream::{FuturesUnordered, StreamExt};
    let backend = bob.grinder().backend().inner();
    let (dcs, alien_disk_controller) = backend
        .disk_controllers()
        .ok_or_else(not_acceptable_backend)?;
    dcs.iter()
        .chain(std::iter::once(&alien_disk_controller))
        .filter(|dc| dc.disk().name() == disk_name)
        .map(|dc| dc.stop())
        .collect::<FuturesUnordered<_>>()
        .collect::<Vec<()>>()
        .await;
    Ok(StatusExt::new(
        StatusCode::OK,
        true,
        "Disk controllers are stopped".to_owned(),
    ))
}

async fn start_all_disk_controllers(
    Extension(bob): Extension<BobServer>,
    disk_name: String,
) -> Result<StatusExt, StatusExt> {
    use futures::stream::{FuturesUnordered, StreamExt};
    let backend = bob.grinder().backend().inner();
    let (dcs, alien_disk_controller) = backend
        .disk_controllers()
        .ok_or_else(not_acceptable_backend)?;
    let target_dcs = dcs
        .iter()
        .chain(std::iter::once(&alien_disk_controller))
        .filter(|dc| dc.disk().name() == disk_name)
        .map(|dc| dc.run())
        .collect::<FuturesUnordered<_>>();
    if target_dcs.is_empty() {
        let err = format!("Disk Controller with name '{}' not found", disk_name);
        warn!("{}", err);
        return Err(StatusExt::new(StatusCode::NOT_FOUND, false, err));
    }
    let err_string = target_dcs
        .fold(String::new(), |mut err_string, res| {
            if let Err(e) = res {
                err_string.push_str(&(e.to_string() + "\n"));
            }
            async move { err_string }
        })
        .await;
    if err_string.is_empty() {
        let msg = format!(
            "all disk controllers for disk '{}' successfully started",
            disk_name
        );
        info!("{}", msg);
        Ok(StatusExt::new(StatusCode::OK, true, msg))
    } else {
        Err(StatusExt::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            false,
            err_string,
        ))
    }
}

async fn vdisks(Extension(bob): Extension<&BobServer>) -> Json<Vec<VDisk>> {
    let vdisks = collect_disks_info(bob);
    Json(vdisks)
}

async fn finalize_outdated_blobs(Extension(bob): Extension<BobServer>) -> StatusExt {
    let backend = bob.grinder().backend();
    backend.close_unneeded_active_blobs(1, 1).await;
    StatusExt::new(
        StatusCode::OK,
        true,
        "Successfully removed outdated blobs".to_string(),
    )
}

async fn vdisk_by_id(
    Extension(bob): Extension<&BobServer>,
    AxumPath(vdisk_id): AxumPath<u32>,
) -> Result<Json<VDisk>, StatusExt> {
    get_vdisk_by_id(bob, vdisk_id)
        .map(Json)
        .ok_or(StatusExt::new(
            StatusCode::NOT_FOUND,
            false,
            "vdisk not found".to_string(),
        ))
}

async fn vdisk_records_count(
    Extension(bob): Extension<BobServer>,
    AxumPath(vdisk_id): AxumPath<u32>,
) -> Result<Json<u64>, StatusExt> {
    let group = find_group(&bob, vdisk_id).await?;
    let holders = group.holders();
    let pearls = holders.read().await;
    let pearls: &[_] = pearls.as_ref();
    let mut sum = 0;
    for pearl in pearls {
        sum += pearl.records_count().await;
    }
    Ok(Json(sum as u64))
}

async fn partitions(
    Extension(bob): Extension<&BobServer>,
    AxumPath(vdisk_id): AxumPath<u32>,
) -> Result<Json<VDiskPartitions>, StatusExt> {
    let group = find_group(bob, vdisk_id).await?;
    debug!("group with provided vdisk_id found");
    let holders = group.holders();
    let pearls = holders.read().await;
    debug!("get pearl holders: OK");
    let pearls: &[_] = pearls.as_ref();
    let partitions = pearls.iter().map(Holder::get_id).collect();
    let ps = VDiskPartitions {
        node_name: group.node_name().to_owned(),
        disk_name: group.disk_name().to_owned(),
        vdisk_id: group.vdisk_id(),
        partitions,
    };
    trace!("partitions: {:?}", ps);
    Ok(Json(ps))
}

async fn partition_by_id(
    Extension(bob): Extension<&BobServer>,
    AxumPath(vdisk_id): AxumPath<u32>,
    AxumPath(partition_id): AxumPath<String>,
) -> Result<Json<Partition>, StatusExt> {
    let group = find_group(bob, vdisk_id).await?;
    debug!("group with provided vdisk_id found");
    let holders = group.holders();
    debug!("get pearl holders: OK");
    let pearls = holders.read().await;
    let pearl = pearls.iter().find(|pearl| pearl.get_id() == partition_id);
    let partition = if let Some(p) = pearl {
        Some(Partition {
            node_name: group.node_name().to_owned(),
            disk_name: group.disk_name().to_owned(),
            vdisk_id: group.vdisk_id(),
            timestamp: p.start_timestamp(),
            records_count: p.records_count().await,
        })
    } else {
        None
    };
    partition.map(Json).ok_or_else(|| {
        let err = format!(
            "partition with id: {} in vdisk {} not found",
            partition_id, vdisk_id
        );
        warn!("{}", err);
        StatusExt::new(StatusCode::NOT_FOUND, false, err)
    })
}

async fn change_partition_state(
    Extension(bob): Extension<&BobServer>,
    AxumPath(vdisk_id): AxumPath<u32>,
    AxumPath(timestamp): AxumPath<u64>,
    AxumPath(action): AxumPath<Action>,
) -> Result<StatusExt, StatusExt> {
    let group = find_group(bob, vdisk_id).await?;
    let res = format!(
        "partitions with timestamp {} on vdisk {} is successfully {:?}ed",
        timestamp, vdisk_id, action
    );
    let result = match action {
        Action::Attach => group.attach(timestamp).await,
        Action::Detach => group.detach(timestamp).await.map(|_| ()),
    };
    match result {
        Ok(_) => {
            info!("{}", res);
            Ok(StatusExt::new(StatusCode::OK, true, res))
        }
        Err(e) => Err(StatusExt::new(StatusCode::OK, false, e.to_string())),
    }
}

async fn remount_vdisks_group(
    Extension(bob): Extension<&BobServer>,
    AxumPath(vdisk_id): AxumPath<u32>,
) -> Result<StatusExt, StatusExt> {
    let group = find_group(bob, vdisk_id).await?;
    match group.remount().await {
        Ok(_) => {
            info!("vdisks group {} successfully restarted", vdisk_id);
            Ok(StatusExt::new(
                StatusCode::OK,
                true,
                format!("vdisks group {} successfully restarted", vdisk_id),
            ))
        }
        Err(e) => Err(StatusExt::new(StatusCode::OK, false, e.to_string())),
    }
}

async fn delete_partition(
    Extension(bob): Extension<&BobServer>,
    AxumPath(vdisk_id): AxumPath<u32>,
    AxumPath(timestamp): AxumPath<u64>,
) -> Result<StatusExt, StatusExt> {
    let group = find_group(bob, vdisk_id).await?;
    let pearls = group.detach(timestamp).await;
    if let Ok(holders) = pearls {
        drop_directories(holders, timestamp, vdisk_id).await
    } else {
        let msg = format!(
            "partitions with timestamp {} not found on vdisk {} or it is active",
            timestamp, vdisk_id
        );
        Err(StatusExt::new(StatusCode::BAD_REQUEST, true, msg))
    }
}

async fn drop_directories(
    holders: Vec<Holder>,
    timestamp: u64,
    vdisk_id: u32,
) -> Result<StatusExt, StatusExt> {
    let mut result = String::new();
    for holder in holders {
        let msg = if let Err(e) = holder.drop_directory().await {
            format!(
                "partitions with timestamp {} delete failed on vdisk {}, error: {}",
                timestamp, vdisk_id, e
            )
        } else {
            format!("partitions deleted with timestamp {}", timestamp)
        };
        result.push_str(&msg);
        result.push('\n');
    }
    if result.is_empty() {
        Ok(StatusExt::new(StatusCode::OK, true, result))
    } else {
        Err(StatusExt::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            true,
            result,
        ))
    }
}

async fn alien() -> &'static str {
    "alien"
}

async fn detach_alien_partitions(
    Extension(bob): Extension<BobServer>,
) -> Result<StatusExt, StatusExt> {
    let backend = bob.grinder().backend().inner();
    let (_, alien_disk_controller) = backend
        .disk_controllers()
        .ok_or_else(not_acceptable_backend)?;
    alien_disk_controller.detach_all().await?;
    Ok(StatusExt::new(StatusCode::OK, true, String::default()))
}

async fn get_alien_directory(Extension(bob): Extension<BobServer>) -> Result<Json<Dir>, StatusExt> {
    let backend = bob.grinder().backend().inner();
    let (_, alien_disk_controller) = backend
        .disk_controllers()
        .ok_or_else(not_acceptable_backend)?;
    let path = PathBuf::from(alien_disk_controller.disk().path());
    let dir = create_directory(&path).await?;
    Ok(Json(dir))
}

async fn get_local_replica_directories(
    Extension(bob): Extension<&BobServer>,
    AxumPath(vdisk_id): AxumPath<u32>,
) -> Result<Json<Vec<Dir>>, StatusExt> {
    let vdisk: VDisk = get_vdisk_by_id(bob, vdisk_id).ok_or_else(|| {
        StatusExt::new(
            StatusCode::NOT_FOUND,
            false,
            format!("VDisk {} not found", vdisk_id),
        )
    })?;
    let local_node_name = bob.grinder().backend().mapper().local_node_name();
    let mut result = vec![];
    for replica in vdisk
        .replicas
        .into_iter()
        .filter(|r| r.node == local_node_name)
    {
        let path = PathBuf::from(replica.path);
        let dir = create_directory(&path).await?;
        result.push(dir);
    }
    Ok(Json(result))
}

fn create_directory(root_path: &Path) -> BoxFuture<Result<Dir, StatusExt>> {
    async move {
        let name = root_path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| internal(format!("failed to get filename for {:?}", root_path)))?;
        let root_path_str = root_path
            .to_str()
            .ok_or_else(|| internal(format!("failed to get path for {:?}", root_path)))?;
        let result = read_dir(root_path).await;
        match result {
            Ok(read_dir) => Ok(read_directory_children(read_dir, name, root_path_str).await),
            Err(e) => Err(internal(format!("read path {:?} failed: {}", root_path, e))),
        }
    }
    .boxed()
}

async fn read_directory_children(mut read_dir: ReadDir, name: &str, path: &str) -> Dir {
    let mut children = Vec::new();
    while let Ok(Some(entry)) = read_dir.next_entry().await {
        let dir = create_directory(&entry.path()).await;
        if let Ok(dir) = dir {
            children.push(dir);
        }
    }
    Dir {
        name: name.to_string(),
        path: path.to_string(),
        children,
    }
}

async fn get_data(
    Extension(bob): Extension<BobServer>,
    AxumPath(key): AxumPath<String>,
) -> Result<impl IntoResponse, StatusExt> {
    let key = DataKey::from_str(&key)?.0;
    let opts = BobOptions::new_get(None);
    let result = bob.grinder().get(key, &opts).await?;

    let content_type = infer_data_type(&result);
    let mut headers = HeaderMap::new();
    headers.insert(
        CONTENT_TYPE,
        content_type
            .parse()
            .expect("failed to parse content type value"),
    );
    Ok((headers, result.inner().to_owned()))
}

async fn put_data(
    Extension(bob): Extension<BobServer>,
    AxumPath(key): AxumPath<String>,
    body: Bytes,
) -> Result<StatusExt, StatusExt> {
    let key = DataKey::from_str(&key)?.0;
    let data_buf = body.to_vec();
    let data = BobData::new(
        data_buf,
        BobMeta::new(chrono::Local::now().timestamp() as u64),
    );

    let opts = BobOptions::new_put(None);
    bob.grinder().put(key, data, opts).await?;
    Ok(StatusCode::CREATED.into())
}

fn internal(message: String) -> StatusExt {
    StatusExt::new(StatusCode::INTERNAL_SERVER_ERROR, false, message)
}

fn bad_request(message: impl Into<String>) -> StatusExt {
    StatusExt::new(StatusCode::BAD_REQUEST, false, message.into())
}

impl DataKey {
    fn from_bytes(mut bytes: Vec<u8>) -> Result<Self, StatusExt> {
        if bytes.len() > BOB_KEY_SIZE && !bytes.iter().skip(BOB_KEY_SIZE).all(|&b| b == 0) {
            return Err(bad_request("Key overflow"));
        }
        bytes.resize(BOB_KEY_SIZE, 0);
        Ok(Self(bytes.into()))
    }

    fn from_guid(guid: &str) -> Result<Self, StatusExt> {
        // let guid = Uuid::from_str(guid)
        // .map_err(|e| bad_request(format!("GUID parse error: {}", e.to_string())))?;
        // Self::from_bytes(guid.as_bytes().to_vec())
        todo!()
    }

    fn from_hex(hex: &str) -> Result<Self, StatusExt> {
        if !hex.as_bytes().iter().all(|c| c.is_ascii_hexdigit()) {
            return Err(bad_request(
                "Hex parse error: non hexadecimal symbol in parameter",
            ));
        }
        let bytes = hex
            .as_bytes()
            .rchunks(2)
            .map(|c| {
                u8::from_str_radix(
                    std::str::from_utf8(c).expect("All chars is ascii hexdigits"),
                    16,
                )
                .expect("All chars is ascii hexdigits")
            })
            .rev()
            .collect();
        Self::from_bytes(bytes)
    }

    fn from_decimal(decimal: &str) -> Result<Self, StatusExt> {
        let number = decimal
            .parse::<u128>()
            .map_err(|e| bad_request(format!("Decimal parse error: {}", e.to_string())))?;
        Self::from_bytes(number.to_le_bytes().into())
    }
}

impl FromStr for DataKey {
    type Err = StatusExt;

    fn from_str(param: &str) -> Result<Self, Self::Err> {
        if param.starts_with('{') && param.ends_with('}') {
            Self::from_guid(&param[1..param.len() - 1])
        } else if param.contains('-') {
            Self::from_guid(param)
        } else if param.starts_with("0x") {
            Self::from_hex(param.get(2..).unwrap_or(""))
        } else if param.chars().all(|c| c.is_ascii_digit()) {
            Self::from_decimal(param)
        } else {
            Err(StatusExt::new(
                StatusCode::BAD_REQUEST,
                false,
                "Invalid key format".into(),
            ))
        }
    }
}

impl IntoResponse for StatusExt {
    fn into_response(self) -> Response<BoxBody> {
        let msg = format!("{{ \"ok\": {}, \"msg\": \"{}\" }}", self.ok, self.msg);
        Response::builder()
            .status(self.status)
            .body(BoxBody::new(body::boxed(body::Full::from(msg))))
            .expect("failed to set body for response")
    }
}

impl StatusExt {
    fn new(status: StatusCode, ok: bool, msg: String) -> Self {
        Self { status, ok, msg }
    }
}

impl From<StatusCode> for StatusExt {
    fn from(status: StatusCode) -> Self {
        // Self {
        //     status,
        //     ok: true,
        //     msg: status.reason().unwrap_or("Unknown").to_owned(),
        // }
        todo!()
    }
}

impl From<IoError> for StatusExt {
    fn from(err: IoError) -> Self {
        Self {
            status: match err.kind() {
                ErrorKind::NotFound => StatusCode::NOT_FOUND,
                _ => StatusCode::BAD_REQUEST,
            }, // TODO: Complete match
            ok: false,
            msg: err.to_string(),
        }
    }
}

impl From<BobError> for StatusExt {
    fn from(err: BobError) -> Self {
        use bob_common::error::Kind;
        let status = match err.kind() {
            Kind::DuplicateKey => StatusCode::CONFLICT,
            Kind::Internal => StatusCode::INTERNAL_SERVER_ERROR,
            Kind::VDiskIsNotReady => StatusCode::INTERNAL_SERVER_ERROR,
            Kind::KeyNotFound(_) => StatusCode::NOT_FOUND,
            _ => StatusCode::BAD_REQUEST,
        };
        Self {
            status,
            ok: false,
            msg: err.to_string(),
        }
    }
}

pub(crate) fn infer_data_type(data: &BobData) -> &'static str {
    match infer::get(data.inner()) {
        None => "*/*",
        Some(t) => t.mime_type(),
    }
}
