use crate::{
    build_info::BuildInfo, hw_metrics_collector::DiskSpaceMetrics, server::Server as BobServer,
};
use axum::{
    body::{self, BoxBody},
    extract::{Extension, Path as AxumPath},
    response::IntoResponse,
    routing::{delete, get, head, post, MethodRouter},
    Json, Router, Server,
};
use axum_server::{
    bind_rustls,
    tls_rustls::{RustlsAcceptor, RustlsConfig},
    Server as AxumServer,
};

pub(crate) use bob_access::Error as AuthError;
use bob_access::{Authenticator, CredentialsHolder};
use bob_backend::pearl::{Group as PearlGroup, Holder, NoopHooks};
use bob_common::{
    configs::node::TLSConfig,
    core_types::{DiskName, NodeDisk, VDisk as DataVDisk},
    data::{BobData, BobKey, BobMeta, BOB_KEY_SIZE},
    error::Error as BobError,
    operation_options::{BobDeleteOptions, BobGetOptions, BobPutOptions},
};
use bytes::Bytes;
use futures::{future::BoxFuture, stream::FuturesUnordered, FutureExt, StreamExt};
use http::{header::CONTENT_TYPE, HeaderMap, Response, StatusCode};
use std::{
    collections::HashMap,
    future::ready,
    io::{Error as IoError, ErrorKind},
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    str::FromStr,
};
use tokio::fs::{read_dir, ReadDir};
use uuid::Uuid;

use self::metric_models::MetricsSnapshotModel;

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
pub struct StatusExt {
    status: StatusCode,
    ok: bool,
    msg: String,
}

impl From<AuthError> for StatusExt {
    fn from(error: AuthError) -> Self {
        Self {
            status: error.status_code(),
            ok: false,
            msg: error.msg().into(),
        }
    }
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
    root_dir_name: String,
}

#[derive(Debug, Serialize)]
struct Space {
    total_disk_space_bytes: u64,
    free_disk_space_bytes: u64,
    used_disk_space_bytes: u64,
    occupied_disk_space_bytes: u64,
}

#[derive(Debug, Serialize)]
pub(crate) struct SpaceInfo {
    #[serde(flatten)]
    space: Space,
    disk_space_by_disk: HashMap<DiskName, Space>,
}

async fn tls_server(tls_config: &TLSConfig, addr: SocketAddr) -> AxumServer<RustlsAcceptor> {
    if let (Some(cert_path), Some(pkey_path)) = (&tls_config.cert_path, &tls_config.pkey_path) {
        let config = RustlsConfig::from_pem_file(cert_path, pkey_path)
            .await
            .expect("can not create tls config from pem file");
        bind_rustls(addr, config)
    } else {
        error!("rest tls enabled, but certificate or private key not specified");
        panic!("rest tls enabled, but certificate or private key not specified");
    }
}

pub(crate) async fn spawn<A>(
    bob: BobServer<A>,
    address: IpAddr,
    port: u16,
    tls_config: &Option<TLSConfig>,
) where
    A: Authenticator,
{
    let socket_addr = SocketAddr::new(address, port);

    let router = router::<A>().layer(Extension(bob));

    if let Some(tls_config) = tls_config
        .as_ref()
        .and_then(|tls_config| tls_config.rest_config())
    {
        let tls_server = tls_server(&tls_config, socket_addr).await;
        let task = tls_server.serve(router.into_make_service());
        tokio::spawn(task);
    } else {
        let task = Server::bind(&socket_addr).serve(router.into_make_service());
        tokio::spawn(task);
    }

    info!("API server started, listening: {}", socket_addr);
}

fn router<A>() -> Router
where
    A: Authenticator,
{
    let mut router = Router::new();
    for (path, service) in routes::<A>()
        .into_iter()
        .chain(s3::routes::<A>().into_iter())
    {
        router = router.route(path, service);
    }
    router
}

fn routes<A>() -> Vec<(&'static str, MethodRouter)>
where
    A: Authenticator + Send + Sync + 'static,
{
    vec![
        ("/status", get(status::<A>)),
        ("/status/space", get(get_space_info::<A>)),
        ("/metrics", get(metrics::<A>)),
        ("/version", get(version)),
        ("/nodes", get(nodes::<A>)),
        ("/disks/list", get(disks_list::<A>)),
        ("/metadata/distrfunc", get(distribution_function::<A>)),
        ("/configuration", get(get_node_configuration::<A>)),
        (
            "/disks/:disk_name/stop",
            post(stop_all_disk_controllers::<A>),
        ),
        (
            "/disks/:disk_name/start",
            post(start_all_disk_controllers::<A>),
        ),
        ("/vdisks", get(vdisks::<A>)),
        ("/blobs/outdated", delete(finalize_outdated_blobs::<A>)),
        ("/vdisks/:vdisk_id", get(vdisk_by_id::<A>)),
        (
            "/vdisks/:vdisk_id/records/count",
            get(vdisk_records_count::<A>),
        ),
        ("/vdisks/:vdisk_id/partitions", get(partitions::<A>)),
        (
            "/vdisks/:vdisk_id/partitions/:partition_id",
            get(partition_by_id::<A>),
        ),
        (
            "/vdisks/:vdisk_id/partitions/by_timestamp/:timestamp/:action",
            post(change_partition_state::<A>),
        ),
        ("/vdisks/:vdisk_id/remount", post(remount_vdisks_group::<A>)),
        (
            "/vdisks/:vdisk_id/partitions/by_timestamp/:timestamp",
            delete(delete_partition::<A>),
        ),
        ("/alien", get(alien)),
        ("/alien/detach", post(detach_alien_partitions::<A>)),
        ("/alien/dir", get(get_alien_directory::<A>)),
        (
            "/vdisks/:vdisk_id/replicas/local/dirs",
            get(get_local_replica_directories::<A>),
        ),
        ("/data/:key", get(get_data::<A>)),
        ("/data/:key", head(exist_data::<A>)),
        ("/data/:key", post(put_data::<A>)),
        ("/data/:key", delete(delete_data::<A>)),
    ]
}

fn data_vdisk_to_scheme(disk: &DataVDisk) -> VDisk {
    VDisk {
        id: disk.id(),
        replicas: collect_replicas_info(disk.replicas()),
    }
}

fn collect_disks_info<A: Authenticator>(bob: &BobServer<A>) -> Vec<VDisk> {
    let mapper = bob.grinder().backend().mapper();
    mapper.vdisks().values().map(data_vdisk_to_scheme).collect()
}

#[inline]
fn get_vdisk_by_id<A: Authenticator>(bob: &BobServer<A>, id: u32) -> Option<VDisk> {
    find_vdisk(bob, id).map(data_vdisk_to_scheme)
}

fn find_vdisk<A: Authenticator>(bob: &BobServer<A>, id: u32) -> Option<&DataVDisk> {
    let mapper = bob.grinder().backend().mapper();
    mapper.get_vdisk(id)
}

fn collect_replicas_info(replicas: &[NodeDisk]) -> Vec<Replica> {
    replicas
        .iter()
        .map(|r| Replica {
            path: r.disk_path().to_owned(),
            disk: r.disk_name().to_string(),
            node: r.node_name().to_string(),
        })
        .collect()
}

// GET /status
async fn status<A: Authenticator>(bob: Extension<BobServer<A>>) -> Json<Node> {
    let mapper = bob.grinder().backend().mapper();
    let name = mapper.local_node_name().to_string();
    let address = mapper.local_node_address().to_owned();
    let vdisks = collect_disks_info(&bob);
    let node = Node {
        name,
        address,
        vdisks,
    };
    Json(node)
}

// GET /status/space
async fn get_space_info<A: Authenticator>(
    bob: Extension<BobServer<A>>,
) -> Result<Json<SpaceInfo>, StatusExt> {
    let mut disk_metrics = bob.grinder().hw_counter().update_space_metrics();
    let summed_metrics: DiskSpaceMetrics = disk_metrics.values().sum();

    let backend = bob.grinder().backend().inner();
    let (dcs, adc) = backend
        .disk_controllers()
        .ok_or_else(not_acceptable_backend)?;
    let mut map = HashMap::new();
    for dc in dcs {
        map.insert(dc.disk().name(), dc.disk_used().await);
    }
    let adc_space = adc.disk_used().await;
    map.entry(adc.disk().name())
        .and_modify(|s| *s += adc_space)
        .or_insert(adc_space);

    Ok(Json(SpaceInfo {
        space: Space {
            total_disk_space_bytes: summed_metrics.total_space,
            used_disk_space_bytes: summed_metrics.used_space,
            free_disk_space_bytes: summed_metrics.free_space,
            occupied_disk_space_bytes: map.values().sum(),
        },
        disk_space_by_disk: map
            .iter()
            .map(|(&name, &occup_space)| {
                (name.clone(), {
                    let DiskSpaceMetrics {
                        total_space,
                        used_space,
                        free_space,
                    } = disk_metrics
                        .remove_entry(name)
                        .map(|(_, space)| space)
                        .unwrap_or_default();
                    Space {
                        total_disk_space_bytes: total_space,
                        free_disk_space_bytes: free_space,
                        used_disk_space_bytes: used_space,
                        occupied_disk_space_bytes: occup_space,
                    }
                })
            })
            .collect(),
    }))
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
async fn find_group<A: Authenticator>(
    bob: &BobServer<A>,
    vdisk_id: u32,
) -> Result<PearlGroup, StatusExt> {
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

// GET /metrics
async fn metrics<A: Authenticator>(bob: Extension<BobServer<A>>) -> Json<MetricsSnapshotModel> {
    let snapshot = bob.metrics().read().expect("rwlock").clone();
    Json(snapshot.into())
}

// GET /version
async fn version() -> Json<VersionInfo> {
    let build_info = BuildInfo::default();

    let version = build_info.version().to_string();
    let build_time = build_info.build_time().to_string();
    let bob_version = Version {
        version,
        build_time,
    };

    let version = build_info.pearl_version().to_string();
    let build_time = build_info.pearl_build_time().to_string();
    let pearl_version = Version {
        version,
        build_time,
    };

    let version_info = VersionInfo {
        bob_version,
        pearl_version,
    };
    Json(version_info)
}

// GET /nodes
async fn nodes<A>(
    bob: Extension<BobServer<A>>,
    creds: CredentialsHolder<A>,
) -> Result<Json<Vec<Node>>, AuthError>
where
    A: Authenticator,
{
    if !bob
        .auth()
        .check_credentials_rest(creds.into())?
        .has_rest_read()
    {
        return Err(AuthError::PermissionDenied);
    }
    let mapper = bob.grinder().backend().mapper();
    let mut nodes = vec![];
    let vdisks = collect_disks_info(&bob);
    for node in mapper.nodes() {
        let vdisks: Vec<_> = vdisks
            .iter()
            .filter_map(|vd| {
                if vd.replicas.iter().any(|r| r.node == *node.name()) {
                    let mut vd = vd.clone();
                    vd.replicas.retain(|r| r.node == *node.name());
                    Some(vd)
                } else {
                    None
                }
            })
            .collect();

        let name = node.name().to_string();
        let address = node.address().to_string();
        let node = Node {
            name,
            address,
            vdisks,
        };

        nodes.push(node);
    }
    Ok(Json(nodes))
}

// GET /disks/list
async fn disks_list<A>(
    bob: Extension<BobServer<A>>,
    creds: CredentialsHolder<A>,
) -> Result<Json<Vec<DiskState>>, StatusExt>
where
    A: Authenticator,
{
    if !bob
        .auth()
        .check_credentials_rest(creds.into())?
        .has_rest_read()
    {
        return Err(AuthError::PermissionDenied.into());
    }
    let backend = bob.grinder().backend().inner();
    let (dcs, alien_disk_controller) = backend
        .disk_controllers()
        .ok_or_else(not_acceptable_backend)?;

    let mut disks = Vec::new();
    for dc in dcs.iter().chain(std::iter::once(&alien_disk_controller)) {
        let disk_path = dc.disk();

        let name = disk_path.name().to_string();
        let path = disk_path.path().to_owned();
        let is_active = dc.is_ready().await;
        let value = DiskState {
            name,
            path,
            is_active,
        };
        disks.push(value);
    }

    Ok(Json(disks))
}

// GET /metadata/distrfunc
async fn distribution_function<A>(
    bob: Extension<BobServer<A>>,
    creds: CredentialsHolder<A>,
) -> Result<Json<DistrFunc>, AuthError>
where
    A: Authenticator,
{
    if !bob
        .auth()
        .check_credentials_rest(creds.into())?
        .has_rest_read()
    {
        return Err(AuthError::PermissionDenied);
    }
    let mapper = bob.grinder().backend().mapper().distribution_func();
    let func = format!("{:?}", mapper);
    Ok(Json(DistrFunc { func }))
}

// GET /configuration
async fn get_node_configuration<A>(
    bob: Extension<BobServer<A>>,
    creds: CredentialsHolder<A>,
) -> Result<Json<NodeConfiguration>, AuthError>
where
    A: Authenticator,
{
    if !bob
        .auth()
        .check_credentials_rest(creds.into())?
        .has_rest_read()
    {
        return Err(AuthError::PermissionDenied);
    }

    let grinder = bob.grinder();
    let config = grinder.node_config();
    Ok(Json(NodeConfiguration {
        blob_file_name_prefix: config.pearl().blob_file_name_prefix().to_owned(),
        root_dir_name: config.pearl().settings().root_dir_name().to_owned(),
    }))
}

// POST /disks/:disk_name/stop
async fn stop_all_disk_controllers<A>(
    bob: Extension<BobServer<A>>,
    AxumPath(disk_name): AxumPath<String>,
    creds: CredentialsHolder<A>,
) -> Result<StatusExt, StatusExt>
where
    A: Authenticator,
{
    if !bob
        .auth()
        .check_credentials_rest(creds.into())?
        .has_rest_write()
    {
        return Err(AuthError::PermissionDenied.into());
    }
    let backend = bob.grinder().backend().inner();
    let (dcs, alien_disk_controller) = backend
        .disk_controllers()
        .ok_or_else(not_acceptable_backend)?;
    dcs.iter()
        .chain(std::iter::once(&alien_disk_controller))
        .filter(|dc| *dc.disk().name() == disk_name)
        .map(|dc| dc.stop())
        .collect::<FuturesUnordered<_>>()
        .collect::<Vec<()>>()
        .await;
    let msg = "Disk controllers are stopped".to_owned();
    let status_ext = StatusExt::new(StatusCode::OK, true, msg);
    Ok(status_ext)
}

// POST /disks/:disk_name/start
async fn start_all_disk_controllers<A>(
    bob: Extension<BobServer<A>>,
    AxumPath(disk_name): AxumPath<String>,
    creds: CredentialsHolder<A>,
) -> Result<StatusExt, StatusExt>
where
    A: Authenticator,
{
    if !bob
        .auth()
        .check_credentials_rest(creds.into())?
        .has_rest_write()
    {
        return Err(AuthError::PermissionDenied.into());
    }
    let backend = bob.grinder().backend().inner();
    let (dcs, alien_disk_controller) = backend
        .disk_controllers()
        .ok_or_else(not_acceptable_backend)?;
    let target_dcs = dcs
        .iter()
        .chain(std::iter::once(&alien_disk_controller))
        .filter(|dc| *dc.disk().name() == disk_name)
        .map(|dc| dc.run(NoopHooks))
        .collect::<FuturesUnordered<_>>();

    if target_dcs.is_empty() {
        let err = format!("Disk Controller with name '{}' not found", disk_name);
        warn!("{}", err);
        return Err(StatusExt::new(StatusCode::NOT_FOUND, false, err));
    }

    let err_msg = target_dcs
        .fold(String::new(), |mut err_string, res| {
            if let Err(e) = res {
                err_string.push_str(&(e.to_string() + "\n"));
            }
            ready(err_string)
        })
        .await;
    if err_msg.is_empty() {
        let msg = format!(
            "all disk controllers for disk '{}' successfully started",
            disk_name
        );
        info!("{}", msg);
        Ok(StatusExt::new(StatusCode::OK, true, msg))
    } else {
        let status_ext = StatusExt::new(StatusCode::INTERNAL_SERVER_ERROR, false, err_msg);
        Err(status_ext)
    }
}

// GET /vdisks
async fn vdisks<A>(
    bob: Extension<BobServer<A>>,
    creds: CredentialsHolder<A>,
) -> Result<Json<Vec<VDisk>>, AuthError>
where
    A: Authenticator,
{
    if !bob
        .auth()
        .check_credentials_rest(creds.into())?
        .has_rest_read()
    {
        return Err(AuthError::PermissionDenied);
    }
    let vdisks = collect_disks_info(&bob);
    Ok(Json(vdisks))
}

// DELETE /blobs/outdated
async fn finalize_outdated_blobs<A>(
    bob: Extension<BobServer<A>>,
    creds: CredentialsHolder<A>,
) -> Result<StatusExt, AuthError>
where
    A: Authenticator,
{
    if !bob
        .auth()
        .check_credentials_rest(creds.into())?
        .has_rest_write()
    {
        return Err(AuthError::PermissionDenied);
    }
    let backend = bob.grinder().backend();
    backend.close_unneeded_active_blobs(1, 1).await;
    let msg = "Successfully removed outdated blobs".to_string();
    Ok(StatusExt::new(StatusCode::OK, true, msg))
}

// GET /vdisks/:vdisk_id
async fn vdisk_by_id<A>(
    bob: Extension<BobServer<A>>,
    AxumPath(vdisk_id): AxumPath<u32>,
    creds: CredentialsHolder<A>,
) -> Result<Json<VDisk>, StatusExt>
where
    A: Authenticator,
{
    if !bob
        .auth()
        .check_credentials_rest(creds.into())?
        .has_rest_read()
    {
        return Err(AuthError::PermissionDenied.into());
    }
    get_vdisk_by_id(&bob, vdisk_id)
        .map(Json)
        .ok_or_else(|| StatusExt::new(StatusCode::NOT_FOUND, false, "vdisk not found".to_string()))
}

// GET /vdisks/:vdisk_id/records/count
async fn vdisk_records_count<A>(
    bob: Extension<BobServer<A>>,
    AxumPath(vdisk_id): AxumPath<u32>,
    creds: CredentialsHolder<A>,
) -> Result<Json<u64>, StatusExt>
where
    A: Authenticator,
{
    if !bob
        .auth()
        .check_credentials_rest(creds.into())?
        .has_rest_read()
    {
        return Err(AuthError::PermissionDenied.into());
    }
    let group = find_group(&bob, vdisk_id).await?;
    let holders = group.holders();
    let pearls = holders.read().await;
    let mut sum = 0;
    for pearl in pearls.iter() {
        sum += pearl.records_count().await;
    }
    Ok(Json(sum as u64))
}

// GET /vdisks/:vdisk_id/partitions
async fn partitions<A>(
    bob: Extension<BobServer<A>>,
    AxumPath(vdisk_id): AxumPath<u32>,
    creds: CredentialsHolder<A>,
) -> Result<Json<VDiskPartitions>, StatusExt>
where
    A: Authenticator,
{
    if !bob
        .auth()
        .check_credentials_rest(creds.into())?
        .has_rest_read()
    {
        return Err(AuthError::PermissionDenied.into());
    }
    let group = find_group(&bob, vdisk_id).await?;
    debug!("group with provided vdisk_id found");
    let holders = group.holders();
    let pearls = holders.read().await;
    debug!("get pearl holders: OK");
    let partitions = pearls.iter().map(Holder::get_id).collect();

    let node_name = group.node_name().to_string();
    let disk_name = group.disk_name().to_string();
    let vdisk_id = group.vdisk_id();
    let ps = VDiskPartitions {
        node_name,
        disk_name,
        vdisk_id,
        partitions,
    };
    trace!("partitions: {:?}", ps);
    Ok(Json(ps))
}

// GET /vdisks/:vdisk_id/partitions/:partition_id
async fn partition_by_id<A>(
    bob: Extension<BobServer<A>>,
    AxumPath((vdisk_id, partition_id)): AxumPath<(u32, String)>,
    creds: CredentialsHolder<A>,
) -> Result<Json<Partition>, StatusExt>
where
    A: Authenticator,
{
    if !bob
        .auth()
        .check_credentials_rest(creds.into())?
        .has_rest_read()
    {
        return Err(AuthError::PermissionDenied.into());
    }
    let group = find_group(&bob, vdisk_id).await?;
    debug!("group with provided vdisk_id found");
    let holders = group.holders();
    debug!("get pearl holders: OK");
    let pearls = holders.read().await;
    let pearl = pearls.iter().find(|pearl| pearl.get_id() == partition_id);
    let partition = if let Some(p) = pearl {
        let partition = Partition {
            node_name: group.node_name().to_string(),
            disk_name: group.disk_name().to_string(),
            vdisk_id: group.vdisk_id(),
            timestamp: p.start_timestamp(),
            records_count: p.records_count().await,
        };
        Some(partition)
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

// POST /vdisks/:vdisk_id/partitions/by_timestamp/:timestamp/:action
async fn change_partition_state<A>(
    bob: Extension<BobServer<A>>,
    AxumPath((vdisk_id, timestamp, action)): AxumPath<(u32, u64, Action)>,
    creds: CredentialsHolder<A>,
) -> Result<StatusExt, StatusExt>
where
    A: Authenticator,
{
    if !bob
        .auth()
        .check_credentials_rest(creds.into())?
        .has_rest_write()
    {
        return Err(AuthError::PermissionDenied.into());
    }
    let group = find_group(&bob, vdisk_id).await?;
    let error = match action {
        Action::Attach => group.attach(timestamp).await.err(),
        Action::Detach => group.detach(timestamp).await.err(),
    };
    if let Some(err) = error {
        Err(StatusExt::new(StatusCode::OK, false, err.to_string()))
    } else {
        let msg = format!(
            "partitions with timestamp {} on vdisk {} is successfully {:?}ed",
            timestamp, vdisk_id, action
        );
        info!("{}", msg);
        Ok(StatusExt::new(StatusCode::OK, true, msg))
    }
}

// POST /vdisks/:vdisk_id/remount
async fn remount_vdisks_group<A>(
    bob: Extension<BobServer<A>>,
    AxumPath(vdisk_id): AxumPath<u32>,
    creds: CredentialsHolder<A>,
) -> Result<StatusExt, StatusExt>
where
    A: Authenticator,
{
    if !bob
        .auth()
        .check_credentials_rest(creds.into())?
        .has_rest_write()
    {
        return Err(AuthError::PermissionDenied.into());
    }
    bob.grinder()
        .backend()
        .inner()
        .remount_vdisk(vdisk_id)
        .await
        .map(|_| {
            StatusExt::new(
                StatusCode::OK,
                true,
                format!("vdisk {} successfully restarted", vdisk_id),
            )
        })
        .map_err(|e| StatusExt::new(StatusCode::OK, false, e.to_string()))
}

// DELETE /vdisks/:vdisk_id/partitions/by_timestamp/:timestamp
async fn delete_partition<A>(
    bob: Extension<BobServer<A>>,
    AxumPath((vdisk_id, timestamp)): AxumPath<(u32, u64)>,
    creds: CredentialsHolder<A>,
) -> Result<StatusExt, StatusExt>
where
    A: Authenticator,
{
    if !bob
        .auth()
        .check_credentials_rest(creds.into())?
        .has_rest_write()
    {
        return Err(AuthError::PermissionDenied.into());
    }
    let group = find_group(&bob, vdisk_id).await?;
    let pearls = group.detach(timestamp).await.ok();
    if let Some(holders) = pearls {
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
    let mut result_msg = String::new();
    let mut is_error = false;
    for holder in holders {
        let msg = if let Some(err) = holder.drop_directory().await.err() {
            is_error = true;
            format!(
                "partitions with timestamp {} delete failed on vdisk {}, error: {}",
                timestamp, vdisk_id, err
            )
        } else {
            format!("partitions deleted with timestamp {}", timestamp)
        };
        result_msg.push_str(&msg);
        result_msg.push('\n');
    }
    if !is_error {
        Ok(StatusExt::new(StatusCode::OK, true, result_msg))
    } else {
        let status_ext = StatusExt::new(StatusCode::INTERNAL_SERVER_ERROR, true, result_msg);
        Err(status_ext)
    }
}

// GET /alien
async fn alien() -> &'static str {
    "alien"
}

// POST /alien/detach
async fn detach_alien_partitions<A>(
    bob: Extension<BobServer<A>>,
    creds: CredentialsHolder<A>,
) -> Result<StatusExt, StatusExt>
where
    A: Authenticator,
{
    if !bob
        .auth()
        .check_credentials_rest(creds.into())?
        .has_rest_write()
    {
        return Err(AuthError::PermissionDenied.into());
    }
    let backend = bob.grinder().backend().inner();
    let (_, alien_disk_controller) = backend
        .disk_controllers()
        .ok_or_else(not_acceptable_backend)?;
    alien_disk_controller.detach_all().await?;
    Ok(StatusExt::new(StatusCode::OK, true, String::default()))
}

// GET /alien/dir
async fn get_alien_directory<A>(
    bob: Extension<BobServer<A>>,
    creds: CredentialsHolder<A>,
) -> Result<Json<Dir>, StatusExt>
where
    A: Authenticator,
{
    if !bob
        .auth()
        .check_credentials_rest(creds.into())?
        .has_rest_read()
    {
        return Err(AuthError::PermissionDenied.into());
    }
    let backend = bob.grinder().backend().inner();
    let (_, alien_disk_controller) = backend
        .disk_controllers()
        .ok_or_else(not_acceptable_backend)?;
    let path = PathBuf::from(alien_disk_controller.disk().path());
    let dir = create_directory(&path).await?;
    Ok(Json(dir))
}

// GET /vdisks/:vdisk_id/replicas/local/dirs
async fn get_local_replica_directories<A>(
    bob: Extension<BobServer<A>>,
    AxumPath(vdisk_id): AxumPath<u32>,
    creds: CredentialsHolder<A>,
) -> Result<Json<Vec<Dir>>, StatusExt>
where
    A: Authenticator,
{
    if !bob
        .auth()
        .check_credentials_rest(creds.into())?
        .has_rest_read()
    {
        return Err(AuthError::PermissionDenied.into());
    }
    let vdisk: VDisk = get_vdisk_by_id(&bob, vdisk_id).ok_or_else(|| {
        let msg = format!("VDisk {} not found", vdisk_id);
        StatusExt::new(StatusCode::NOT_FOUND, false, msg)
    })?;
    let local_node_name = bob.grinder().backend().mapper().local_node_name();
    let mut result = vec![];
    for replica in vdisk
        .replicas
        .into_iter()
        .filter(|r| r.node == *local_node_name)
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

// GET /data/:key
async fn get_data<A>(
    bob: Extension<BobServer<A>>,
    AxumPath(key): AxumPath<String>,
    creds: CredentialsHolder<A>,
) -> Result<impl IntoResponse, StatusExt>
where
    A: Authenticator,
{
    if !bob.auth().check_credentials_rest(creds.into())?.has_read() {
        return Err(AuthError::PermissionDenied.into());
    }
    let key = DataKey::from_str(&key)?.0;
    let opts = BobGetOptions::from_grpc(None);
    let result = bob.grinder().get(key, &opts).await?;

    let content_type = infer_data_type(&result);
    let mut headers = HeaderMap::new();
    let val = content_type
        .parse()
        .map_err(|e| {
            error!("content type parsing error: {}", e);
            StatusExt {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                ok: false,
                msg: "content type parsing failed".to_string(),
            }
        })
        .expect("failed to parse content type value");
    headers.insert(CONTENT_TYPE, val);
    Ok((headers, result.inner().to_owned()))
}

// HEAD /data/:key
async fn exist_data<A>(
    bob: Extension<BobServer<A>>,
    AxumPath(key): AxumPath<String>,
    creds: CredentialsHolder<A>,
) -> Result<StatusCode, StatusExt>
where
    A: Authenticator,
{
    if !bob.auth().check_credentials_rest(creds.into())?.has_read() {
        return Err(AuthError::PermissionDenied.into());
    }
    let keys = [DataKey::from_str(&key)?.0];
    let opts = BobGetOptions::from_grpc(None);
    let result = bob.grinder().exist(&keys, &opts).await?;

    match result.get(0) {
        Some(true) => Ok(StatusCode::OK),
        Some(false) => Ok(StatusCode::NOT_FOUND),
        None => Err(StatusExt::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            false,
            "Missing 'exist' result".to_owned(),
        )),
    }
}

// POST /data/:key
async fn put_data<A>(
    bob: Extension<BobServer<A>>,
    AxumPath(key): AxumPath<String>,
    body: Bytes,
    creds: CredentialsHolder<A>,
) -> Result<StatusExt, StatusExt>
where
    A: Authenticator,
{
    if !bob.auth().check_credentials_rest(creds.into())?.has_write() {
        return Err(AuthError::PermissionDenied.into());
    }
    let key = DataKey::from_str(&key)?.0;
    let meta = BobMeta::new(chrono::Utc::now().timestamp() as u64);
    let data = BobData::new(body, meta);

    let opts = BobPutOptions::from_grpc(None);
    bob.grinder().put(key, &data, opts).await?;
    Ok(StatusCode::CREATED.into())
}

// DELETE /data/:key
async fn delete_data<A>(
    bob: Extension<BobServer<A>>,
    AxumPath(key): AxumPath<String>,
    creds: CredentialsHolder<A>,
) -> Result<StatusExt, StatusExt>
where
    A: Authenticator,
{
    if !bob.auth().check_credentials_rest(creds.into())?.has_write() {
        return Err(AuthError::PermissionDenied.into());
    }
    let key = DataKey::from_str(&key)?.0;
    bob.grinder()
        .delete(
            key,
            &BobMeta::new(chrono::Utc::now().timestamp() as u64),
            BobDeleteOptions::from_grpc(None),
        )
        .await
        .map_err(|e| internal(e.to_string()))?;
    Ok(StatusExt::new(StatusCode::OK, true, format!("Done")))
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
        let guid =
            Uuid::from_str(guid).map_err(|e| bad_request(format!("GUID parse error: {}", e)))?;
        Self::from_bytes(guid.as_bytes().to_vec())
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
            .map_err(|e| bad_request(format!("Decimal parse error: {}", e)))?;
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
            let msg = "Invalid key format".into();
            Err(StatusExt::new(StatusCode::BAD_REQUEST, false, msg))
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
        Self {
            status,
            ok: true,
            msg: status.canonical_reason().unwrap_or("Unknown").to_owned(),
        }
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
            Kind::HolderTemporaryUnavailable => StatusCode::SERVICE_UNAVAILABLE,
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
