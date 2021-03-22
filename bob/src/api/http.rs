use crate::server::Server as BobServer;
use bob_backend::pearl::{Group as PearlGroup, Holder};
use bob_common::{
    data::{BobData, BobKey, BobMeta, BobOptions, VDisk as DataVDisk},
    node::Disk as NodeDisk,
};
use futures::{future::BoxFuture, FutureExt};
use rocket::{
    http::{ContentType, RawStr, Status},
    request::FromParam,
    response::{Content, Responder, Result as RocketResult},
    Config, Data, Request, Response, Rocket, State,
};
use rocket_contrib::json::Json;
use std::{
    io::{Cursor, ErrorKind, Read},
    path::{Path, PathBuf},
    str::FromStr,
    thread,
};
use tokio::{
    fs::{read_dir, ReadDir},
    runtime::Runtime,
};

#[derive(Debug, Clone)]
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
    status: Status,
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

fn runtime() -> Runtime {
    // TODO: run web server on same runtime as bob (update to async rocket when it's stable)
    debug!("HOT FIX: run web server on same runtime as bob");
    Runtime::new().expect("create runtime")
}

pub(crate) fn spawn(bob: BobServer, port: u16) {
    let routes = routes![
        status,
        vdisks,
        vdisk_by_id,
        partitions,
        partition_by_id,
        change_partition_state,
        delete_partition,
        alien,
        remount_vdisks_group,
        get_local_replica_directories,
        nodes,
        finalize_outdated_blobs,
        vdisk_records_count,
        distribution_function,
        get_data,
        put_data
    ];
    let task = move || {
        info!("API server started");
        let mut config = Config::production();
        config.set_port(port);
        Rocket::custom(config)
            .manage(bob)
            .mount("/", routes)
            .launch();
    };
    thread::spawn(task);
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

fn not_acceptable_backend() -> Status {
    let mut status = Status::NotAcceptable;
    status.reason = "only pearl backend supports partitions";
    warn!("{:?}", status);
    status
}

fn find_group<'a>(bob: &'a State<BobServer>, vdisk_id: u32) -> Result<&'a PearlGroup, StatusExt> {
    let backend = bob.grinder().backend().inner();
    debug!("get backend: OK");
    let groups = backend.vdisks_groups().ok_or_else(not_acceptable_backend)?;
    debug!("get vdisks groups: OK");
    groups
        .iter()
        .find(|group| group.vdisk_id() == vdisk_id)
        .ok_or_else(|| {
            let err = format!("vdisk with id: {} not found", vdisk_id);
            warn!("{}", err);
            StatusExt::new(Status::NotFound, false, err)
        })
}

#[get("/status")]
fn status(bob: State<BobServer>) -> Json<Node> {
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

#[get("/nodes")]
fn nodes(bob: State<BobServer>) -> Json<Vec<Node>> {
    let mapper = bob.grinder().backend().mapper();
    let mut nodes = vec![];
    let vdisks = collect_disks_info(&bob);
    for node in mapper.nodes().values() {
        let vdisks: Vec<VDisk> = vdisks
            .iter()
            .filter_map(|vd| {
                if vd.replicas.iter().any(|r| r.node == node.name()) {
                    let mut vd = vd.clone();
                    vd.replicas.drain_filter(|r| r.node != node.name());
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

#[get("/metadata/distrfunc")]
fn distribution_function(bob: State<BobServer>) -> Json<DistrFunc> {
    let mapper = bob.grinder().backend().mapper();
    Json(DistrFunc {
        func: format!("{:?}", mapper.distribution_func()),
    })
}

#[get("/vdisks")]
fn vdisks(bob: State<BobServer>) -> Json<Vec<VDisk>> {
    let vdisks = collect_disks_info(&bob);
    Json(vdisks)
}

#[delete("/blobs/outdated")]
fn finalize_outdated_blobs(bob: State<BobServer>) -> StatusExt {
    let bob = bob.clone();
    runtime().spawn(async move {
        let backend = bob.grinder().backend();
        backend.close_unneeded_active_blobs(1, 1).await;
    });
    StatusExt::new(
        Status::Ok,
        true,
        "Successfully removed outdated blobs".to_string(),
    )
}

#[get("/vdisks/<vdisk_id>")]
fn vdisk_by_id(bob: State<BobServer>, vdisk_id: u32) -> Option<Json<VDisk>> {
    get_vdisk_by_id(&bob, vdisk_id).map(Json)
}

#[get("/vdisks/<vdisk_id>/records/count")]
fn vdisk_records_count(bob: State<BobServer>, vdisk_id: u32) -> Result<Json<u64>, StatusExt> {
    let group = find_group(&bob, vdisk_id)?;
    let holders = group.holders();
    let sum = runtime().block_on(async move {
        let pearls = holders.read().await;
        let pearls: &[_] = pearls.as_ref();
        let mut sum = 0;
        for pearl in pearls {
            sum += pearl.records_count().await;
        }
        sum
    });
    Ok(Json(sum as u64))
}

#[get("/vdisks/<vdisk_id>/partitions")]
fn partitions(bob: State<BobServer>, vdisk_id: u32) -> Result<Json<VDiskPartitions>, StatusExt> {
    let group = find_group(&bob, vdisk_id)?;
    debug!("group with provided vdisk_id found");
    let holders = group.holders();
    let pearls = runtime().block_on(holders.read());
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

#[get("/vdisks/<vdisk_id>/partitions/<partition_id>")]
fn partition_by_id(
    bob: State<'_, BobServer>,
    vdisk_id: u32,
    partition_id: String,
) -> Result<Json<Partition>, StatusExt> {
    let group = find_group(&bob, vdisk_id)?;
    debug!("group with provided vdisk_id found");
    let holders = group.holders();
    debug!("get pearl holders: OK");
    // TODO: run web server on same runtime as bob
    debug!("HOT FIX: run web server on same runtime as bob");
    let rt = Runtime::new().expect("create runtime");
    let pearls = rt.block_on(holders.read());
    let pearl = pearls.iter().find(|pearl| pearl.get_id() == partition_id);
    let partition = pearl.map(|p| Partition {
        node_name: group.node_name().to_owned(),
        disk_name: group.disk_name().to_owned(),
        vdisk_id: group.vdisk_id(),
        timestamp: p.start_timestamp(),
        records_count: rt.block_on(p.records_count()),
    });
    partition.map(Json).ok_or_else(|| {
        let err = format!(
            "partition with id: {} in vdisk {} not found",
            partition_id, vdisk_id
        );
        warn!("{}", err);
        StatusExt::new(Status::NotFound, false, err)
    })
}

#[post("/vdisks/<vdisk_id>/partitions/by_timestamp/<timestamp>/<action>")]
fn change_partition_state(
    bob: State<BobServer>,
    vdisk_id: u32,
    timestamp: u64,
    action: Action,
) -> Result<StatusExt, StatusExt> {
    let group = find_group(&bob, vdisk_id)?;
    let group = group.clone();
    // TODO: run web server on same runtime as bob
    debug!("HOT FIX: run web server on same runtime as bob");
    let rt = Runtime::new().expect("create runtime");
    let res = format!(
        "partitions with timestamp {} on vdisk {} is successfully {:?}ed",
        timestamp, vdisk_id, action
    );
    let task = async move {
        match action {
            Action::Attach => group.attach(timestamp).await,
            Action::Detach => group.detach(timestamp).await.map(|_| ()),
        }
    };
    match rt.block_on(task) {
        Ok(_) => {
            info!("{}", res);
            Ok(StatusExt::new(Status::Ok, true, res))
        }
        Err(e) => Err(StatusExt::new(Status::Ok, false, e.to_string())),
    }
}

#[post("/vdisks/<vdisk_id>/remount")]
fn remount_vdisks_group(bob: State<BobServer>, vdisk_id: u32) -> Result<StatusExt, StatusExt> {
    let group = find_group(&bob, vdisk_id)?;
    let group = group.clone();
    debug!("HOT FIX: run web server on same runtime as bob");
    let rt = Runtime::new().expect("create runtime");
    let task = group.remount();
    match rt.block_on(task) {
        Ok(_) => {
            info!("vdisks group {} successfully restarted", vdisk_id);
            Ok(StatusExt::new(
                Status::Ok,
                true,
                format!("vdisks group {} successfully restarted", vdisk_id),
            ))
        }
        Err(e) => Err(StatusExt::new(Status::Ok, false, e.to_string())),
    }
}

#[delete("/vdisks/<vdisk_id>/partitions/by_timestamp/<timestamp>")]
fn delete_partition(
    bob: State<BobServer>,
    vdisk_id: u32,
    timestamp: u64,
) -> Result<StatusExt, StatusExt> {
    let group = find_group(&bob, vdisk_id)?;
    let task = async {
        let pearls = group.detach(timestamp).await;
        if let Ok(holders) = pearls {
            drop_directories(holders, timestamp, vdisk_id).await
        } else {
            let msg = format!(
                "partitions with timestamp {} not found on vdisk {} or it is active",
                timestamp, vdisk_id
            );
            Err(StatusExt::new(Status::BadRequest, true, msg))
        }
    };
    runtime().block_on(task)
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
        Ok(StatusExt::new(Status::Ok, true, result))
    } else {
        Err(StatusExt::new(Status::InternalServerError, true, result))
    }
}

#[get("/alien")]
fn alien(_bob: State<BobServer>) -> &'static str {
    "alien"
}

#[get("/vdisks/<vdisk_id>/replicas/local/dirs")]
fn get_local_replica_directories(
    bob: State<BobServer>,
    vdisk_id: u32,
) -> Result<Json<Vec<Dir>>, StatusExt> {
    let vdisk: VDisk = get_vdisk_by_id(&bob, vdisk_id).ok_or_else(|| {
        StatusExt::new(
            Status::NotFound,
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
        let dir = runtime().block_on(create_directory(&path)).ok_or_else(|| {
            StatusExt::new(
                Status::InternalServerError,
                false,
                format!("Failed to get dirs for replica {:?}", path),
            )
        })?;
        result.push(dir);
    }
    Ok(Json(result))
}

fn create_directory(root_path: &Path) -> BoxFuture<Option<Dir>> {
    async move {
        let name = root_path.file_name()?.to_str()?;
        let root_path_str = root_path.to_str()?;
        let result = read_dir(root_path).await;
        match result {
            Ok(read_dir) => Some(
                read_directory_children(read_dir, name.to_string(), root_path_str.to_string())
                    .await,
            ),
            Err(e) => {
                error!("read path {:?} failed: {}", root_path, e);
                None
            }
        }
    }
    .boxed()
}

async fn read_directory_children(mut read_dir: ReadDir, name: String, path: String) -> Dir {
    let mut children = Vec::new();
    while let Some(child) = read_dir.next_entry().await.transpose() {
        if let Ok(entry) = child {
            let dir = create_directory(&entry.path()).await;
            if let Some(dir) = dir {
                children.push(dir);
            }
        }
    }
    Dir {
        name,
        path,
        children,
    }
}

#[get("/data/<key>")]
fn get_data(_bob: State<BobServer>, key: BobKey) -> Result<Content<Vec<u8>>, StatusExt> {
    let opts = BobOptions::new_get(None);
    let result = runtime()
        .block_on(async { _bob.grinder().get(key, &opts).await })
        .map_err(|err| -> StatusExt { err.into() })?;
    let mime_type = infer::get(result.inner());
    let mime_type = match mime_type {
        None => ContentType::Any,
        Some(t) => ContentType::from_str(t.mime_type()).unwrap_or_default(),
    };
    Ok(Content(mime_type, result.inner().to_owned()))
}

#[post("/data/<key>", data = "<data>")]
fn put_data(_bob: State<BobServer>, key: BobKey, data: Data) -> Result<StatusExt, StatusExt> {
    let mut data_buf = vec![];
    data.open()
        .read_to_end(&mut data_buf)
        .map_err(|err| -> StatusExt { err.into() })?;
    let data = BobData::new(
        data_buf,
        BobMeta::new(chrono::Local::now().timestamp() as u64),
    );

    let opts = BobOptions::new_put(None);
    runtime()
        .block_on(async { _bob.grinder().put(key, data, opts).await })
        .map_err(|err| -> StatusExt { err.into() })?;

    Ok(Status::Created.into())
}

impl<'r> FromParam<'r> for Action {
    type Error = &'r RawStr;

    fn from_param(param: &'r RawStr) -> Result<Self, Self::Error> {
        error!("{}", param.as_str());
        match param.as_str() {
            "attach" => Ok(Self::Attach),
            "detach" => Ok(Self::Detach),
            _ => Err(param),
        }
    }
}

impl Responder<'_> for StatusExt {
    fn respond_to(self, _: &Request) -> RocketResult<'static> {
        let msg = format!("{{ \"ok\": {}, \"msg\": \"{}\" }}", self.ok, self.msg);
        Response::build()
            .status(self.status)
            .sized_body(Cursor::new(msg))
            .ok()
    }
}

impl StatusExt {
    fn new(status: Status, ok: bool, msg: String) -> Self {
        Self { status, ok, msg }
    }
}

impl From<Status> for StatusExt {
    fn from(status: Status) -> Self {
        Self {
            status,
            ok: true,
            msg: status.reason.to_owned(),
        }
    }
}

impl From<std::io::Error> for StatusExt {
    fn from(err: std::io::Error) -> Self {
        Self {
            status: match err.kind() {
                ErrorKind::NotFound => Status::NotFound,
                _ => Status::BadRequest,
            }, // TODO: Complete match
            ok: false,
            msg: err.to_string(),
        }
    }
}

impl From<bob_common::error::Error> for StatusExt {
    fn from(err: bob_common::error::Error) -> Self {
        let status = if err.is_duplicate() {
            Status::Conflict
        } else if err.is_internal() {
            Status::InternalServerError
        } else if err.is_key_not_found() {
            Status::NotFound
        } else if err.is_not_ready() {
            Status::InternalServerError
        } else {
            Status::BadRequest
        };
        Self {
            status,
            ok: false,
            msg: err.to_string(),
        }
    }
}
