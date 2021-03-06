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
};
use tokio::fs::{read_dir, ReadDir};

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

#[derive(Debug, Serialize)]
pub(crate) struct DiskState {
    name: String,
    path: String,
    is_active: bool,
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
        get_alien_directory,
        remount_vdisks_group,
        start_all_disk_controllers,
        stop_all_disk_controllers,
        get_local_replica_directories,
        nodes,
        disks_list,
        finalize_outdated_blobs,
        vdisk_records_count,
        distribution_function,
        get_data,
        put_data
    ];
    let task = async move {
        info!("API server started");
        let mut config = Config::production();
        config.set_port(port);
        Rocket::custom(config)
            .manage(bob)
            .mount("/", routes)
            .launch();
    };
    tokio::spawn(task);
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

// !notice: only finds normal group
fn find_group(bob: &State<BobServer>, vdisk_id: u32) -> Result<PearlGroup, StatusExt> {
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
            StatusExt::new(Status::NotFound, false, err)
        })?;
    let task = async move {
        needed_dc.vdisk_group(vdisk_id).await.map_err(|_| {
            let err = format!("Disk Controller with vdisk #{} not found", vdisk_id);
            warn!("{}", err);
            StatusExt::new(Status::NotFound, false, err)
        })
    };
    bob.block_on(task)
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

#[get("/disks/list")]
fn disks_list(bob: State<BobServer>) -> Result<Json<Vec<DiskState>>, StatusExt> {
    let backend = bob.grinder().backend().inner();
    let (dcs, adc) = backend
        .disk_controllers()
        .ok_or_else(not_acceptable_backend)?;

    let task = async move {
        let mut disks = Vec::new();
        for dc in dcs.iter().chain(std::iter::once(&adc)) {
            let disk_path = dc.disk();
            disks.push(DiskState {
                name: disk_path.name().to_owned(),
                path: disk_path.path().to_owned(),
                is_active: dc.is_ready().await,
            });
        }
        disks
    };

    let disks = bob.block_on(task);

    Ok(Json(disks))
}

#[get("/metadata/distrfunc")]
fn distribution_function(bob: State<BobServer>) -> Json<DistrFunc> {
    let mapper = bob.grinder().backend().mapper();
    Json(DistrFunc {
        func: format!("{:?}", mapper.distribution_func()),
    })
}

#[post("/disks/<disk_name>/stop")]
fn stop_all_disk_controllers(
    bob: State<BobServer>,
    disk_name: String,
) -> Result<StatusExt, StatusExt> {
    use futures::stream::{FuturesUnordered, StreamExt};
    let backend = bob.grinder().backend().inner();
    let (dcs, adc) = backend
        .disk_controllers()
        .ok_or_else(not_acceptable_backend)?;
    let tasks = async move {
        dcs.iter()
            .chain(std::iter::once(&adc))
            .filter(|dc| dc.disk().name() == disk_name)
            .map(|dc| dc.stop())
            .collect::<FuturesUnordered<_>>()
            .collect::<Vec<()>>()
            .await;
    };
    bob.block_on(tasks);
    Ok(StatusExt::new(
        Status::Ok,
        true,
        "Disk controllers are stopped".to_owned(),
    ))
}

#[post("/disks/<disk_name>/start")]
fn start_all_disk_controllers(
    bob: State<BobServer>,
    disk_name: String,
) -> Result<StatusExt, StatusExt> {
    use futures::stream::{FuturesUnordered, StreamExt};
    let backend = bob.grinder().backend().inner();
    let (dcs, adc) = backend
        .disk_controllers()
        .ok_or_else(not_acceptable_backend)?;
    let target_dcs = dcs
        .iter()
        .chain(std::iter::once(&adc))
        .filter(|dc| dc.disk().name() == disk_name)
        .map(|dc| dc.run())
        .collect::<FuturesUnordered<_>>();
    if target_dcs.is_empty() {
        let err = format!("Disk Controller with name '{}' not found", disk_name);
        warn!("{}", err);
        return Err(StatusExt::new(Status::NotFound, false, err));
    }
    let task = async move {
        let err_string = target_dcs
            .fold(String::new(), |mut err_string, res| {
                if let Err(e) = res {
                    err_string.push_str(&(e.to_string() + "\n"));
                }
                async move { err_string }
            })
            .await;
        if err_string.is_empty() {
            Ok(())
        } else {
            Err(err_string)
        }
    };
    match bob.block_on(task) {
        Ok(()) => {
            let msg = format!(
                "all disk controllers for disk '{}' successfully started",
                disk_name
            );
            info!("{}", msg);
            Ok(StatusExt::new(Status::Ok, true, msg))
        }
        Err(e) => Err(StatusExt::new(Status::InternalServerError, false, e)),
    }
}

#[get("/vdisks")]
fn vdisks(bob: State<BobServer>) -> Json<Vec<VDisk>> {
    let vdisks = collect_disks_info(&bob);
    Json(vdisks)
}

#[delete("/blobs/outdated")]
fn finalize_outdated_blobs(bob: State<BobServer>) -> StatusExt {
    bob.block_on(async {
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
    let sum = bob.block_on(async {
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
    let pearls = bob.block_on(holders.read());
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
    let pearls = bob.block_on(holders.read());
    let pearl = pearls.iter().find(|pearl| pearl.get_id() == partition_id);
    let partition = pearl.map(|p| Partition {
        node_name: group.node_name().to_owned(),
        disk_name: group.disk_name().to_owned(),
        vdisk_id: group.vdisk_id(),
        timestamp: p.start_timestamp(),
        records_count: bob.block_on(p.records_count()),
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
    let res = format!(
        "partitions with timestamp {} on vdisk {} is successfully {:?}ed",
        timestamp, vdisk_id, action
    );
    let task = async {
        match action {
            Action::Attach => group.attach(timestamp).await,
            Action::Detach => group.detach(timestamp).await.map(|_| ()),
        }
    };
    match bob.block_on(task) {
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
    let task = group.remount();
    match bob.block_on(task) {
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
    bob.block_on(task)
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

#[get("/alien/dir")]
fn get_alien_directory(bob: State<BobServer>) -> Result<Json<Dir>, StatusExt> {
    let backend = bob.grinder().backend().inner();
    let (_, adc) = backend
        .disk_controllers()
        .ok_or_else(not_acceptable_backend)?;
    let path = PathBuf::from(adc.disk().path());
    let dir = bob.block_on(create_directory(&path))?;
    Ok(Json(dir))
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
        let dir = bob.block_on(create_directory(&path))?;
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
    while let Some(child) = read_dir.next_entry().await.transpose() {
        if let Ok(entry) = child {
            let dir = create_directory(&entry.path()).await;
            if let Ok(dir) = dir {
                children.push(dir);
            }
        }
    }
    Dir {
        name: name.to_string(),
        path: path.to_string(),
        children,
    }
}

#[get("/data/<key>")]
fn get_data(bob: State<BobServer>, key: BobKey) -> Result<Content<Vec<u8>>, StatusExt> {
    let opts = BobOptions::new_get(None);
    let result = bob
        .block_on(async { bob.grinder().get(key, &opts).await })
        .map_err(|err| -> StatusExt { err.into() })?;
    let mime_type = infer::get(result.inner());
    let mime_type = match mime_type {
        None => ContentType::Any,
        Some(t) => ContentType::from_str(t.mime_type()).unwrap_or_default(),
    };
    Ok(Content(mime_type, result.inner().to_owned()))
}

#[post("/data/<key>", data = "<data>")]
fn put_data(bob: State<BobServer>, key: BobKey, data: Data) -> Result<StatusExt, StatusExt> {
    let mut data_buf = vec![];
    data.open()
        .read_to_end(&mut data_buf)
        .map_err(|err| -> StatusExt { err.into() })?;
    let data = BobData::new(
        data_buf,
        BobMeta::new(chrono::Local::now().timestamp() as u64),
    );

    let opts = BobOptions::new_put(None);
    bob.block_on(async { bob.grinder().put(key, data, opts).await })
        .map_err(|err| -> StatusExt { err.into() })?;

    Ok(Status::Created.into())
}

fn internal(message: String) -> StatusExt {
    StatusExt::new(Status::InternalServerError, false, message)
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
        use bob_common::error::Kind;
        let status = match err.kind() {
            Kind::DuplicateKey => Status::Conflict,
            Kind::Internal => Status::InternalServerError,
            Kind::VDiskIsNotReady => Status::InternalServerError,
            Kind::KeyNotFound(_) => Status::NotFound,
            _ => Status::BadRequest,
        };
        Self {
            status,
            ok: false,
            msg: err.to_string(),
        }
    }
}
