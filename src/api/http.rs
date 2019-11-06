use super::prelude::*;

#[derive(Debug)]
pub enum Action {
    Attach,
    Detach,
}

#[derive(Debug, Serialize)]
pub struct Node {
    name: String,
    address: String,
    disks: Vec<Disk>,
}

#[derive(Debug, Serialize)]
pub struct Disk {
    id: u32,
    replicas: Vec<Replica>,
}

#[derive(Debug, Serialize)]
pub struct Replica {
    node: String,
    disk: String,
    path: String,
}

pub fn spawn(bob: &BobSrv) {
    let bob = bob.clone();
    thread::spawn(move || {
        info!("API server started");
        rocket::ignite()
            .manage(bob)
            .mount(
                "/",
                routes![
                    status,
                    vdisks,
                    vdisk_by_id,
                    partitions,
                    partition_by_id,
                    change_partition_state,
                    alien
                ],
            )
            .launch();
    });
}

fn collect_disks_info(vdisks: &[DataVDisk]) -> Vec<Disk> {
    vdisks
        .iter()
        .map(|disk| Disk {
            id: disk.id.as_u32(),
            replicas: collect_replicas_info(&disk.replicas),
        })
        .collect()
}

fn collect_replicas_info(replicas: &[DataNodeDisk]) -> Vec<Replica> {
    replicas
        .iter()
        .map(|r| Replica {
            path: r.disk_path.to_owned(),
            disk: r.disk_name.to_owned(),
            node: r.node_name.to_owned(),
        })
        .collect()
}

#[get("/status")]
fn status(bob: State<BobSrv>) -> Json<Node> {
    let mapper = bob.grinder.backend.mapper();
    let name = mapper.local_node_name().to_owned();
    let address = mapper.local_node_address();
    let vdisks = mapper.vdisks();
    let disks = collect_disks_info(vdisks);
    let node = Node {
        name,
        address,
        disks,
    };
    Json(node)
}

#[get("/vdisks")]
fn vdisks(_bob: State<BobSrv>) -> &'static str {
    "vdisks"
}

#[get("/vdisks/<vdisk_id>")]
fn vdisk_by_id(_bob: State<BobSrv>, vdisk_id: usize) -> String {
    format!("vdisk by id {}", vdisk_id)
}

#[get("/vdisks/<vdisk_id>/partitions")]
fn partitions(_bob: State<BobSrv>, vdisk_id: usize) -> String {
    format!("partitions of vdisk {}", vdisk_id)
}

#[get("/vdisks/<vdisk_id>/partitions/<partition_id>")]
fn partition_by_id(_bob: State<BobSrv>, vdisk_id: usize, partition_id: usize) -> String {
    format!("partition {} of vdisk {}", partition_id, vdisk_id)
}

#[put("/vdisks/<vdisk_id>/partitions/<partition_id>/<action>")]
fn change_partition_state(
    _bob: State<BobSrv>,
    vdisk_id: usize,
    partition_id: usize,
    action: Action,
) -> String {
    format!(
        "{:?} partittion {} of vd {}",
        action, partition_id, vdisk_id
    )
}

#[get("/alien")]
fn alien(_bob: State<BobSrv>) -> &'static str {
    "alien"
}

impl<'r> FromParam<'r> for Action {
    type Error = &'r RawStr;

    fn from_param(param: &'r RawStr) -> Result<Self, Self::Error> {
        error!("{}", param.as_str());
        match param.as_str() {
            "attach" => Ok(Action::Attach),
            "detach" => Ok(Action::Detach),
            _ => Err(param),
        }
    }
}
