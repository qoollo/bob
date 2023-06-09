use crate::{
    node::NodeName,
};
use bob_grpc::{DeleteOptions, GetOptions, GetSource, PutOptions};

#[derive(Debug, Clone)]
pub struct BobPutOptions {
    force_node: bool,
    overwrite: bool,
    remote_nodes: Vec<NodeName>
}

#[derive(Debug, Clone)]
pub struct BobGetOptions {
    force_node: bool,
    get_source: GetSource,
}

#[derive(Debug, Clone)]
pub struct BobDeleteOptions {
    force_node: bool,
    is_alien: bool,
    force_alien_nodes: Vec<NodeName>
}

impl BobPutOptions {
    pub fn new_local() -> Self {
        BobPutOptions {
            remote_nodes: vec![],
            force_node: true,
            overwrite: false,
        }
    }

    pub fn new_alien(remote_nodes: Vec<NodeName>) -> Self {
        BobPutOptions {
            remote_nodes,
            force_node: true,
            overwrite: false,
        }
    }

    pub fn from_grpc(options: Option<PutOptions>) -> Self {
        if let Some(vopts) = options {
            BobPutOptions {
                force_node: vopts.force_node,
                overwrite: vopts.overwrite,
                remote_nodes: vopts.remote_nodes.iter().map(|nn| NodeName::from(nn)).collect()
            }
        } else {
            BobPutOptions {
                force_node: false,
                overwrite: false,
                remote_nodes: vec![]
            }
        }
    }

    pub fn to_grpc(&self) -> PutOptions {
        PutOptions { 
            remote_nodes: self.remote_nodes.iter().map(|nn| nn.to_string()).collect(), 
            force_node: self.force_node, 
            overwrite: self.overwrite 
        }
    }

    pub fn force_node(&self) -> bool {
        self.force_node
    }

    pub fn overwrite(&self) -> bool {
        self.overwrite
    }

    pub fn to_alien(&self) -> bool {
        !self.remote_nodes.is_empty()
    }

    pub fn remote_nodes(&self) -> &[NodeName] {
        &self.remote_nodes
    }
}

impl BobGetOptions {
    pub fn new_local() -> Self {
        BobGetOptions {
            force_node: true,
            get_source: GetSource::Normal,
        }
    }

    pub fn new_alien() -> Self {
        BobGetOptions {
            force_node: true,
            get_source: GetSource::Alien,
        }
    }

    pub fn new_all() -> Self {
        BobGetOptions {
            force_node: true,
            get_source: GetSource::All,
        }
    }


    pub fn from_grpc(options: Option<GetOptions>) -> Self {
        if let Some(vopts) = options {
            BobGetOptions {
                force_node: vopts.force_node,
                get_source: GetSource::from(vopts.source)
            }
        } else {
            BobGetOptions {
                force_node: false,
                get_source: GetSource::All
            }
        }
    }

    pub fn to_grpc(&self) -> GetOptions {
        GetOptions { 
            force_node: self.force_node, 
            source: self.get_source.into()
        }
    }

    pub fn force_node(&self) -> bool {
        self.force_node
    }

    pub fn get_all(&self) -> bool {
        self.get_source == GetSource::All
    }

    pub fn get_normal(&self) -> bool {
        self.get_source == GetSource::Normal || self.get_source == GetSource::All
    }

    pub fn get_alien(&self) -> bool {
        self.get_source == GetSource::Alien || self.get_source == GetSource::All
    }
}


impl BobDeleteOptions {
    pub fn new_local() -> Self {
        BobDeleteOptions {
            force_node: true,
            is_alien: false,
            force_alien_nodes: vec![]
        }
    }

    pub fn new_alien(force_alien_nodes: Vec<NodeName>) -> Self {
        BobDeleteOptions { 
            force_node: true, 
            is_alien: true, 
            force_alien_nodes: force_alien_nodes
        }
    }

    pub fn from_grpc(options: Option<DeleteOptions>) -> Self {
        if let Some(vopts) = options {
            BobDeleteOptions {
                force_node: vopts.force_node,
                is_alien: vopts.is_alien,
                force_alien_nodes: vopts.force_alien_nodes.iter().map(|nn| NodeName::from(nn)).collect()
            }
        } else {
            BobDeleteOptions {
                force_node: false,
                is_alien: false,
                force_alien_nodes: vec![]
            }
        }
    }

    pub fn to_grpc(&self) -> DeleteOptions {
        DeleteOptions { 
            force_alien_nodes: self.force_alien_nodes.iter().map(|nn| nn.to_string()).collect(), 
            force_node: self.force_node, 
            is_alien: self.is_alien 
        }
    }

    pub fn force_node(&self) -> bool {
        self.force_node
    }

    pub fn to_alien(&self) -> bool {
        self.is_alien
    }

    pub fn force_delete_nodes(&self) -> &[NodeName] {
        &self.force_alien_nodes
    }

    pub fn is_force_delete(&self, node_name: &NodeName) -> bool {
        self.force_alien_nodes.iter().any(|x| x == node_name)
    }
}