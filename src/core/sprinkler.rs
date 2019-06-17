pub struct SprinklerGetResult {
    pub data: Vec<u8>,
}

#[derive(Debug)]
pub struct SprinklerGetError {}

#[derive(Debug)]
pub struct SprinklerError {
    pub total_ops: u16,
    pub ok_ops: u16,
    pub quorum: u8,
}

impl std::fmt::Display for SprinklerError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "ok:{} total:{} q:{}",
            self.ok_ops, self.total_ops, self.quorum
        )
    }
}

#[derive(Debug)]
pub struct SprinklerResult {
    pub total_ops: u16,
    pub ok_ops: u16,
    pub quorum: u8,
}

impl std::fmt::Display for SprinklerResult {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "ok:{} total:{} q:{}",
            self.ok_ops, self.total_ops, self.quorum
        )
    }
}