#[derive(Debug)]
pub struct Settings {}

pub trait AccessStorage {
    fn load(&self) -> Settings;
}
