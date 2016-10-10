pub trait Store {}

#[derive(Debug,Clone)]
pub struct RamStore;

impl RamStore {
    pub fn new() -> Self {
        RamStore
    }
}

impl Store for RamStore {}
