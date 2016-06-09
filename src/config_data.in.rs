#[derive(Debug,PartialEq,Eq,PartialOrd,Ord,Hash,Clone,Copy,Default, Serialize, Deserialize)]
pub struct Epoch(u64);

#[derive(Debug,PartialEq,Eq,Default,Clone, Serialize, Deserialize)]
struct ConfigSequencer {
    keys: Vec<String>,
    epoch: Epoch,
}

#[derive(Clone,Debug, Eq, PartialEq, Hash, Default, Serialize, Deserialize)]
pub struct ConfigurationView<T> {
    pub epoch: Epoch,
    pub ord: usize,
    pub next: Option<T>,
}


