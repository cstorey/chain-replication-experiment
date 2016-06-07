#[derive(Debug, Copy, Clone,Hash, Eq,PartialEq,Ord,PartialOrd, Serialize, Deserialize)]
pub struct NodeId(usize);
impl From<usize> for NodeId {
    fn from(id: usize) -> NodeId {
        NodeId(id)
    }
}

#[derive(Debug,PartialEq,Eq,PartialOrd,Ord,Hash,Clone, Serialize, Deserialize)]
struct ProcessState {
    time: Timestamp<u64>,
    process: NodeId,
    state: String,
}

#[derive(Debug,PartialEq,Eq,PartialOrd,Ord,Hash,Clone, Serialize, Deserialize)]
struct MessageRecv {
    sent: Timestamp<u64>,
    recv: Timestamp<u64>,
    src: NodeId,
    dst: NodeId,
    data: String,
}
#[derive(Debug,PartialEq,Eq,PartialOrd,Ord,Hash,Clone, Serialize, Deserialize)]
struct NodeCrashed {
    time: Timestamp<u64>,
    process: NodeId,
}

#[derive(Debug,PartialEq,Eq,PartialOrd,Ord,Hash,Clone, Serialize, Deserialize)]
enum TraceEvent {
    ProcessState(ProcessState),   
    MessageRecv(MessageRecv),   
    NodeCrashed(NodeCrashed),   
}
