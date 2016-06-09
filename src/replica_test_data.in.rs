#[derive(Debug, Copy, Clone,Hash, Eq,PartialEq,Ord,PartialOrd, Serialize, Deserialize)]
pub enum NodeId {
    Client,
    Oracle,
    Replica(usize),
}

#[derive(Debug,PartialEq,Eq,PartialOrd,Ord,Hash,Clone, Serialize, Deserialize)]
struct ProcessState {
    time: Timestamp<u64>,
    process: NodeId,
    state: String,
}

#[derive(Debug,PartialEq,Eq,PartialOrd,Ord,Hash,Clone, Serialize, Deserialize)]
struct MessageRecv<M> {
    sent: Timestamp<u64>,
    recv: Timestamp<u64>,
    src: NodeId,
    dst: NodeId,
    data: M,
}
#[derive(Debug,PartialEq,Eq,PartialOrd,Ord,Hash,Clone, Serialize, Deserialize)]
struct NodeCrashed {
    time: Timestamp<u64>,
    process: NodeId,
}

#[derive(Debug,PartialEq,Eq,PartialOrd,Ord,Hash,Clone, Serialize, Deserialize)]
enum TraceEvent<M> {
    ProcessState(ProcessState),
    MessageRecv(MessageRecv<M>),
    NodeCrashed(NodeCrashed),
}

#[derive(Debug,Eq, PartialEq, Hash, Clone, Serialize, Deserialize)]
enum ReplCommand {
    ClientOperation(Buf),
    ConsumeFrom(Seqno),
    Response(OpResp),
    Forward(ReplicationMessage),
    ConsumerMsg(Seqno, Buf),
    ViewChange(ConfigurationView<NodeId>),
}
