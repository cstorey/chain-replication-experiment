
use vastatrix::data::{Buf, OpResp, Operation, PeerMsg, ReplicationMessage, Seqno};
use vastatrix::config::Epoch;
use quickcheck::{self, Arbitrary, Gen, TestResult};
use vastatrix::replica::{Log, Outputs, ReplModel};
use std::sync::mpsc::channel;
use replication_log::{TestLog, VecLog, hash};
use vastatrix::config::ConfigurationView;
use env_logger;
use spki_sexp;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use mio::Token;
use hybrid_clocks::{Clock, ManualClock, Timestamp, WallT};
use std::mem;
use std::iter;
use std::sync::{Arc, Mutex};
use std::path::{Path, PathBuf};
use std::fs::File;
use serde_json;
use time;

#[derive(Debug)]
pub enum OutMessage {
    Response(Token, OpResp),
    Forward(ReplicationMessage),
    LogCommitted(Seqno),
    ConsumerMessage(Token, Seqno, Buf),
}

#[derive(Debug)]
pub struct Outs(VecDeque<OutMessage>);
impl Outs {
    pub fn new() -> Outs {
        Outs(VecDeque::new())
    }
    pub fn inner(self) -> VecDeque<OutMessage> {
        self.0
    }

    pub fn borrow(&self) -> &VecDeque<OutMessage> {
        &self.0
    }
}

impl Outputs for Outs {
    fn respond_to(&mut self, token: Token, resp: &OpResp) {
        self.0.push_back(OutMessage::Response(token, resp.clone()))
    }
    fn forward_downstream(&mut self, now: Timestamp<WallT>, epoch: Epoch, msg: PeerMsg) {
        self.0.push_back(OutMessage::Forward(ReplicationMessage {
            epoch: epoch,
            ts: now,
            msg: msg,
        }))
    }
    fn consumer_message(&mut self, token: Token, seqno: Seqno, msg: Buf) {
        self.0.push_back(OutMessage::ConsumerMessage(token, seqno, msg))
    }
}

#[derive(Debug,Eq,PartialEq,Clone)]
enum ReplCommand {
    ClientOperation(Token, Vec<u8>),
    ConsumeFrom(Token, Seqno),
    Response(Token, OpResp),
    Forward(ReplicationMessage),
    ConsumerMsg(Seqno, Buf),
}

#[derive(Debug,Eq,PartialEq,Clone)]
struct Commands(Vec<ReplCommand>);

#[derive(Debug,PartialEq,Eq,Clone)]
struct FakeReplica {
    epoch: Epoch,
    prepared: Option<Seqno>,
    committed: Option<Seqno>,
    responded: Option<Seqno>,
    outstanding: HashMap<Token, isize>,
    log_committed: Option<Seqno>,
}

impl FakeReplica {
    fn new() -> FakeReplica {
        FakeReplica {
            epoch: Default::default(),
            prepared: Default::default(),
            committed: Default::default(),
            responded: Default::default(),
            outstanding: HashMap::new(),
            log_committed: Default::default(),
        }
    }
}

impl Arbitrary for ReplCommand {
    fn arbitrary<G: Gen>(g: &mut G) -> ReplCommand {
        let case = u64::arbitrary(g) % 1;
        let res = match case {
            0 => {
                ReplCommand::ClientOperation(Token(Arbitrary::arbitrary(g)),
                                             Arbitrary::arbitrary(g))
            }
            _ => unimplemented!(),
        };
        res
    }
    fn shrink(&self) -> Box<Iterator<Item = Self> + 'static> {
        match self {
            &ReplCommand::ClientOperation(ref token, ref op) => {
                Box::new((token.as_usize(), op.clone())
                             .shrink()
                             .map(|(t, op)| ReplCommand::ClientOperation(Token(t), op)))
            }
            _ => unimplemented!(),
        }
    }
}

fn precondition(_model: &FakeReplica, cmd: &ReplCommand) -> bool {
    match cmd {
        &ReplCommand::ClientOperation(ref _token, ref _s) => true,
        _ => unimplemented!(),
    }
}

fn postcondition<L: TestLog>(_actual: &ReplModel<L>,
                             model: &FakeReplica,
                             cmd: &ReplCommand,
                             observed: &Outs)
                             -> bool {
    debug!("observed: {:?}; model:{:?}", observed, model);
    for msg in observed.0.iter() {
        match msg {
            &OutMessage::Forward(ReplicationMessage { epoch, msg: PeerMsg::Prepare(seqno, _), .. }) => {
                    assert_eq!(epoch, model.epoch);
                    assert!(Some(seqno) > model.prepared);
                }
            &OutMessage::Forward(ReplicationMessage { epoch, msg: PeerMsg::CommitTo(seqno), .. }) => {
                    assert_eq!(epoch, model.epoch);
                    assert!(Some(seqno) <= model.prepared);
                    assert!(Some(seqno) <= model.log_committed);
                }
            &OutMessage::Forward(ReplicationMessage { epoch, .. }) => {
                assert_eq!(epoch, model.epoch);
            }
            &OutMessage::Response(token, OpResp::Ok(epoch, seqno, _)) |
            &OutMessage::Response(token, OpResp::Err(epoch, seqno, _)) => {
                assert!(model.outstanding.contains_key(&token));
                assert_eq!(epoch, model.epoch);
                assert!(model.responded < Some(seqno));
            }
            &OutMessage::Response(token, _) => {
                assert!(model.outstanding.contains_key(&token));
            }
            &OutMessage::LogCommitted(_seqno) => {}
            &OutMessage::ConsumerMessage(_, _, _) => {}
        };
    }

    match cmd {
        &ReplCommand::ClientOperation(ref _token, ref _s) => true,
        _ => unimplemented!(),
    }
}

impl FakeReplica {
    fn next_state(&mut self, cmd: &ReplCommand) -> () {
        match cmd {
            &ReplCommand::ClientOperation(ref token, ref _op) => {
                *self.outstanding.entry(*token).or_insert(0) += 1;
            }
            _ => unimplemented!(),
        }
    }

    fn update_from_outputs(&mut self, observed: &Outs) -> () {
        for msg in observed.0.iter() {
            match msg {
                &OutMessage::Forward(ReplicationMessage { epoch, msg: PeerMsg::Prepare(seqno, _), .. }) => {
                        self.epoch = epoch;
                        self.prepared = Some(seqno);
                    }
                &OutMessage::Forward(ReplicationMessage { epoch, msg: PeerMsg::CommitTo(seqno), .. }) => {
                        self.epoch = epoch;
                        self.committed = Some(seqno);
                    }
                &OutMessage::Forward(ReplicationMessage { epoch, msg: _, .. }) => {
                    self.epoch = epoch;
                }
                &OutMessage::Response(token, OpResp::Ok(epoch, seqno, _)) |
                &OutMessage::Response(token, OpResp::Err(epoch, seqno, _)) => {
                    *self.outstanding.entry(token).or_insert(0) -= 1;
                    self.epoch = epoch;
                    self.responded = Some(seqno);
                }
                &OutMessage::Response(token, _) => {
                    *self.outstanding.entry(token).or_insert(0) -= 1;
                }
                &OutMessage::LogCommitted(seqno) => {
                    self.log_committed = Some(seqno);
                }
                &OutMessage::ConsumerMessage(_, _, _) => {}
            };
        }
    }
}

fn apply_cmd<L: TestLog, O: Outputs>(actual: &mut ReplModel<L>,
                                     cmd: &ReplCommand,
                                     token: Token,
                                     outputs: &mut O)
                                     -> () {
    match cmd {
        &ReplCommand::ClientOperation(ref token, ref op) => {
            actual.process_client(outputs, *token, &op)
        }

        &ReplCommand::Forward(
                ReplicationMessage { epoch, msg: PeerMsg::Prepare(seq, ref data) , .. }) => {
                actual.process_operation(outputs, token, seq, epoch, &data);
            }
        &ReplCommand::Forward(ReplicationMessage { epoch, msg: PeerMsg::CommitTo(seq) , .. }) => {
            actual.commit_observed(seq);
        }
        &ReplCommand::Forward(
                ReplicationMessage { epoch, ts, msg: PeerMsg::HelloDownstream , .. }) => {
                actual.hello_downstream(outputs, token, ts, epoch);
            }
        &ReplCommand::Response(token, ref reply) => {
            actual.process_downstream_response(outputs, reply)
        }
        &ReplCommand::ConsumeFrom(token, seq) => actual.consume_requested(outputs, token, seq),
        other => panic!("Unimplemented apply_cmd: {:?}", other),
    };
    actual.process_replication(outputs);
}

impl Arbitrary for Commands {
    fn arbitrary<G: Gen>(g: &mut G) -> Commands {
        let slots: Vec<()> = Arbitrary::arbitrary(g);
        let mut commands: Vec<ReplCommand> = Vec::new();
        let mut model = FakeReplica::new();

        for _ in slots {
            let cmd = (0..)
                          .map(|_| {
                              let cmd: ReplCommand = Arbitrary::arbitrary(g);
                              cmd
                          })
                          .skip_while(|cmd| !precondition(&model, cmd))
                          .next()
                          .expect("Some valid command");

            model.next_state(&cmd);
            commands.push(cmd);
        }
        Commands(commands)
    }
    fn shrink(&self) -> Box<Iterator<Item = Self> + 'static> {
        // TODO: Filter out invalid sequences.
        let ret = Arbitrary::shrink(&self.0).map(Commands).filter(validate_commands);
        Box::new(ret)
    }
}
fn validate_commands(cmds: &Commands) -> bool {
    let model = FakeReplica::new();

    cmds.0
        .iter()
        .scan(model, |model, cmd| {
            let ret = precondition(&model, cmd);
            model.next_state(&cmd);
            Some(ret)
        })
        .all(|p| p)
}

#[test]
fn simulate_three_node_system_vec() {
    env_logger::init().unwrap_or(());
    let mut sim = NetworkSim::<VecLog>::new(3);
    sim.run_for(10, "simulate_three_node_system_vec", |t, sim, state| {
        if t == 0 {
            sim.client_operation(state, b"hello_world".to_vec());
        }
    });
    sim.validate_client_responses(1);
    sim.validate_logs();
}

#[test]
fn simulate_three_node_system_streaming_vec() {
    env_logger::init().unwrap_or(());
    let mut sim = NetworkSim::<VecLog>::new(3);
    sim.run_for(15,
                "simulate_three_node_system_streaming_vec",
                |t, sim, state| {
                    if t < 3 {
                        sim.client_operation(state, format!("hello_world at {}", t).into_bytes())
                    }
                });
    sim.validate_logs();
    sim.validate_client_responses(3);
}

#[test]
fn simulate_three_node_system_with_consumer() {
    env_logger::init().unwrap_or(());
    let mut sim = NetworkSim::<VecLog>::new(3);
    let mut produced_messages = Vec::new();
    sim.run_for(15,
                "simulate_three_node_system_with_consumer",
                |t, sim, state| {
                    if t == 0 {
                        sim.consume_from(state, Seqno::zero())
                    }
                    if t < 3 {
                        let m = format!("hello_world at {}", t).into_bytes();
                        produced_messages.push(Buf::from(m.clone()));
                        sim.client_operation(state, m);
                    }
                });
    sim.validate_logs();
    sim.validate_client_responses(produced_messages.len());

    debug!("consumed: {:?}", sim.consumed_messages());
    assert_eq!(sim.consumed_messages().into_iter().map(|(s, v)| v).collect::<Vec<_>>(),
               produced_messages)
}

#[test]
fn simulate_tail_crash() {
    env_logger::init().unwrap_or(());

    let mut sim = NetworkSim::<VecLog>::new(3);
    let mut produced_messages = Vec::new();
    sim.run_for(15, "simulate_tail_crash", |t, sim, state| {
        if t == 2 {
            let tail = sim.tail_node();
            sim.crash_node(state, tail)
        }

        if t < 3 {
            let m = format!("hello_world at {}", t).into_bytes();
            produced_messages.push(Buf::from(m.clone()));
            sim.client_operation(state, m);
        }
    });
    sim.validate_logs();
    sim.validate_client_responses(3);
}

struct OutBufs<'a> {
    node_id: NodeId,
    view: ConfigurationView<NodeId>,
    epoch: Epoch,
    ports: &'a mut NetworkState,
}

impl<'a> OutBufs<'a> {
    fn enqueue(&mut self, dest: NodeId, cmd: ReplCommand) {
        let &mut OutBufs { node_id, .. } = self;
        self.ports.enqueue(node_id, dest, cmd);
    }
}

impl<'a> Outputs for OutBufs<'a> {
    fn respond_to(&mut self, token: Token, resp: &OpResp) {
        let cmd = ReplCommand::Response(token, resp.clone());
        self.enqueue(NodeId(token.as_usize()), cmd);
    }

    fn forward_downstream(&mut self, now: Timestamp<WallT>, epoch: Epoch, msg: PeerMsg) {
        if let Some(downstream_id) = self.view.next {
            self.enqueue(downstream_id,
                         ReplCommand::Forward(ReplicationMessage {
                             epoch: epoch,
                             ts: now,
                             msg: msg,
                         }));
        } else {
            warn!("OutBufs#forward_downstream no next in view; node: {:?}, view: {:?}; msg: \
                       {:?}",
                  self.node_id,
                  self.view,
                  msg);

        }
    }

    fn consumer_message(&mut self, token: Token, seqno: Seqno, msg: Buf) {
        let &mut OutBufs { node_id, .. } = self;
        self.enqueue(NodeId(token.as_usize()),
                     ReplCommand::ConsumerMsg(seqno, msg))
    }
}

struct NetworkSim<L> {
    nodes: BTreeMap<NodeId, ReplModel<L>>,
    node_count: usize,
    epoch: Epoch,
    client_id: NodeId,
    client_buf: VecDeque<ReplCommand>,
}

#[derive(Debug)]
struct NetworkState {
    tracer: Tracer,
    clock: Clock<ManualClock>,
    input_bufs: HashMap<NodeId, VecDeque<(NodeId, Timestamp<u64>, ReplCommand)>>,
    output_bufs: HashMap<NodeId, VecDeque<(NodeId, Timestamp<u64>, ReplCommand)>>,
}

impl NetworkState {
    fn is_quiescent(&self) -> bool {
        self.input_bufs
            .iter()
            .chain(self.output_bufs.iter())
            .flat_map(|(_, q)| q)
            .all(|_| false)
    }

    fn enqueue(&mut self, src: NodeId, dst: NodeId, data: ReplCommand) {
        let sent = self.clock.now();
        self.output_bufs.entry(dst).or_insert_with(VecDeque::new).push_back((src, sent, data));
        trace!("enqueue:{:?}->{:?}; {:?}", src, dst, self);
    }

    fn dequeue_one(&mut self) -> Option<(NodeId, NodeId, ReplCommand)> {
        let (dest, ref mut queue) = if let Some((dest, q)) = self.input_bufs
                                                                 .iter_mut()
                                                                 .filter(|&(_, ref q)| {
                                                                     !q.is_empty()
                                                                 })
                                                                 .next() {
            trace!("dequeue_one: Node:{:?}: {:?}", dest, q);
            (*dest, q)
        } else {
            trace!("dequeue_one: empty?");
            return None;
        };
        if let Some((src, sent, it)) = queue.pop_front() {
            let recv = self.clock.now();
            self.tracer.recv(sent, recv, &src, &dest, format!("{:?}", it));
            Some((src, dest, it))
        } else {
            None
        }
    }

    fn flip(&mut self) {
        trace!("flip before:{:?}", self);
        {
            let &mut NetworkState { ref mut input_bufs, ref mut output_bufs, .. } = self;
            mem::swap(input_bufs, output_bufs);
        }
        trace!("flip after :{:?}", self);
    }

    fn crash_node(&mut self, node_id: NodeId) {
        let t = self.clock.now();
        self.tracer.node_crashed(t, &node_id);
    }
}

#[cfg(feature = "serde_macros")]
include!("replica_test_data.in.rs");

#[cfg(not(feature = "serde_macros"))]
include!(concat!(env!("OUT_DIR"), "/replica_test_data.rs"));

#[derive(Debug)]
struct Tracer {
    entries: Vec<TraceEvent>,
}

impl Tracer {
    fn new() -> Tracer {
        Tracer { entries: Vec::new() }
    }
    fn state(&mut self, t: Timestamp<u64>, process: &NodeId, state: String) {

        let m = ProcessState {
            time: t,
            process: *process,
            state: state,
        };
        self.entries.push(TraceEvent::ProcessState(m));
    }

    fn recv(&mut self,
            sent: Timestamp<u64>,
            recv: Timestamp<u64>,
            src: &NodeId,
            dst: &NodeId,
            data: String) {
        let m = MessageRecv {
            sent: sent,
            recv: recv,
            src: *src,
            dst: *dst,
            data: data,
        };
        self.entries.push(TraceEvent::MessageRecv(m));
    }

    fn node_crashed(&mut self, t: Timestamp<u64>, process: &NodeId) {
        let m = NodeCrashed {
            time: t,
            process: *process,
        };
        self.entries.push(TraceEvent::NodeCrashed(m));
    }
    fn persist_to(&self, path: &Path) {
        let mut f = File::create(path).expect("create file");
        serde_json::to_writer(&mut f, &self.entries).expect("write json");
    }
}

impl<L: TestLog> NetworkSim<L> {
    fn new(node_count: usize) -> Self {
        let epoch = Epoch::from(0);

        let nodes = (0..node_count).map(|id| (NodeId(id), ReplModel::new(L::new()))).collect();

        NetworkSim {
            nodes: nodes,
            node_count: node_count,
            epoch: epoch,
            client_id: NodeId(node_count + 42),
            client_buf: VecDeque::new(),
        }
    }

    fn config_of(&self, node_id: NodeId) -> ConfigurationView<NodeId> {
        let node_count = self.node_count;
        let members = self.nodes.iter().map(|(&id, _)| (id, id)).collect::<BTreeMap<_, _>>();
        ConfigurationView::of_membership(self.epoch, &node_id, members.clone()).expect("in view")
    }

    fn configs(&self) -> BTreeMap<NodeId, ConfigurationView<NodeId>> {
        let node_count = self.node_count;
        let members = self.nodes.iter().map(|(&id, _)| (id, id)).collect::<BTreeMap<_, _>>();
        members.keys()
               .map(|&id| {
                   (id,
                    ConfigurationView::of_membership(self.epoch, &id, members.clone())
                        .expect("in view"))
               })
               .collect()
    }

    fn run_for<F: FnMut(usize, &mut Self, &mut NetworkState)>(&mut self,
                                                              end_of_time: usize,
                                                              test_name: &str,
                                                              mut f: F) {
        let mut epoch = None;
        let path = PathBuf::from(format!("target/run-trace-{}.jsons", test_name));
        let mut tracer = Tracer::new();

        let mut state = NetworkState {
            clock: Clock::manual(0),
            tracer: tracer,
            input_bufs: HashMap::new(),
            output_bufs: HashMap::new(),
        };

        for t in 0..end_of_time {
            state.clock.set_time(t as u64);
            if epoch != Some(self.epoch) {
                let configs = self.configs();
                for (id, n) in self.nodes.iter_mut() {
                    let config = &configs[id];
                    debug!("configure epoch: {:?}; node: {:?}", self.epoch, config);
                    n.reconfigure(config)
                }
                epoch = Some(self.epoch);
            }

            let now = state.clock.now();
            for (id, n) in self.nodes.iter() {
                state.tracer.state(now, id, format!("{:?}", n));
            }

            debug!("time: {:?}/{:?}", t, end_of_time);
            f(t, self, &mut state);
            if state.is_quiescent() {
                break;
            }

            self.step(&mut state);
            debug!("network state {:?}; quiet:{:?}; : {:?}",
                   t,
                   state.is_quiescent(),
                   state);

        }
        state.tracer.persist_to(&path);
        assert!(state.is_quiescent());
    }

    fn tail_node(&self) -> NodeId {
        NodeId(self.node_count - 1)
    }

    fn head_node(&self) -> NodeId {
        NodeId(0)
    }

    fn crash_node(&mut self, state: &mut NetworkState, node_id: NodeId) {
        state.crash_node(node_id);
        if let Some(old) = self.nodes.remove(&node_id) {
            info!("Crashing node: {:?}: {:?}", node_id, old);
            self.epoch = self.epoch.succ();
        }
    }

    fn client_operation(&self, state: &mut NetworkState, val: Vec<u8>) {
        let head = self.head_node();
        let cmd = ReplCommand::ClientOperation(self.client_id.token(), val);
        state.enqueue(self.client_id, head, cmd);
    }

    fn consume_from(&self, state: &mut NetworkState, seqno: Seqno) {
        let tail = self.tail_node();
        let cmd = ReplCommand::ConsumeFrom(self.client_id.token(), seqno);

        state.enqueue(self.client_id, tail, cmd);
    }

    fn unexpected_responses(&self) -> Vec<&ReplCommand> {
        self.client_buf
            .iter()
            .filter_map(|m| {
                match m {
                    &ReplCommand::Response(_, _) |
                    &ReplCommand::ConsumerMsg(_, _) => None,
                    other => Some(other),
                }
            })
            .collect()
    }

    fn client_responses<'a>(&'a self) -> Vec<&'a OpResp> {
        self.client_buf
            .iter()
            .filter_map(|m| {
                match m {
                    &ReplCommand::Response(_, ref m) => Some(m),
                    _ => None,
                }
            })
            .collect()
    }

    fn acked_seqnos(&self) -> Vec<Seqno> {
        self.client_responses()
            .into_iter()
            .filter_map(|m| {
                match m {
                    &OpResp::Ok(_, ref seq, _) => Some(seq),
                    _ => None,
                }
            })
            .cloned()
            .collect()
    }

    fn validate_client_responses(&self, count: usize) {
        debug!("Client responses: {:?}", self.client_buf);

        assert_eq!(self.unexpected_responses(), Vec::<&ReplCommand>::new());

        let (ok, errors): (Vec<_>, Vec<_>) = self.client_responses()
                                                 .into_iter()
                                                 .partition(|&m| {
                                                     match m {
                                                         &OpResp::Ok(_, _, _) => true,
                                                         _ => false,
                                                     }
                                                 });

        debug!("Errors: {:?}", errors);
        debug!("Ok: {:?}", ok);
        // We should get some response for all items.
        assert_eq!(ok.len() + errors.len(), count);
        // assert!(errors.is_empty());

        let seqs = self.acked_seqnos();

        // For now, we'll assume that we either get okays or errors.
        // assert_eq!(seqs.len(), count);
        assert!(seqs.iter().zip(seqs.iter().skip(1)).all(|(a, b)| a < b),
                "is sorted");
    }

    fn validate_logs(&self) {
        let logs_by_node = self.nodes
                               .iter()
                               .map(|(id, n)| {
                                   let committed_seq = n.borrow_log().read_committed();
                                   debug!("Committed @{:?}: {:?}", id, committed_seq);
                                   let committed = n.borrow_log()
                                                    .read_from(Seqno::zero())
                                                    .take_while(|&(s, _)| Some(s) <= committed_seq)
                                                    .map(|(s, val)| (s, hash(&val)))
                                                    .collect::<BTreeMap<_, _>>();
                                   (id, committed)
                               })
                               .collect::<BTreeMap<_, _>>();

        debug!("Committed logs: {:#?}", logs_by_node);
        for (a, log_a) in logs_by_node.iter() {
            for (b, log_b) in logs_by_node.iter().filter(|&(b, _)| a != b) {
                debug!("compare log for {:?} ({:x}) with {:?} ({:x})",
                       a,
                       hash(log_a),
                       b,
                       hash(log_b));
                assert_eq!(log_a, log_b);
            }
        }

        let acked_seqnos = self.acked_seqnos().into_iter().collect::<HashSet<Seqno>>();
        let consumed_seqnos = self.consumed_messages()
                                  .into_iter()
                                  .map(|(s, _)| s)
                                  .collect::<HashSet<Seqno>>();
        for (n, log) in logs_by_node.iter() {
            let log_seqnos = log.keys().cloned().collect::<HashSet<Seqno>>();

            let acked_diff = acked_seqnos.difference(&log_seqnos)
                                         .cloned()
                                         .collect::<Vec<Seqno>>();
            debug!("Acked but not in log for {:?}: {:?}", n, acked_diff);
            assert_eq!(acked_diff, vec![]);

            let consumed_diff = consumed_seqnos.difference(&log_seqnos)
                                               .cloned()
                                               .collect::<Vec<Seqno>>();
            debug!("Consumed but not in log for {:?}: {:?}", n, consumed_diff);
            assert_eq!(consumed_diff, vec![]);
        }
    }

    fn consumed_messages(&self) -> Vec<(Seqno, Buf)> {
        self.client_buf
            .iter()
            .filter_map(|m| {
                match m {
                    &ReplCommand::ConsumerMsg(seq, ref buf) => Some((seq, buf.clone())),
                    _ => None,
                }
            })
            .collect()
    }

    fn step(&mut self, state: &mut NetworkState) {
        let views = self.configs();
        while let Some((src, dest, input)) = state.dequeue_one() {
            debug!("Inputs: {:?} -> {:?}: {:?}", src, dest, input);
            if dest == self.client_id {
                self.client_buf.push_back(input);
            } else {
                let view = views[&dest].clone();
                if let Some(ref mut n) = self.nodes.get_mut(&dest) {
                    let mut output = OutBufs {
                        node_id: dest,
                        view: view,
                        ports: state,
                        epoch: self.epoch,
                    };

                    debug!("Feed node {:?} with {:?}", dest, input);
                    apply_cmd(n, &input, src.token(), &mut output);

                } else {
                    warn!("Dropping message for node: {:?}: {:?}", dest, input);
                }
            }
        }

        for (nid, n) in self.nodes.iter_mut() {
            n.borrow_log().quiesce();

            let view = views[nid].clone();
            let mut output = OutBufs {
                node_id: *nid,
                view: view,
                ports: state,
                epoch: self.epoch,
            };
            while n.process_replication(&mut output) {
                debug!("iterated replication for node {:?}", nid);
            }
        }

        state.flip();
    }
}
