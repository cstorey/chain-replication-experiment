use vastatrix::data::{Buf, OpResp, PeerMsg, ReplicationMessage, Seqno};
use vastatrix::config::Epoch;
use vastatrix::replica::{Log, Outputs, ReplModel};
use replication_log::{TestLog, VecLog, hash};
use vastatrix::config::ConfigurationView;
use env_logger;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use hybrid_clocks::{Clock, ManualClock, Timestamp, WallT};
use std::mem;
use std::cmp;
use std::iter;
use std::path::{Path, PathBuf};
use std::fs::File;
use std::io::Write;
use serde_json;
use std::hash::{SipHasher,Hash, Hasher};

use bformulae::Bools;

use petgraph::Graph;
use petgraph::visit::DfsIter;

#[cfg(feature = "serde_macros")]
include!("replica_test_data.in.rs");

#[cfg(not(feature = "serde_macros"))]
include!(concat!(env!("OUT_DIR"), "/replica_test_data.rs"));

#[derive(Debug,Eq,PartialEq,Clone)]
struct Commands(Vec<ReplCommand>);


struct IteratedF<T, F>(T, F);

fn iterate<T: Clone, F: Fn(&T) -> T>(init: T, f: F) -> IteratedF<T, F> {
    IteratedF(init, f)
}

impl<T: Clone, F: Fn(&T) -> T> iter::Iterator for IteratedF<T, F> {
    type Item = T;
    fn next(&mut self) -> Option<T> {
        let &mut IteratedF(ref mut val, ref f) = self;
        let curr = val.clone();
        *val = f(&curr);
        Some(curr)
    }
}

fn apply_cmd<L: TestLog, O: Outputs<Dest = NodeId>>(actual: &mut ReplModel<L, NodeId>,
                                                    cmd: &ReplCommand,
                                                    src: NodeId,
                                                    outputs: &mut O)
                                                    -> () {
    match cmd {
        &ReplCommand::ClientOperation(ref op) => actual.process_client(outputs, src, &op),

        &ReplCommand::Forward(
                ReplicationMessage { epoch, msg: PeerMsg::Prepare(seq, ref data) , .. }) => {
                actual.process_operation(outputs, src, seq, epoch, &data);
            }
        &ReplCommand::Forward(ReplicationMessage { msg: PeerMsg::CommitTo(seq) , .. }) => {
            actual.commit_observed(seq);
        }
        &ReplCommand::Forward(
                ReplicationMessage { epoch, ts, msg: PeerMsg::HelloDownstream , .. }) => {
                actual.hello_downstream(outputs, src, ts, epoch);
            }
        &ReplCommand::Response(ref reply) => actual.process_downstream_response(outputs, reply),
        &ReplCommand::ConsumeFrom(seq) => actual.consume_requested(outputs, src, seq),
        &ReplCommand::ViewChange(ref config) => actual.reconfigure(config),
        other => panic!("Unimplemented apply_cmd: {:?}", other),
    };
    actual.process_replication(outputs);
}

#[test]
fn simulate_three_node_system_vec() {
    env_logger::init().unwrap_or(());
    let mut sim = NetworkSim::<VecLog>::new(3);
    sim.run_for(10, "simulate_three_node_system_vec", |t, sim, state| {
        if t == 0 {
            sim.client_operation(state, b"hello_world".to_vec().into());
        }
    });
    sim.validate_client_responses(1);
    sim.validate_logs();
}

#[test]
#[ignore]
fn simulate_three_node_system_with_failure_archons () {
    env_logger::init().unwrap_or(());
    let mut stack = vec![BTreeSet::new()];
    let mut seen = BTreeSet::new();
    while let Some(failure_set) = stack.pop() {
        seen.insert(failure_set.clone());
        let mut h = SipHasher::new();
        failure_set.hash(&mut h);

        let mut sim = NetworkSim::<VecLog>::new(3).set_failures(failure_set);
        let name = format!("simulate_three_node_system_with_failure_archons-{:08x}", h.finish());
        info!("Test run named: {:?}", name);
        let new_failures = sim.run_for(10, &name, |t, sim, state| {
            if t == 0 {
                sim.client_operation(state, b"hello_world".to_vec().into());
            }
        });
        info!("New potential failures set: {:?}", new_failures);
        stack.extend(new_failures.into_iter().filter(|f| !seen.contains(f)));
        info!("Remaining failures: {:?}", stack);

        sim.validate_client_responses(1);
        sim.validate_logs();
    }
}


#[test]
fn simulate_three_node_system_streaming_vec() {
    env_logger::init().unwrap_or(());
    let mut sim = NetworkSim::<VecLog>::new(3);
    sim.run_for(15,
                "simulate_three_node_system_streaming_vec",
                |t, sim, state| {
                    if t < 3 {
                        sim.client_operation(state,
                                             format!("hello_world at {}", t).into_bytes().into())
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
                        let m = Buf::from(format!("hello_world at {}", t).into_bytes());
                        produced_messages.push(m.clone());
                        sim.client_operation(state, m);
                    }
                });
    sim.validate_logs();
    sim.validate_client_responses(produced_messages.len());

    debug!("consumed: {:?}", sim.consumed_messages());
    assert_eq!(sim.consumed_messages().into_iter().map(|(_, v)| v).collect::<Vec<_>>(),
               produced_messages)
}

#[test]
fn simulate_tail_crash() {
    env_logger::init().unwrap_or(());

    let mut sim = NetworkSim::<VecLog>::new(3);
    let mut produced_messages = Vec::new();
    sim.run_for(100, "simulate_tail_crash", |t, sim, state| {
        if t == 5 {
            let tail = sim.tail_node();
            sim.crash_node(state, tail)
        }

        if t < 3 {
            let m = Buf::from(format!("hello_world at {}", t).into_bytes());
            produced_messages.push(m.clone());
            sim.client_operation(state, m);
        }
    });
    sim.validate_logs();
    sim.validate_client_responses(3);
}

struct OutBufs<'a> {
    node_id: NodeId,
    ports: &'a mut NetworkState,
}

impl<'a> OutBufs<'a> {
    fn enqueue(&mut self, dest: NodeId, cmd: ReplCommand) {
        let &mut OutBufs { node_id, .. } = self;
        self.ports.enqueue(node_id, dest, cmd);
    }
}

impl<'a> Outputs for OutBufs<'a> {
    type Dest = NodeId;
    fn respond_to(&mut self, dest: NodeId, resp: &OpResp) {
        let cmd = ReplCommand::Response(resp.clone());
        self.enqueue(dest, cmd);
    }

    fn forward_downstream(&mut self,
                          downstream_id: &NodeId,
                          now: Timestamp<WallT>,
                          epoch: Epoch,
                          msg: PeerMsg) {
        self.enqueue(*downstream_id,
                     ReplCommand::Forward(ReplicationMessage {
                         epoch: epoch,
                         ts: now,
                         msg: msg,
                     }));
    }

    fn consumer_message(&mut self, dest: &NodeId, seqno: Seqno, msg: Buf) {
        self.enqueue(*dest, ReplCommand::ConsumerMsg(seqno, msg))
    }
}

struct NetworkSim<L> {
    nodes: BTreeMap<NodeId, ReplModel<L, NodeId>>,
    node_count: usize,
    epoch: Epoch,
    client_id: NodeId,
    client_buf: VecDeque<ReplCommand>,
    drops: BTreeSet<CausalVar>,
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

    fn dequeue_one(&mut self) -> Option<(NodeId, NodeId, ReplCommand, Timestamp<u64>, Timestamp<u64>)> {
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
            Some((src, dest, it, sent, recv))
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

#[derive(Debug)]
struct Tracer {
    entries: Vec<TraceEvent<ReplCommand>>,
}

impl<M> TraceEvent<M> {
    fn as_causal_var(&self) -> Option<CausalVar> {
        match self {
            &TraceEvent::MessageRecv(MessageRecv { sent, recv, src, dst, .. }) => {
                Some(CausalVar::Message(sent.time, src, dst))
            }
            &TraceEvent::Committed(Committed { process, offset, .. }) => {
                Some(CausalVar::Commit(offset, process))
            }
            _ => None,
        }
    }
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
            data: ReplCommand) {
        let m = MessageRecv {
            sent: sent,
            recv: recv,
            src: *src,
            dst: *dst,
            data: data,
        };
        let ev = TraceEvent::MessageRecv(m);
        debug!("Observe at {:?}: {:?}", ev.as_causal_var(), ev);
        self.entries.push(ev);
    }

    fn node_crashed(&mut self, t: Timestamp<u64>, process: &NodeId) {
        let m = NodeCrashed {
            time: t,
            process: *process,
        };
        self.entries.push(TraceEvent::NodeCrashed(m));
    }
    fn committed(&mut self, t: Timestamp<u64>, process: &NodeId, offset: Seqno) {
        let m = Committed {
            time: t,
            process: *process,
            offset: offset,
        };
        debug!("Note committed: {:?}", m);
        self.entries.push(TraceEvent::Committed(m));
    }

    fn persist_to(&self, path: &Path) {
        let mut f = File::create(path).expect("create file");
        serde_json::to_writer(&mut f, &self.entries).expect("write json");
    }

    fn messages(&self) -> Vec<&MessageRecv<ReplCommand>> {
        self.entries
            .iter()
            .filter_map(|e| {
                match e {
                    &TraceEvent::MessageRecv(ref e) => Some(e),
                    _ => None,
                }
            })
            .collect()
    }
}

impl<L: TestLog> NetworkSim<L> {
    fn new(node_count: usize) -> Self {
        let epoch = Epoch::from(0);

        let nodes = (0..node_count)
                        .map(|id| (NodeId::Replica(id), ReplModel::new(L::new())))
                        .collect();

        NetworkSim {
            nodes: nodes,
            node_count: node_count,
            epoch: epoch,
            client_id: NodeId::Client,
            client_buf: VecDeque::new(),
            drops: BTreeSet::new(),
        }
    }
    fn set_failures(mut self, failures: BTreeSet<CausalVar>) -> Self {
        debug!("Dropping messages on: {:?}", failures);
        self.drops = failures;
        self
    }

    fn configs(&self) -> BTreeMap<NodeId, ConfigurationView<NodeId>> {
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
                                                              mut f: F) -> BTreeSet<BTreeSet<CausalVar>> {
        let mut epoch = None;
        let path = PathBuf::from(format!("target/run-trace-{}.jsons", test_name));
        let tracer = Tracer::new();

        let mut state = NetworkState {
            clock: Clock::manual(0),
            tracer: tracer,
            input_bufs: HashMap::new(),
            output_bufs: HashMap::new(),
        };

        for t in 0..end_of_time {
            state.clock.set_time(t as u64);
            if epoch != Some(self.epoch) {
                epoch = Some(self.epoch);
                let configs = self.configs();
                for (id, config) in configs {
                    debug!("configure epoch: {:?}; node: {:?}", self.epoch, config);
                    state.enqueue(NodeId::Oracle, id, ReplCommand::ViewChange(config));
                }
            }

            let now = state.clock.now();
            for (id, n) in self.nodes.iter() {
                state.tracer.state(now, id, format!("{:?}", n));
            }
            let now = state.clock.now();
            for (id, model) in self.nodes.iter() {
                if let Some(offset) = model.borrow_log()
                                           .read_committed()
                                           .expect("read_committed") {
                    state.tracer.committed(now, id, offset);
                }
            }

            debug!("time: {:?}/{:?}", t, end_of_time);
            f(t, self, &mut state);
            if state.is_quiescent() {
                break;
            }

            self.step(&mut state, &path);
            debug!("network state {:?}; quiet:{:?}; : {:?}",
                   t,
                   state.is_quiescent(),
                   state);

        }

        state.tracer.persist_to(&path);
        debug!("messages: {:#?}", state.tracer.messages());
        assert!(state.is_quiescent());

        let tracer = state.tracer;
        let deps = Self::infer_causality(&*tracer.entries);

        Self::derive_formula(self.nodes.keys().cloned(), deps)
    }

    fn derive_formula<I: Iterator<Item = NodeId>>(nodes: I, deps: Graph<CausalVar, usize>) -> BTreeSet<BTreeSet<CausalVar>> {
        // Commit invariant
        // Forall(N)Exists(X, Y) Predecessor(X, Y) = (C(N, X) => C(N, Y))
        let nodes = nodes.collect::<BTreeSet<NodeId>>();
        let mut committed_offsets = BTreeMap::new();
        let commit_events = deps.node_indices()
                                .filter_map(|ni| {
                                    match &deps[ni] {
                                        &CausalVar::Commit(seq, node) => Some((ni, seq, node)),
                                        _ => None,
                                    }
                                })
                                .collect::<BTreeSet<_>>();


        for &(_, offset, process) in commit_events.iter() {
            let ent = committed_offsets.entry(process).or_insert(offset);
            *ent = cmp::max(*ent, offset);
        }

        debug!("Commit goals: {:?}", committed_offsets);
        let max = committed_offsets.values().cloned().max().unwrap_or(Seqno::zero());
        debug!("max: {:?}", max);

        // Show that each seqno will be committed at node N if it is commited at N-1
        let commit_goal = iterate(Seqno::zero(), Seqno::succ)
                              .take_while(|&s| s <= max)
                              .flat_map(|seq| {
                                  nodes.iter().zip(nodes.iter().skip(1)).map(move |(&x, &y)| {
                                      Bools::var(CausalVar::Commit(seq, x))
                                          .implies(Bools::var(CausalVar::Commit(seq, y)))
                                  })
                              })
                              .fold(Bools::var(CausalVar::True), |l, r| l & r);

        let mut commit_env = BTreeMap::new();
        commit_env.insert(CausalVar::True, true);
        commit_env.insert(CausalVar::False, false);

        // Extend the environment with facts about which (node, seqno) have been committed.
        commit_env.extend(committed_offsets.into_iter().flat_map(|(p, off)| {
            iterate(Seqno::zero(), Seqno::succ)
                .take_while(|&s| s <= max)
                .map(move |s| (CausalVar::Commit(s, p), s <= off))
        }));

        info!("Goals: {:?}", commit_goal);
        info!("env: {:?}", commit_env);
        assert!(commit_goal.eval(&commit_env).expect("eval"));

        let all_goals = commit_events.iter().map(|&(ci, _seq, _node)| {
                // Now, for each observed commit, traverse it's supporting messages from the edge
            let reachable = DfsIter::new(&deps, ci).map(|ni| {
                deps[ni].clone()
            }).collect::<BTreeSet<CausalVar>>();
            info!("Reachable from {:?}: {:?}", deps[ci], reachable);
            // So, we want to say that each commit goal is equal to the success of each message that supports it.
            let commit_goal = Bools::var(deps[ci].clone());
            let message_supports = reachable.into_iter().map(Bools::var).fold(Bools::var(CausalVar::True), |l, r| l & r);
            let goal = commit_goal.is(message_supports);
            debug!("Combined goal {:?}: {:?}", (_seq, _node), goal);
            goal
        }).collect::<BTreeSet<_>>();

        for it in all_goals.iter() {
            println!("Goal: {}", it);
        }
        // We want to find a pattern of message drops (false M(...)          .
        // variables) that results in falsifying one or more of our commit   .
        // goals. To express this in circuit satisfiability, we assert:
        // (G0 == F) | ... | (GN == F).

        let falsified_goal = all_goals.into_iter()
                                      .map(|g| g.is(Bools::var(CausalVar::False)))
                                      .fold(Bools::var(CausalVar::False), |l, r| l | r);

        let mut cnf = falsified_goal.to_cnf(&commit_env);

        let mut results = BTreeSet::new();
        // Our "solution" is the set of message vars assigned with `false`.
        // Create a partial ordering such that a <= b iff forall(x in a) x in b
        // So, we want to find the least set of solutions.
        for result in cnf {
            let falsies = result.into_iter()
                                .filter(|&(ref k, _)| {
                                    if let &CausalVar::Message(_, _, _) = k {
                                        true
                                    } else {
                                        false
                                    }
                                })
                                .filter(|&(_, v)| !v)
                                .map(|(k, _)| k)
                                .collect::<BTreeSet<_>>();
            trace!("potential failures: {:?}", falsies);
            for s in results.iter().filter(|s| falsies.is_subset(s)).cloned().collect::<Vec<_>>() {
                results.remove(&s);
            }
            let has_subset = results.iter().any(|s| falsies.is_superset(s));
            if !has_subset {
                results.insert(falsies);
            }
        }
        for res in results.iter() {
            debug!("lower bound result: {:?}", res);
        }
        results
    }

    fn infer_causality<'a>(messages: &'a [TraceEvent<ReplCommand>]) -> Graph<CausalVar, usize> {
        let mut g = Graph::new();
        let mut nodes = HashMap::new();

        for (x, y) in messages.iter()
                              .enumerate()
                              .flat_map(|(yi, y)| messages.iter().map(move |x| (x, y))) {
            let (xcv, ycv) = match (x.as_causal_var(), y.as_causal_var()) {
                (Some(a), Some(b)) => (a, b),
                _ => continue,
            };

            let causedp = match (x, y) {
                (&TraceEvent::MessageRecv(ref x),
                 &TraceEvent::MessageRecv(ref y))
                    if x.dst == y.src => Self::peer_message_causal(&x.data, &y.data),
                (&TraceEvent::MessageRecv(ref x),
                 &TraceEvent::MessageRecv(ref y))
                    if x.src == y.src => Self::self_message_causal(&x.data, &y.data),
                (&TraceEvent::MessageRecv(ref x),
                 &TraceEvent::Committed(ref c))
                    if x.dst == c.process => Self::recv_message_commit_causalp(&x.data, &c.offset),
                (&TraceEvent::MessageRecv(ref x),
                 &TraceEvent::Committed(ref c))
                    if x.src == c.process => Self::send_message_commit_causalp(&x.data, &c.offset),
                (&TraceEvent::Committed(ref c),
                 &TraceEvent::MessageRecv(ref y))
                    if y.src == c.process => Self::send_commit_message_causalp(&c.offset, &y.data),
                _ => false,
            };
            trace!("causedp: {:?}; Compare: {:?} / {:?}", causedp, x, y);

            let xn = *nodes.entry(xcv.clone()).or_insert_with(|| g.add_node(xcv));
            let yn = *nodes.entry(ycv.clone()).or_insert_with(|| g.add_node(ycv));
            if causedp {
                trace!("Add edge: {:?} <- {:?}", g[xn], g[yn]);
                // it's a "caused by relation"
                g.update_edge(yn, xn, 1usize);
            }
        }
        let mut of = File::create("target/causality.dot").expect("file create");

        writeln!(&mut of, "{}", ::petgraph::dot::Dot::new(&g)).expect("writeln!");
        g
    }
    fn recv_message_commit_causalp(x: &ReplCommand, seq: &Seqno) -> bool {
        match x {
            &ReplCommand::Forward(ReplicationMessage { msg: PeerMsg::Prepare(ref s0, _), .. })
                if s0 <= seq => true,
            &ReplCommand::Forward(ReplicationMessage { msg: PeerMsg::CommitTo(ref s0), .. })
                if s0 <= seq => false,
            &ReplCommand::Forward(ref other) => {
                debug!("recv_message_commit_causalp: Unhandled: {:?} / {:?}",
                       x,
                       seq);
                false
            }
            other => {
                trace!("recv_message_commit_causalp: Unhandled: {:?} / {:?}",
                       x,
                       seq);
                false
            }
        }
    }

    fn send_message_commit_causalp(x: &ReplCommand, seq: &Seqno) -> bool {
        match x {
            &ReplCommand::Forward(ReplicationMessage { msg: PeerMsg::Prepare(ref s0, _), .. })
                if s0 <= seq => true,
            &ReplCommand::Forward(ref other) => {
                debug!("recv_message_commit_causalp: Unhandled: {:?} / {:?}",
                       x,
                       seq);
                false
            }
            other => {
                trace!("recv_message_commit_causalp: Unhandled: {:?} / {:?}",
                       x,
                       seq);
                false
            }
        }
    }
    fn send_commit_message_causalp(seq: &Seqno, y: &ReplCommand) -> bool {
        match y {
            &ReplCommand::Forward(ReplicationMessage { msg: PeerMsg::CommitTo(ref s0), .. })
                if seq <= s0 => false,
            &ReplCommand::Forward(ref other) => {
                debug!("message_commit_causalp: Unhandled: {:?} / {:?}", seq, y);
                false
            }
            other => {
                trace!("send_commit_message_causalp: Unhandled: {:?} / {:?}",
                       seq,
                       y);
                false
            }
        }
    }

    fn peer_message_causal(x: &ReplCommand, y: &ReplCommand) -> bool {
        match (x, y) {
            (&ReplCommand::ClientOperation(ref data0),
             &ReplCommand::Forward(ReplicationMessage { msg: PeerMsg::Prepare(_, ref data1), .. })) => {
                data0 == data1
            }
            (&ReplCommand::Forward(ReplicationMessage { msg: PeerMsg::Prepare(s0, _), .. }),
             &ReplCommand::Forward(ReplicationMessage { msg: PeerMsg::Prepare(s1, _), .. })) |
            (&ReplCommand::Forward(ReplicationMessage { msg: PeerMsg::Prepare(s0, _), .. }),
             &ReplCommand::Response(OpResp::Ok(_, s1, _))) |
            (&ReplCommand::Forward(ReplicationMessage { msg: PeerMsg::Prepare(s0, _), .. }),
             &ReplCommand::Response(OpResp::Err(_, s1, _))) |
            (&ReplCommand::Response(OpResp::Err(_, s0, _)),
             &ReplCommand::Response(OpResp::Err(_, s1, _))) |
            (&ReplCommand::Response(OpResp::Ok(_, s0, _)),
             &ReplCommand::Response(OpResp::Ok(_, s1, _))) => s0 == s1,
            (&ReplCommand::Forward(ReplicationMessage { msg: PeerMsg::HelloDownstream, epoch: _, .. }),
                 &ReplCommand::Response(OpResp::HelloIHave(_, _)))
                => true,
            (&ReplCommand::Response(OpResp::HelloIHave(_, s0)),
             &ReplCommand::Forward(ReplicationMessage { msg: PeerMsg::Prepare(s1, _), .. })) => {
                s0 <= Some(s1)
            }
            (&ReplCommand::Forward(ReplicationMessage { msg: PeerMsg::CommitTo(s0), .. }),
             &ReplCommand::Forward(ReplicationMessage { msg: PeerMsg::CommitTo(s1), .. })) => {
                s0 >= s1
            }
            (_, _) => false,
        }
    }

    fn self_message_causal(x: &ReplCommand, y: &ReplCommand) -> bool {
        match (x, y) {
            (&ReplCommand::Forward(ReplicationMessage { msg: PeerMsg::Prepare(s0, _), .. }),
             &ReplCommand::Forward(ReplicationMessage { msg: PeerMsg::CommitTo(s1), .. })) => {
                s0 <= s1
            }
            (&ReplCommand::Forward(ReplicationMessage { msg: PeerMsg::Prepare(s0, _), .. }),
             &ReplCommand::Forward(ReplicationMessage { msg: PeerMsg::Prepare(s1, _), .. })) => {
                s0.succ() == s1
            }
            (_, _) => false,
        }
    }

    fn tail_node(&self) -> NodeId {
        NodeId::Replica(self.node_count - 1)
    }

    fn head_node(&self) -> NodeId {
        NodeId::Replica(0)
    }

    fn crash_node(&mut self, state: &mut NetworkState, node_id: NodeId) {
        state.crash_node(node_id);
        if let Some(old) = self.nodes.remove(&node_id) {
            info!("Crashing node: {:?}: {:?}", node_id, old);
            self.epoch = self.epoch.succ();
        }
    }

    fn client_operation(&self, state: &mut NetworkState, val: Buf) {
        let head = self.head_node();
        let cmd = ReplCommand::ClientOperation(val);
        state.enqueue(self.client_id, head, cmd);
    }

    fn consume_from(&self, state: &mut NetworkState, seqno: Seqno) {
        let tail = self.tail_node();
        let cmd = ReplCommand::ConsumeFrom(seqno);

        state.enqueue(self.client_id, tail, cmd);
    }

    fn unexpected_responses(&self) -> Vec<&ReplCommand> {
        self.client_buf
            .iter()
            .filter_map(|m| {
                match m {
                    &ReplCommand::Response(_) |
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
                    &ReplCommand::Response(ref m) => Some(m),
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
                                   let committed_seq = n.borrow_log()
                                                        .read_committed()
                                                        .expect("read_committed");
                                   debug!("Committed @{:?}: {:?}", id, committed_seq);
                                   let committed = n.borrow_log()
                                                    .read_from(Seqno::zero())
                                                    .expect("read_from")
                                                    .map(|it| it.expect("read from item"))
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

    fn step(&mut self, state: &mut NetworkState, path: &Path) {
        while let Some((src, dest, input, sent_t, recv_t)) = state.dequeue_one() {
            debug!("Inputs: {:?} -> {:?}: {:?}", src, dest, input);
            if dest == self.client_id {
                self.client_buf.push_back(input);
            } else {
                let cv = CausalVar::Message(sent_t.time, src, dest);
                debug!("dropping? {:?} -> {:?}", cv, self.drops.contains(&cv));
                if self.drops.contains(&cv) {
                    warn!("Injecting failure for nodes: {:?} -> {:?} at {:?}", src, dest, recv_t);
                    // We also need to indicate that the "connection" was dropped. Because failure detectors.

                } else if let Some(ref mut n) = self.nodes.get_mut(&dest) {
                    debug!("Feed node {:?} with {:?} at {:?}", dest, input, recv_t);
                    state.tracer.recv(sent_t, recv_t, &src, &dest, input.clone());
                    state.tracer.persist_to(&path);
                    let mut output = OutBufs {
                        node_id: dest,
                        ports: state,
                    };

                    apply_cmd(n, &input, src, &mut output);

                } else {
                    warn!("Dropping message for dead node: {:?}: {:?}", dest, input);
                }
            }
        }

        for (nid, n) in self.nodes.iter_mut() {
            n.borrow_log().quiesce();

            let mut output = OutBufs {
                node_id: *nid,
                ports: state,
            };
            while n.process_replication(&mut output) {
                debug!("iterated replication for node {:?}", nid);
            }
        }

        state.flip();
    }
}
