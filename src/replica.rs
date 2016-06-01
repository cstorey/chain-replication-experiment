use std::cmp;
use std::fmt;
use std::thread;
use std::collections::HashMap;
use std::sync::mpsc::{Sender, Receiver, channel};
use mio;

use data::{OpResp, PeerMsg, Seqno, NodeViewConfig, Buf};
use config::{ConfigurationView, Epoch};
use consumer::Consumer;
use {Notifier};
use hybrid_clocks::{Clock,Wall, Timestamp, WallT};

#[derive(Debug)]
struct Forwarder {
    last_prepared_downstream: Option<Seqno>,
    last_committed_downstream: Option<Seqno>,
    last_acked_downstream: Option<Seqno>,
    pending_operations: HashMap<Seqno, mio::Token>,
}

#[derive(Debug)]
struct Terminus {
    consumers: HashMap<mio::Token, Consumer>,
}

#[derive(Debug)]
enum ReplRole {
    Forwarder(Forwarder),
    Terminus(Terminus),
}

pub trait Log: fmt::Debug {
    type Cursor : Iterator<Item=(Seqno, Vec<u8>)>;
    fn seqno(&self) -> Seqno;
    fn read_prepared(&self) -> Option<Seqno>;
    fn read_committed(&self) -> Option<Seqno>;
    fn read_from<'a>(&'a self, Seqno) -> Self::Cursor;
    fn prepare(&mut self, Seqno, &[u8]);
    fn commit_to(&mut self, Seqno) -> bool;
}

pub trait Outputs {
    fn respond_to(&mut self, mio::Token, &OpResp);
    fn forward_downstream(&mut self, Timestamp<WallT>, Epoch, PeerMsg);
    fn consumer_message(&mut self, mio::Token, Seqno, Buf);
}

type ReplOp<L> = Box<FnMut(&mut ReplModel<L>, &mut Notifier) + Send>;
#[derive(Debug)]
pub struct ReplModel<L> {
    next: ReplRole,
    log: L,
    clock: Clock<Wall>,
    current_epoch: Epoch,
    upstream_committed: Option<Seqno>,
    commit_requested: Option<Seqno>,
    is_head: bool,
}

impl ReplRole {
    fn process_operation<O: Outputs>(&mut self,
                         output: &mut O,
                         token: mio::Token,
                         epoch: Epoch,
                         seqno: Seqno,
                         op: &[u8]) {
        trace!("role: process_operation: {:?}/{:?}", epoch, seqno);
        match self {
            &mut ReplRole::Forwarder(ref mut f) => f.process_operation(output, token, epoch, seqno, op),
            &mut ReplRole::Terminus(ref mut t) => t.process_operation(output, token, epoch, seqno, op),
        }
    }
    fn process_downstream_response<O: Outputs>(&mut self, out: &mut O, reply: &OpResp) {
        trace!("ReplRole: process_downstream_response: {:?}", reply);
        match self {
            &mut ReplRole::Forwarder(ref mut f) => f.process_downstream_response(out, reply),
            _ => (),
        }
    }

    fn process_replication<L: Log, O: Outputs>(&mut self, clock: &mut Clock<Wall>, epoch: Epoch, log: &L, out: &mut O) -> bool {
        trace!("ReplRole: process_replication");
        match self {
            &mut ReplRole::Forwarder(ref mut f) => f.process_replication(clock, epoch, log, out),
            &mut ReplRole::Terminus(ref mut t) => t.process_replication(clock, epoch, log, out),
        }
    }

    pub fn consume_requested<O: Outputs>(&mut self, out: &mut O, token: mio::Token, epoch: Epoch, mark: Seqno) {
        match self {
            &mut ReplRole::Terminus(ref mut t) => t.consume_requested(out, token, epoch, mark),
            other => {
                error!("consume_requested of {:?}", other);
                out.respond_to(token, &OpResp::Err(epoch, mark, "cannot consume from non-terminus".to_string()))
            },
        }
    }
    fn reset(&mut self) {
        match self {
            &mut ReplRole::Forwarder(ref mut f) => f.reset(),
            _ => (),
        }
    }
}

impl Forwarder {
    fn new() -> Forwarder {
        Forwarder {
            last_prepared_downstream: None,
            last_acked_downstream: None,
            last_committed_downstream: None,
            pending_operations: HashMap::new(),
        }
    }

    fn process_operation<O: Outputs>(&mut self,
                         _output: &mut O,
                         token: mio::Token,
                         _epoch: Epoch,
                         seqno: Seqno,
                         _op: &[u8]) {
        trace!("Forwarder: process_operation: {:?}/{:?}", _epoch, seqno);
        self.pending_operations.insert(seqno, token);
    }

    fn process_downstream_response<O: Outputs>(&mut self, out: &mut O, reply: &OpResp) {
        trace!("Forwarder: {:?}", self);
        trace!("Forwarder: process_downstream_response: {:?}", reply);
        match reply {
            &OpResp::Ok(_epoch, seqno, _) | &OpResp::Err(_epoch, seqno, _) => {
                self.last_acked_downstream = Some(seqno);
                if let Some(token) = self.pending_operations.remove(&seqno) {
                    trace!("Found in-flight op {:?} for client token {:?}",
                           seqno,
                           token);
                    out.respond_to(token, reply);
                } else {
                    warn!("Unexpected response for seqno: {:?}", seqno);
                }
            }
            &OpResp::HelloIWant(ts, last_prepared_downstream) => {
                info!("{}; Downstream has {:?}", ts, last_prepared_downstream);
                // assert!(last_prepared_downstream <= self.seqno());
                self.last_acked_downstream = Some(last_prepared_downstream);
                self.last_prepared_downstream = Some(last_prepared_downstream);
            }
        }
    }

    fn process_replication<L: Log, O: Outputs>(&mut self,
                                                          clock: &mut Clock<Wall>,
                                                          epoch: Epoch,
                                                          log: &L,
                                                          out: &mut O)
                                                          -> bool {
        debug!("pre  Repl: Ours: {:?}; downstream prepared: {:?}; committed: {:?}; acked: {:?}",
               log.seqno(),
               self.last_prepared_downstream,
               self.last_committed_downstream,
               self.last_acked_downstream);

        let mut changed = false;
        let send_next = self.last_prepared_downstream.map(|s| s.succ()).unwrap_or_else(Seqno::zero);
        let max_to_prepare_now = log.seqno();
        debug!("Window prepare {:?} - {:?}; prepared locally: {:?}",
               send_next,
               max_to_prepare_now,
               log.read_prepared());

        if send_next <= max_to_prepare_now {
            for (i, op) in log.read_from(send_next).take_while(|&(i, _)| i <= max_to_prepare_now) {
                    debug!("Queueing seq:{:?}/{:?}; ds/seqno: {:?}",
                           i,
                           op,
                           self.last_prepared_downstream);
                    out.forward_downstream(clock.now(), epoch, PeerMsg::Prepare(i, op.into()));
                    self.last_prepared_downstream = Some(i);
                    changed = true
                }
            }
        debug!("post Repl: Ours: {:?}; downstream sent: {:?}; acked: {:?}",
               log.seqno(),
               self.last_prepared_downstream,
               self.last_acked_downstream);

        if let (Some(ds_prepared), ds_committed, Some(our_committed)) =
               (self.last_prepared_downstream,
                self.last_committed_downstream,
                log.read_committed()) {
            let max_to_commit_now = cmp::min(ds_prepared, our_committed);
            if ds_committed.map(|c| c < max_to_commit_now).unwrap_or(true) {
                debug!("Window commit {:?} - {:?}; committed locally: {:?}",
                       ds_committed,
                       max_to_commit_now,
                       our_committed);

                out.forward_downstream(clock.now(), epoch, PeerMsg::CommitTo(max_to_commit_now));
                self.last_committed_downstream = Some(max_to_commit_now);
                changed = true
            }
        }
        changed
    }

    fn reset(&mut self) {
        self.last_prepared_downstream = None
    }
}

impl Terminus {
    fn new() -> Terminus {
        Terminus { consumers: HashMap::new() }
    }

    fn process_operation<O: Outputs>(&mut self,
                         output: &mut O,
                         token: mio::Token,
                         epoch: Epoch,
                         seqno: Seqno,
                         op: &[u8])
                         {
        trace!("Terminus! {:?}/{:?}", seqno, op);
        output.respond_to(token, &OpResp::Ok(epoch, seqno, None))
    }
    pub fn consume_requested<O: Outputs>(&mut self, _out: &mut O, token: mio::Token, epoch: Epoch, mark: Seqno) {
        debug!("Terminus: consume_requested: {:?} from {:?}@{:?}", token, mark, epoch);
        let consumer = self.consumers.entry(token).or_insert_with(|| Consumer::new());
        consumer.consume_requested(mark).expect("consume");
    }

    fn process_replication<L: Log, O: Outputs>(&mut self,
            _clock: &mut Clock<Wall>, _epoch: Epoch, log: &L, out: &mut O) -> bool {
        let mut changed = false;
        for (&token, cons) in self.consumers.iter_mut() {
            changed |= cons.process(out, token, log)
        }
        changed
    }

}

impl<L: Log> ReplModel<L> {
    pub fn new(log: L) -> ReplModel<L> {
        ReplModel {
            log: log,
            current_epoch: Default::default(),
            clock: Clock::wall(),
            next: ReplRole::Terminus(Terminus::new()),
            upstream_committed: None,
            commit_requested: None,
            is_head: false,
        }
    }

    #[cfg(test)]
    fn borrow_log(&self) -> &L {
        &self.log
    }

    pub fn seqno(&self) -> Seqno {
        self.log.seqno()
    }

    fn next_seqno(&self) -> Seqno {
        self.log.seqno()
    }

    fn run_from(&mut self, rx: Receiver<ReplOp<L>>, mut tx: Notifier) {
        loop {
            let mut cmd = rx.recv().expect("recv model command");
            cmd(self, &mut tx);

            while self.process_replication(&mut tx) {/* Nothing */}
        }
    }

    // If we can avoid coupling the ingest clock to the replicator; we can
    // factor this out into the client proxy handler.
    pub fn process_client<O: Outputs>(&mut self, output: &mut O, token: mio::Token, op: &[u8]) {
        let seqno = self.next_seqno();
        let epoch = self.current_epoch;
        assert!(self.is_head);
        self.process_operation(output, token, seqno, epoch, op)
    }

    pub fn process_operation<O: Outputs>(&mut self,
                             output: &mut O,
                             token: mio::Token,
                             seqno: Seqno,
                             epoch: Epoch,
                             op: &[u8]) {
        debug!("process_operation: {:?}/{:?}", epoch, seqno);

        if epoch != self.current_epoch {
            warn!("Operation epoch ({:?}) differers from our last observed configuration: ({:?})",
                  epoch,
                  self.current_epoch);
            let resp = OpResp::Err(epoch,
                                   seqno,
                                   format!("BadEpoch: {:?}; last seen config: {:?}",
                                           epoch,
                                           self.current_epoch));
            return output.respond_to(token, &resp);
        }

        self.log.prepare(seqno, &op);

        self.next.process_operation(output, token, epoch, seqno, &op)
    }

    pub fn process_downstream_response<O: Outputs>(&mut self, out: &mut O, reply: &OpResp) {
        trace!("ReplRole: process_downstream_response: {:?}", reply);
        self.next.process_downstream_response(out, reply)
    }

    pub fn process_replication<O: Outputs>(&mut self, out: &mut O) -> bool {
        trace!("process_replication: {:?}", self);
        let mut res = self.next.process_replication(&mut self.clock, self.current_epoch, &self.log, out);
        res |= self.flush();
        res
    }

    pub fn reset(&mut self) {
        self.next.reset()
    }

    pub fn commit_observed(&mut self, seqno: Seqno) {
        debug!("Observed upstream commit point: {:?}; current: {:?}",
               seqno,
               self.upstream_committed);
        assert!(self.upstream_committed.map(|committed| committed <= seqno).unwrap_or(true));
        self.upstream_committed = Some(seqno);
    }

    fn flush(&mut self) -> bool {
        if self.is_head {
            self.upstream_committed = self.log.read_prepared();
            trace!("Auto committing to: {:?}", self.upstream_committed);
        } else {
            trace!("Flush to upstream commit point: {:?}",
                   self.upstream_committed);
        }

        match self.upstream_committed {
            Some(seqno) if self.commit_requested < self.upstream_committed => {
                trace!("Commit for {:?}", seqno);
                self.commit_requested = self.upstream_committed;
                self.log.commit_to(seqno)
            },
            _ => false
        }
    }

    pub fn hello_downstream<O: Outputs>(&mut self, out: &mut O, token: mio::Token, at: Timestamp<WallT>, epoch: Epoch) {
        debug!("{}; hello_downstream: {:?}; {:?}", at, token, epoch);
        let msg = OpResp::HelloIWant(at, self.log.seqno());
        info!("Inform upstream about our current version, {:?}!", msg);
        out.respond_to(token, &msg)
    }

    pub fn consume_requested<O: Outputs>(&mut self, out: &mut O, token: mio::Token, mark: Seqno) {
        self.next.consume_requested(out, token, self.current_epoch, mark)
    }

    fn configure_forwarding(&mut self, is_forwarder: bool) {
        // XXX: Replays?
        match (is_forwarder, &mut self.next) {
            (true, role @ &mut ReplRole::Terminus(_)) => {
                info!("Switched to forwarding from {:?}", role);
                *role = ReplRole::Forwarder(Forwarder::new());
            }

            (false, role @ &mut ReplRole::Forwarder(_)) => {
                info!("Switched to terminating from {:?}", role);
                *role = ReplRole::Terminus(Terminus::new());
            }
            _ => {
                info!("No change of config");
            }
        }
        debug!("Next: {:?}", self.next);
    }

    fn set_is_head(&mut self, is_head: bool) {
        self.is_head = is_head;
    }

    fn set_epoch(&mut self, epoch: Epoch) {
        self.current_epoch = epoch;
    }

    pub fn reconfigure<T: fmt::Debug + Clone>(&mut self, view: &ConfigurationView<T>) {
        info!("Reconfiguring from: {:?}", view);
        self.set_epoch(view.epoch);
        self.configure_forwarding(view.should_connect_downstream().is_some());
        self.set_is_head(view.is_head());
        info!("Reconfigured from: {:?}", view);
    }

}

pub struct ReplProxy<L: Log + Send + 'static> {
    _inner_thread: thread::JoinHandle<()>,
    tx: Sender<ReplOp<L>>,
}

impl<L: Log + Send + 'static> ReplProxy<L> {
    pub fn build(mut inner: ReplModel<L>, notifications: Notifier) -> Self {
        let (tx, rx) = channel();
        let inner_thread = thread::Builder::new().name("replmodel".to_string())
            .spawn(move || inner.run_from(rx, notifications)).expect("spawn model");
        ReplProxy {
            _inner_thread: inner_thread,
            tx: tx,
        }
    }

    fn invoke<F: FnMut(&mut ReplModel<L>, &mut Notifier) + Send + 'static>(&mut self, f: F) {
        self.tx.send(Box::new(f)).expect("send to inner thread")
    }

    pub fn process_operation (&mut self, token: mio::Token, epoch: Epoch, seqno: Seqno, op: Buf) {
       self.invoke(move |m, tx| m.process_operation(tx, token, seqno, epoch, &op))
    }
    pub fn client_operation (&mut self, token: mio::Token, op: Buf){
       self.invoke(move |m, tx| m.process_client(tx, token, &op))
    }
    pub fn commit_observed (&mut self, _epoch: Epoch, seq: Seqno){
       self.invoke(move |m, _| m.commit_observed(seq))
    }
    pub fn response_observed(&mut self, resp: OpResp){
        self.invoke(move |m, tx| m.process_downstream_response(tx, &resp))
    }
    pub fn new_configuration(&mut self, view: ConfigurationView<NodeViewConfig>){
       self.invoke(move |m, _| m.reconfigure(&view))
    }
    pub fn hello_downstream (&mut self, token: mio::Token, at: Timestamp<WallT>, epoch: Epoch){
       self.invoke(move |m, tx| m.hello_downstream(tx, token, at, epoch))
    }
    pub fn consume_requested(&mut self, token: mio::Token, mark: Seqno){
       self.invoke(move |m, tx| m.consume_requested(tx, token, mark))
    }
    pub fn reset(&mut self) {
       self.invoke(move |m, _| m.reset())
    }
}

#[cfg(test)]
pub mod test {
    use data::{Operation, OpResp, PeerMsg, Seqno, Buf, ReplicationMessage};
    use config::Epoch;
    use quickcheck::{self, Arbitrary, Gen, TestResult};
    use replica::{ReplModel, Outputs, Log};
    use std::sync::mpsc::channel;
    use replication_log::test::{VecLog, TestLog, hash};
    use config::ConfigurationView;
    use env_logger;
    use spki_sexp;
    use std::collections::{VecDeque, HashMap, BTreeMap};
    use mio::Token;
    use hybrid_clocks::{Clock,Wall, Timestamp, WallT};
    use std::mem;
    use std::iter;
    use std::sync::{Arc,Mutex};

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
            self.0.push_back(OutMessage::Forward(
                ReplicationMessage {
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
        ClientOperation(Token, Vec<u8>), // UpstreamOperation(Token, Seqno, Epoch, Operation),
        Response(Token, OpResp),
        Forward(ReplicationMessage),
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

    fn postcondition<L: TestLog>(_actual: &ReplModel<L>, model: &FakeReplica,
            cmd: &ReplCommand, observed: &Outs) -> bool {
        debug!("observed: {:?}; model:{:?}", observed, model);
        for msg in observed.0.iter() {
            match msg {
                &OutMessage::Forward(ReplicationMessage { epoch, msg: PeerMsg::Prepare(seqno, _), .. }) => {
                    assert_eq!(epoch, model.epoch);
                    assert!(Some(seqno) > model.prepared);
                },
                &OutMessage::Forward(ReplicationMessage { epoch, msg: PeerMsg::CommitTo(seqno), .. }) => {
                    assert_eq!(epoch, model.epoch);
                    assert!(Some(seqno) <= model.prepared);
                    assert!(Some(seqno) <= model.log_committed);
                },
                &OutMessage::Forward(ReplicationMessage { epoch, .. }) => {
                    assert_eq!(epoch, model.epoch);
                },
                &OutMessage::Response(token, OpResp::Ok(epoch, seqno, _)) |
                        &OutMessage::Response(token,  OpResp::Err(epoch, seqno, _)) => {
                    assert!(model.outstanding.contains_key(&token));
                    assert_eq!(epoch, model.epoch);
                    assert!(model.responded < Some(seqno));
                },
                &OutMessage::Response(token, _) => {
                    assert!(model.outstanding.contains_key(&token));
                },
                &OutMessage::LogCommitted(_seqno) => {},
                &OutMessage::ConsumerMessage(_, _, _) => {},
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
                    },
                    &OutMessage::Forward(ReplicationMessage { epoch, msg: PeerMsg::CommitTo(seqno), .. }) => {
                        self.epoch = epoch;
                        self.committed = Some(seqno);
                    },
                    &OutMessage::Forward(ReplicationMessage { epoch, msg: _, .. }) => {
                        self.epoch = epoch;
                    },
                    &OutMessage::Response(token, OpResp::Ok(epoch, seqno, _)) |
                            &OutMessage::Response(token,  OpResp::Err(epoch, seqno, _)) => {
                        *self.outstanding.entry(token).or_insert(0) -= 1;
                        self.epoch = epoch;
                        self.responded = Some(seqno);
                    },
                    &OutMessage::Response(token, _) => {
                        *self.outstanding.entry(token).or_insert(0) -= 1;
                    },
                    &OutMessage::LogCommitted(seqno) => {
                        self.log_committed = Some(seqno);
                    },
                    &OutMessage::ConsumerMessage(_, _, _) => {},
                };
            }
        }
    }

    fn apply_cmd<L: TestLog, O: Outputs>(actual: &mut ReplModel<L>, cmd: &ReplCommand, token: Token, outputs: &mut O) -> () {
        match cmd {
            &ReplCommand::ClientOperation(ref token, ref op) => {
                actual.process_client(outputs, *token, &op)
            },

            &ReplCommand::Forward(
                ReplicationMessage { epoch, msg: PeerMsg::Prepare(seq, ref data) , .. }) => {
                actual.process_operation(outputs, token, seq, epoch, &data);
            },
            &ReplCommand::Forward(
                ReplicationMessage { epoch, msg: PeerMsg::CommitTo(seq) , .. }) => {
                actual.commit_observed(seq);
            },
            &ReplCommand::Response(token, ref reply) => {
                actual.process_downstream_response(outputs, reply)
            },
            other => panic!("Unimplemented apply_cmd: {:?}", other)
        };
        actual.process_replication(outputs);
    }

    impl Arbitrary for Commands {
        fn arbitrary<G: Gen>(g: &mut G) -> Commands {
            let slots : Vec<()> = Arbitrary::arbitrary(g);
            let mut commands : Vec<ReplCommand> = Vec::new();
            let mut model = FakeReplica::new();

            for _ in slots {
                let cmd = (0..).map(|_| { let cmd : ReplCommand = Arbitrary::arbitrary(g); cmd })
                    .skip_while(|cmd| !precondition(&model, cmd))
                    .next()
                    .expect("Some valid command");

                model.next_state(&cmd);
                commands.push(cmd);
            };
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

        cmds.0.iter().scan(model, |model, cmd| {
            let ret = precondition(&model, cmd);
            model.next_state(&cmd);
            Some(ret)
        }).all(|p| p)
    }

    #[test]
    fn simulate_three_node_system_vec() {
        env_logger::init().unwrap_or(());
        let mut sim = NetworkSim::<VecLog>::new(3);
        sim.run_for(10, |t, sim, state| {
            if t == 0 {
                sim.client_request(state, b"hello_world".to_vec());
            }
        });
        sim.validate();
    }

    #[test]
    fn simulate_three_node_system_streaming_vec() {
        env_logger::init().unwrap_or(());
        let mut sim = NetworkSim::<VecLog>::new(3);
        sim.run_for(15, |t, sim, state| {
            if t < 3 {
                sim.client_request(state, format!("hello_world at {}", t).into_bytes())
            }
        });
        sim.validate();
    }


    struct OutBufs<'a> {
        node_id: usize,
        epoch: Epoch,
        ports: &'a mut HashMap<usize, VecDeque<(usize, ReplCommand)>>,
        clock: &'a mut Clock<Wall>,
    }

    impl<'a> OutBufs<'a> {
        fn enqueue(&mut self, dest: usize, cmd: ReplCommand) {
            let &mut OutBufs { node_id, ref mut ports, .. } = self;
            let queue = ports.entry(dest).or_insert_with(VecDeque::new);
            queue.push_back((node_id, cmd));
        }


        fn commit_observed(&mut self, seq: Seqno) {
            let now = self.clock.now();
            let epoch = self.epoch;
            self.forward_downstream(now, epoch, PeerMsg::CommitTo(seq));
        }
    }
    impl<'a> Outputs for OutBufs<'a> {
        fn respond_to(&mut self, token: Token, resp: &OpResp) {
            let cmd = ReplCommand::Response(token, resp.clone());
            self.enqueue(token.as_usize(), cmd);
        }

        fn forward_downstream(&mut self, now: Timestamp<WallT>, epoch: Epoch, msg: PeerMsg) {
            let &mut OutBufs { node_id, .. } = self;
            let downstream_id = node_id + 1; // TODO: Use a "view" object

            self.enqueue(downstream_id,
                ReplCommand::Forward(ReplicationMessage {
                    epoch: epoch,
                    ts: now,
                    msg: msg,
                }));
        }
        fn consumer_message(&mut self, token: Token, seqno: Seqno, msg: Buf) {
            unimplemented!()
        }
    }

    struct NetworkSim<L> {
        nodes: Vec<ReplModel<L>>,
        commit_queues: Vec<Arc<Mutex<VecDeque<Seqno>>>>,
        node_count: usize,
        epoch: Epoch,
        client_id: usize,
        configs: Vec<ConfigurationView<()>>,
        client_buf: VecDeque<ReplCommand>,
    }

    struct NetworkState {
        input_bufs: HashMap<usize, VecDeque<(usize, ReplCommand)>>,
        output_bufs: HashMap<usize, VecDeque<(usize, ReplCommand)>>,
    }

    impl NetworkState {
        fn is_quiescent(&self) -> bool {
            self.input_bufs.is_empty() && self.output_bufs.is_empty()
        }
    }

    impl<L: TestLog> NetworkSim<L> {
        fn new(node_count: usize) -> Self {
            let mut nodes = Vec::new();
            let mut commit_queues = Vec::new();

            let epoch = Epoch::from(0);

            for id in 0..node_count {
                let q = Arc::new(Mutex::new(VecDeque::new()));
                let q2 = q.clone();
                let log = L::new(move |seq| {
                    info!("committed {}: {:?}", id, seq);
                    q2.lock().expect("lock mutex").push_back(seq);
                });
                nodes.push(ReplModel::new(log));
                commit_queues.push(q);
            }

            let configs = (0..node_count)
                .zip((1..node_count).map(Some).chain(iter::repeat(None))).map(|(nid, next)| {
                debug!("fake config: id:{:?}; next:{:?}", nid, next);
                ConfigurationView {
                    epoch: epoch,
                    ord: nid,
                    next: next.map(|x| ()),
                }
            }).collect::<Vec<_>>();

            NetworkSim {
                nodes: nodes,
                commit_queues: commit_queues,
                node_count: node_count,
                epoch: epoch,
                client_id: node_count + 42,
                configs: configs,
                client_buf: VecDeque::new(),
            }
        }

        fn run_for<F: FnMut(usize, &Self, &mut NetworkState)>(&mut self, end_of_time: usize, mut f: F) {
            // Configuration event

            for (ref mut n, ref config) in self.nodes.iter_mut().zip(self.configs.iter()) {
                debug!("configure node: {:?}", config);
                n.reconfigure(config)
            }

            let mut state = NetworkState {
                input_bufs: HashMap::new(),
                output_bufs: HashMap::new(),
            };

            for t in 0..end_of_time {
                debug!("time: {:?}/{:?}", t, end_of_time);
                f(t, self, &mut state);
                self.step(t, &mut state);
            }

            assert!(state.is_quiescent());
        }

        fn client_request(&self, state: &mut NetworkState, val: Vec<u8>) {
            let head = 0usize;
            let cmd = ReplCommand::ClientOperation(Token(self.client_id), val);
            state.input_bufs.entry(head).or_insert_with(VecDeque::new).push_back(
                    (self.client_id, cmd));
        }

        fn validate(&self) {
            debug!("Client responses: {:?}", self.client_buf);

            assert_eq!(self.client_buf.iter()
                    .filter_map(|m| match m { &ReplCommand::Response(_, _) => None, other => Some(other)})
                    .collect::<Vec<&ReplCommand>>(),
                    Vec::<&ReplCommand>::new());

            let (ok, errors) : (Vec<_>, Vec<_>) = self.client_buf.iter()
                .filter_map(|m| match m { &ReplCommand::Response(_, ref m) => Some(m), _ => None })
                .partition(|&m| match m { &OpResp::Ok(_, _, _) => true, _ => false });

            debug!("Errors: {:?}", errors);
            assert!(errors.is_empty());

            let seqs = ok.into_iter()
                .filter_map(|m| match m { &OpResp::Ok(_, ref seq, _) => Some(seq), _ => None })
                .collect::<Vec<_>>();

            assert!(seqs.iter().zip(seqs.iter().skip(1)).all(|(a, b)| a < b), "is sorted");

            let logs_by_node = self.nodes.iter().enumerate().map(|(id, n)| {
                let committed_seq = n.borrow_log().read_committed();
                debug!("Committed @{:?}: {:?}", id, committed_seq);
                let committed = n.borrow_log().read_from(Seqno::zero())
                        .take_while(|&(s, _)| Some(s) <= committed_seq)
                        .map(|(s, val)| (s, hash(&val)))
                        .collect::<BTreeMap<_, _>>();
                committed
            }).collect::<Vec<_>>();

            debug!("Committed logs: {:#?}", logs_by_node);
            for a in 0..self.node_count {
                for b in (a..self.node_count).filter(|&b| a != b) {
                    let log_a = &logs_by_node[a];
                    let log_b = &logs_by_node[b];
                    debug!("compare log for {:?} ({:x}) with {:?} ({:x})", a, hash(log_a), b, hash(log_b));
                    assert_eq!(log_a, log_b);
                }
            }
        }

        fn step(&mut self, t: usize, state: &mut NetworkState) {
            let &mut NetworkState {
                ref mut input_bufs, ref mut output_bufs,
            } = state;

            let mut clock = Clock::wall();
            for (node_id, mut inputs) in input_bufs.drain() {
                debug!("Inputs for {:?}: {:?}", node_id, inputs);
                if node_id == self.client_id {
                    self.client_buf.extend(inputs.into_iter().map(|(_, m)| m));
                } else {
                    assert!(node_id < self.node_count);
                    let ref mut n = self.nodes[node_id];
                    let ref mut commits = self.commit_queues[node_id];
                    let mut output = OutBufs { node_id: node_id, ports: output_bufs, epoch: self.epoch, clock: &mut clock };

                    for (src, c)  in inputs {
                        debug!("Feed node {:?} with {:?}", node_id, c);
                        apply_cmd(n, &c, Token(src), &mut output);
                    }
                    n.borrow_log().quiesce();

                    while n.process_replication(&mut output) {
                        debug!("iterated replication for node {:?}", node_id);
                    }

                    // TODO: De-async the commit nonsense.
                    let mut queue = commits.lock().expect("lock");
                    if let Some(_) = self.configs[node_id].next {
                        if let Some(s) = queue.iter().max() {
                            output.commit_observed(*s);
                        }
                    }
                    queue.clear();
                }
            }

            mem::swap(input_bufs, output_bufs);

        }
    }
}
