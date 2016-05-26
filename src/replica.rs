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
    fn seqno(&self) -> Seqno;
    fn read_prepared(&self) -> Option<Seqno>;
    fn read_committed(&self) -> Option<Seqno>;
    fn read(&self, Seqno) -> Option<Vec<u8>>;
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
        if let Some(send_next) = self.last_prepared_downstream {
            if send_next < log.seqno() {
                let max_to_prepare_now = log.seqno();
                debug!("Window prepare {:?} - {:?}; prepared locally: {:?}",
                       send_next,
                       max_to_prepare_now,
                       log.read_prepared());
                for i in send_next.upto(&max_to_prepare_now) {
                    if let Some(op) = log.read(i) {
                        debug!("Queueing seq:{:?}/{:?}; ds/seqno: {:?}",
                               i,
                               op,
                               self.last_prepared_downstream);
                        out.forward_downstream(clock.now(), epoch, PeerMsg::Prepare(i, op.into()));
                        self.last_prepared_downstream = Some(i.succ());
                        changed = true
                    } else {
                        panic!("Attempted to replicate item not in log: {:?}", self);
                    }
                }
            }
            debug!("post Repl: Ours: {:?}; downstream sent: {:?}; acked: {:?}",
                   log.seqno(),
                   self.last_prepared_downstream,
                   self.last_acked_downstream);
        }

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

    pub fn reconfigure(&mut self, view: &ConfigurationView<NodeViewConfig>) {
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
    use replica::{ReplModel, Outputs};
    use std::sync::mpsc::channel;
    use replication_log::test::VecLog;
    use replication_log::test::TestLog;
    use env_logger;
    use spki_sexp;
    use std::collections::{VecDeque, HashMap};
    use mio::Token;
    use hybrid_clocks::{Clock, Timestamp, WallT};

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

    #[derive(Debug,PartialEq,Eq,Clone)]
    enum ReplCommand {
        ClientOperation(Token, Vec<u8>), // UpstreamOperation(Token, Seqno, Epoch, Operation),
    }

    #[derive(Debug,PartialEq,Eq,Clone)]
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
            }
        }
    }

    fn precondition(_model: &FakeReplica, cmd: &ReplCommand) -> bool {
        match cmd {
            &ReplCommand::ClientOperation(ref _token, ref _s) => true,
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
        }
    }

    impl FakeReplica {
        fn next_state(&mut self, cmd: &ReplCommand) -> () {
            match cmd {
                &ReplCommand::ClientOperation(ref token, ref _op) => {
                    *self.outstanding.entry(*token).or_insert(0) += 1;
                }
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

    fn apply_cmd<L: TestLog>(actual: &mut ReplModel<L>, cmd: &ReplCommand, outputs: &mut Outs) -> () {
        match cmd {
            &ReplCommand::ClientOperation(ref token, ref op) => {
                actual.process_client(outputs, *token, &op)
            },
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

    fn simulate_single_node_chain_prop<L: TestLog>(cmds: Commands) -> TestResult {
        let Commands(cmds) = cmds;
        debug!("Command sequence: {:?}", cmds);
        let epoch : Epoch = From::from(42);

        let mut model = FakeReplica::new();

        let (tx, rx) = channel();
        let mut actual = {
            let log = L::new(move |seq| {
                info!("committed: {:?}", seq);
                tx.send(seq).expect("send")
                // TODO: Verify me.
            });
            ReplModel::new(log)
        };
        actual.set_is_head(true);
        actual.set_epoch(epoch);
        model.epoch = epoch;

        for cmd in cmds {
            let mut observed = Outs(VecDeque::new());
            if !precondition(&model, &cmd) {
                // we have produced an invalid sequence.
                return TestResult::discard();
            }
            let ret = apply_cmd(&mut actual, &cmd, &mut observed);
            while let Ok(seq) = rx.try_recv() {
                observed.0.push_back(OutMessage::LogCommitted(seq))
            }
            trace!("Apply: {:?} => {:?}, {:?}", cmd, ret, observed);
            model.next_state(&cmd);
            assert!(postcondition(&actual, &model, &cmd, &observed));
            model.update_from_outputs(&observed);
        }

        debug!("Model state:{:?}", model);

        assert_eq!(
                model.outstanding.iter().filter(|&(_k, v)| v != &0).collect::<Vec<_>>(),
                vec![]);

        assert_eq!(model.prepared, Default::default());
        assert_eq!(model.log_committed, model.responded);

        TestResult::passed()
    }

    #[test]
    fn simulate_single_node_chain_mem() {
        env_logger::init().unwrap_or(());
        quickcheck::quickcheck(simulate_single_node_chain_prop::<VecLog> as fn(vals: Commands) -> TestResult)
    }

    fn should_forward_all_requests_downstream_when_present_prop<L: TestLog>(
        log_prefix: Vec<Operation>,
        downstream_has: usize,
        commands: Vec<ReplCommand>,
        ) -> TestResult {
        let downstream_has = if log_prefix.is_empty() {
            0
        } else {
            downstream_has % log_prefix.len() as usize
        };

        let mut clock = Clock::wall();

        debug!("should_forward_all_requests_downstream_when_present_prop:");
        debug!("log_prefix: {:?}", log_prefix);
        debug!("downstream_has: {:?}", downstream_has);
        debug!("commands: {:?}", commands);
        let mut log = L::new(move |seq| {
            info!("committed: {:?}", seq);
            // TODO: Verify me.
        });

        for (_seq, it) in log_prefix.iter().enumerate() {
            let seq = log.seqno();
            let data_bytes = spki_sexp::as_bytes(it).expect("encode operation");
            log.prepare(seq, &data_bytes)
        }

        let mut replication = ReplModel::new(log);
        replication.set_is_head(true);
        let mut observed = Outs(VecDeque::new());

        replication.configure_forwarding(true);
        replication.process_downstream_response(&mut observed, &OpResp::HelloIWant(clock.now(), Seqno::new(downstream_has as u64)));

        while replication.process_replication(&mut observed) {
            debug!("iterated replication");
        }

        for cmd in commands.iter() {
            debug!("apply: {:?}", cmd);
            match cmd {
                &ReplCommand::ClientOperation(ref token, ref op) => {
                    let data_bytes = spki_sexp::as_bytes(op).expect("encode operation");
                    replication.process_client(&mut observed, token.clone(), &data_bytes)
                },
            };
            while replication.process_replication(&mut observed) {
                debug!("iterated replication");
            }
        }

        debug!("Stopping Log");
        drop(replication);
        debug!("Stopped Log");

        let prepares =  observed.0.into_iter()
            .filter_map(|op| match op {
                    OutMessage::Forward(ReplicationMessage { msg: PeerMsg::Prepare(seq, op), .. }) =>
                        Some((seq.offset() as usize, op)),
                    _ => None
            })
            .collect::<Vec<_>>();
        let expected = log_prefix.into_iter()
            .map(|op| spki_sexp::as_bytes(&op).expect("encode operation"))
            .skip(downstream_has)
            .chain(commands.into_iter()
            .filter_map(|c| match c {
                ReplCommand::ClientOperation(_, op) => Some(spki_sexp::as_bytes(&op).expect("encode operation")),
            }))
            .map(Into::into)
            .zip(downstream_has..).map(|(x, i)| (i, x))
            .collect::<Vec<_>>();
        assert_eq!(prepares, expected);
        TestResult::from_bool(true)
    }

    #[test]
    fn should_forward_all_requests_downstream_when_present() {
        env_logger::init().unwrap_or(());
        quickcheck::quickcheck(
            should_forward_all_requests_downstream_when_present_prop::<VecLog> as
            fn(Vec<Operation>, usize, Vec<ReplCommand>) -> TestResult)
    }
}
