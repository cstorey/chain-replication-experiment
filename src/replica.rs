use std::cmp;
use std::fmt;
use std::mem;
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
struct Handshaker {
    epoch: Epoch,
    is_sent: bool,
    pending_operations: HashMap<Seqno, mio::Token>,
}

#[derive(Debug)]
enum ReplRole {
    Handshaking(Handshaker),
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
            &mut ReplRole::Handshaking(ref mut h) => h.process_operation(output, token, epoch, seqno, op),
        }
    }
    fn process_downstream_response<O: Outputs>(&mut self, out: &mut O, reply: &OpResp) {
        trace!("ReplRole: process_downstream_response: {:?}", reply);
        let next = match self {
            &mut ReplRole::Forwarder(ref mut f) => {
                f.process_downstream_response(out, reply);
                None
            },
            &mut ReplRole::Handshaking(ref mut h) => h.process_downstream_response(out, reply),
            _ => None,
        };
        if let Some(next) = next {
            *self = next;
        }
    }

    fn process_replication<L: Log, O: Outputs>(&mut self, clock: &mut Clock<Wall>, epoch: Epoch, log: &L, out: &mut O) -> bool {
        trace!("ReplRole: process_replication");
        match self {
            &mut ReplRole::Forwarder(ref mut f) => f.process_replication(clock, epoch, log, out),
            &mut ReplRole::Terminus(ref mut t) => t.process_replication(clock, epoch, log, out),
            &mut ReplRole::Handshaking(ref mut h) => h.process_replication(clock, epoch, log, out),

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
    fn new(pending: HashMap<Seqno, mio::Token>) -> Forwarder {
        Forwarder {
            last_prepared_downstream: None,
            last_acked_downstream: None,
            last_committed_downstream: None,
            pending_operations: pending,
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
        trace!("Terminus: process_operation: {:?}/{:?}", epoch, seqno);
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

impl Handshaker {
    fn new(epoch: Epoch) -> Handshaker {
        Handshaker {
            epoch: epoch,
            is_sent: false,
            pending_operations: HashMap::new(),
        }
    }

    fn process_operation<O: Outputs>(&mut self,
                         _output: &mut O,
                         token: mio::Token,
                         _epoch: Epoch,
                         seqno: Seqno,
                         _op: &[u8]) {
        trace!("Handshaker: process_operation: {:?}/{:?}", _epoch, seqno);
        self.pending_operations.insert(seqno, token);
    }


    fn process_downstream_response<O: Outputs>(&mut self, out: &mut O, reply: &OpResp)
            -> Option<ReplRole> {
        trace!("Forwarder: {:?}", self);
        trace!("Forwarder: process_downstream_response: {:?}", reply);

        // Maybe the pending_gubbins should be moved up.
        match reply {
            &OpResp::Ok(_epoch, seqno, _) | &OpResp::Err(_epoch, seqno, _) => {
                if let Some(token) = self.pending_operations.remove(&seqno) {
                    trace!("Found in-flight op {:?} for client token {:?}",
                           seqno,
                           token);
                    out.respond_to(token, reply);
                } else {
                    warn!("Unexpected response for seqno: {:?}", seqno);
                }
                None
            }
            &OpResp::HelloIWant(ts, last_prepared_downstream) => {
                info!("{}; Downstream has {:?}", ts, last_prepared_downstream);
                info!("Handshaking: switched to forwarding from {:?}", self);

                let pending = mem::replace(&mut self.pending_operations, HashMap::new());
                trace!("Pending operations: {:?}", self);
                Some(ReplRole::Forwarder(Forwarder::new(pending)))
            }
        }
    }

    fn process_replication<L: Log, O: Outputs>(&mut self,
            clock: &mut Clock<Wall>, epoch: Epoch, _log: &L, out: &mut O) -> bool {
        // We should probaby remember that we have sent this...
        if !self.is_sent {
            out.forward_downstream(clock.now(), epoch, PeerMsg::HelloDownstream);
            self.is_sent = true;
            true
        } else {
            false
        }
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

    fn configure_forwarding<T: Clone>(&mut self, view: &ConfigurationView<T>) {
        let is_forwarder = view.should_connect_downstream().is_some();
        match (is_forwarder, &mut self.next) {
            (true, role) => {
                info!("Handshaking: switched to greeting from {:?}", role);
                *role = ReplRole::Handshaking(Handshaker::new(view.epoch));
            }

            (false, role @ &mut ReplRole::Forwarder(_)) | (false, role @ &mut ReplRole::Handshaking(_)) => {
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
        self.configure_forwarding(view);
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
    use hybrid_clocks::{Clock, ManualClock, Timestamp, WallT};
    use std::mem;
    use std::iter;
    use std::sync::{Arc,Mutex};
    use std::path::{Path,PathBuf};
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
            &ReplCommand::Forward(
                ReplicationMessage { epoch, ts, msg: PeerMsg::HelloDownstream , .. }) => {
                actual.hello_downstream(outputs, token, ts, epoch);
            },
            &ReplCommand::Response(token, ref reply) => {
                actual.process_downstream_response(outputs, reply)
            },
            &ReplCommand::ConsumeFrom(token, seq) => {
                actual.consume_requested(outputs, token, seq)
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
        sim.run_for(15, "simulate_three_node_system_streaming_vec", |t, sim, state| {
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
        sim.run_for(15, "simulate_three_node_system_with_consumer", |t, sim, state| {
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
                warn!("OutBufs#forward_downstream no next in view; node: {:?}, view: {:?}; msg: {:?}",
                      self.node_id,
                      self.view,
                      msg);

            }
        }

        fn consumer_message(&mut self, token: Token, seqno: Seqno, msg: Buf) {
            let &mut OutBufs { node_id, .. } = self;
            self.enqueue(NodeId(token.as_usize()), ReplCommand::ConsumerMsg(seqno, msg))
        }
    }

    #[derive(Debug, Copy, Clone,Hash, Eq,PartialEq,Ord,PartialOrd)]
    struct NodeId(usize);
    impl NodeId {
        fn token(&self) -> Token {
            Token(self.0)
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
            self.input_bufs.iter().chain(self.output_bufs.iter()).flat_map(|(_, q)| q).all(|_| false)
        }

        fn enqueue(&mut self, src:NodeId, dst: NodeId, data: ReplCommand) {
            let sent = self.clock.now();
            self.output_bufs.entry(dst).or_insert_with(VecDeque::new).push_back((src, sent, data));
            trace!("enqueue:{:?}->{:?}; {:?}", src, dst, self);
        }

        fn dequeue_one(&mut self) -> Option<(NodeId, NodeId, ReplCommand)> {
            let (dest, ref mut queue) =
                if let Some((dest, q)) = self.input_bufs.iter_mut().filter(|&(_, ref q)| !q.is_empty()).next() {
                trace!("dequeue_one: Node:{:?}: {:?}", dest, q);
                (*dest, q)
            } else {
                trace!("dequeue_one: empty?");
                return None
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
    }

    #[derive(Debug)]
    struct Tracer {
        f: File,
    }
    impl Tracer {
        fn new(p: &Path) -> Tracer {
            let f = File::create(p).expect("open file");
            Tracer { f: f }
        }
        fn state(&mut self, t: Timestamp<u64>, process: &NodeId, state: String) {
            use std::io::Write;
            let mut m = BTreeMap::new();
            use serde_json::value::to_value;
            m.insert("type".to_string(), to_value("state"));
            m.insert("time".to_string(), to_value(&t));
            m.insert("process".to_string(), to_value(&format!("{:?}", process)));
            m.insert("state".to_string(), to_value(&state));
            serde_json::to_writer(&mut self.f, &m).expect("write json");
            self.f.write_all(b"\n").expect("write nl");
        }

        fn recv(&mut self, sent: Timestamp<u64>, recvd: Timestamp<u64>, src: &NodeId, dst: &NodeId, data: String) {
            use std::io::Write;
            use serde_json::value::to_value;
            let mut m = BTreeMap::new();
            m.insert("type".to_string(), to_value("recv"));
            m.insert("sent".to_string(), to_value(&sent));
            m.insert("recv".to_string(), to_value(&recvd));
            m.insert("src".to_string(), to_value(&format!("{:?}", src)));
            m.insert("dst".to_string(), to_value(&format!("{:?}", dst)));
            m.insert("data".to_string(), to_value(&data));
            serde_json::to_writer(&mut self.f, &m).expect("write json");
            self.f.write_all(b"\n").expect("write nl");
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
            ConfigurationView::of_membership(self.epoch, &node_id, members.clone())
                .expect("in view")
        }

        fn configs(&self) -> BTreeMap<NodeId, ConfigurationView<NodeId>> {
            let node_count = self.node_count;
            let members = self.nodes.iter().map(|(&id, _)| (id, id)).collect::<BTreeMap<_, _>>();
            members.keys()
                .map(|&id| (id,
                            ConfigurationView::of_membership(self.epoch, &id, members.clone()).expect("in view")))
                .collect()
        }

        fn run_for<F: FnMut(usize, &mut Self, &mut NetworkState)>(&mut self, end_of_time: usize, test_name:&str, mut f: F) {
            let mut epoch = None;
            let path = PathBuf::from(format!("target/run-trace-{}.jsons", test_name));
            let mut tracer = Tracer::new(&path);

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
                if state.is_quiescent() { break; }

                self.step(&mut state);
                debug!("network state {:?}; quiet:{:?}; : {:?}", t, state.is_quiescent(), state);

            }

            assert!(state.is_quiescent());
        }

        fn tail_node(&self) -> NodeId {
            NodeId(self.node_count - 1)
        }

        fn head_node(&self) -> NodeId {
            NodeId(0)
        }

        fn crash_node(&mut self, state: &mut NetworkState, node_id: NodeId) {
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

        fn validate_client_responses(&self, count: usize) {
            debug!("Client responses: {:?}", self.client_buf);

            assert_eq!(self.client_buf.iter()
                    .filter_map(|m| match m {
                        &ReplCommand::Response(_, _) | &ReplCommand::ConsumerMsg(_, _) => None, other => Some(other)
                        })
                    .collect::<Vec<&ReplCommand>>(),
                    Vec::<&ReplCommand>::new());

            let (ok, errors) : (Vec<_>, Vec<_>) = self.client_buf.iter()
                .filter_map(|m| match m { &ReplCommand::Response(_, ref m) => Some(m), _ => None })
                .partition(|&m| match m { &OpResp::Ok(_, _, _) => true, _ => false });

            debug!("Errors: {:?}", errors);
            // assert!(errors.is_empty());

            let seqs = ok.into_iter()
                .filter_map(|m| match m { &OpResp::Ok(_, ref seq, _) => Some(seq), _ => None })
                .collect::<Vec<_>>();

            assert_eq!(seqs.len(), count);
            assert!(seqs.iter().zip(seqs.iter().skip(1)).all(|(a, b)| a < b), "is sorted");
        }

        fn validate_logs(&self) {
            let logs_by_node = self.nodes.iter().map(|(id, n)| {
                let committed_seq = n.borrow_log().read_committed();
                debug!("Committed @{:?}: {:?}", id, committed_seq);
                let committed = n.borrow_log().read_from(Seqno::zero())
                        .take_while(|&(s, _)| Some(s) <= committed_seq)
                        .map(|(s, val)| (s, hash(&val)))
                        .collect::<BTreeMap<_, _>>();
                (id, committed)
            }).collect::<BTreeMap<_, _>>();

            debug!("Committed logs: {:#?}", logs_by_node);
            for (a, log_a) in logs_by_node.iter() {
                for (b, log_b) in logs_by_node.iter().filter(|&(b, _)| a != b) {
                    debug!("compare log for {:?} ({:x}) with {:?} ({:x})", a, hash(log_a), b, hash(log_b));
                    assert_eq!(log_a, log_b);
                }
            }
        }

        fn consumed_messages(&self) -> Vec<(Seqno, Buf)> {
             self.client_buf.iter().filter_map(|m| match m {
                &ReplCommand::ConsumerMsg(seq, ref buf) => Some((seq, buf.clone())),
                _ => None,
            }).collect()
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
                        let mut output = OutBufs { node_id: dest, view: view, ports: state, epoch: self.epoch };

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
                let mut output = OutBufs { node_id: *nid, view: view, ports: state, epoch: self.epoch };
                while n.process_replication(&mut output) {
                    debug!("iterated replication for node {:?}", nid);
                }
            }

            state.flip();
        }
    }
}
