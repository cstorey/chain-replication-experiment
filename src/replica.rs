use std::cmp;
use std::fmt;
use std::mem;
use std::thread;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::mpsc::{Receiver, Sender, channel};
use mio;

use data::{Buf, OpResp, PeerMsg, Seqno};
use config::{ConfigurationView, Epoch};
use consumer::Consumer;
use Notifier;
use hybrid_clocks::{Clock, Timestamp, Wall, WallT};

#[derive(Debug)]
struct Forwarder<D> {
    last_prepared_downstream: Option<Seqno>,
    last_committed_downstream: Option<Seqno>,
    last_acked_downstream: Option<Seqno>,
    pending_operations: HashMap<Seqno, D>,
    downstream: D,
}

#[derive(Debug)]
struct Terminus<D: Hash + Eq> {
    consumers: HashMap<D, Consumer>,
}

#[derive(Debug)]
struct Handshaker<D> {
    epoch: Epoch,
    is_sent: bool,
    pending_operations: HashMap<Seqno, D>,
    downstream: D,
}

#[derive(Debug)]
enum ReplRole<D: Hash + Eq> {
    Handshaking(Handshaker<D>),
    Forwarder(Forwarder<D>),
    Terminus(Terminus<D>),
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
    /// Internal reference to a connection.
    type Dest;
    fn respond_to(&mut self, Self::Dest, &OpResp);
    fn forward_downstream(&mut self, Timestamp<WallT>, Epoch, PeerMsg);
    fn consumer_message(&mut self, Self::Dest, Seqno, Buf);
}

#[derive(Debug)]
pub struct ReplModel<L, D: Eq + Hash> {
    next: ReplRole<D>,
    log: L,
    clock: Clock<Wall>,
    current_epoch: Epoch,
    upstream_committed: Option<Seqno>,
    commit_requested: Option<Seqno>,
    is_head: bool,
}

impl<D: fmt::Debug + Eq + Hash + Clone> ReplRole<D> {
    fn process_operation<O: Outputs<Dest = D>>(&mut self,
                                               output: &mut O,
                                               token: O::Dest,
                                               epoch: Epoch,
                                               seqno: Seqno,
                                               op: &[u8]) {
        trace!("role: process_operation: {:?}/{:?}", epoch, seqno);
        match self {
            &mut ReplRole::Forwarder(ref mut f) => {
                f.process_operation(output, token, epoch, seqno, op)
            }
            &mut ReplRole::Terminus(ref mut t) => {
                t.process_operation(output, token, epoch, seqno, op)
            }
            &mut ReplRole::Handshaking(ref mut h) => {
                h.process_operation(output, token, epoch, seqno, op)
            }
        }
    }
    fn process_downstream_response<O: Outputs<Dest = D>>(&mut self, out: &mut O, reply: &OpResp) {
        trace!("ReplModel: process_downstream_response: {:?}", reply);
        let next = match self {
            &mut ReplRole::Forwarder(ref mut f) => {
                f.process_downstream_response(out, reply);
                None
            }
            &mut ReplRole::Handshaking(ref mut h) => h.process_downstream_response(out, reply),
            _ => None,
        };
        if let Some(next) = next {
            *self = next;
        }
    }

    fn process_replication<L: Log, O: Outputs<Dest = D>>(&mut self,
                                                         clock: &mut Clock<Wall>,
                                                         epoch: Epoch,
                                                         log: &L,
                                                         out: &mut O)
                                                         -> bool {
        trace!("ReplRole: process_replication");
        match self {
            &mut ReplRole::Forwarder(ref mut f) => f.process_replication(clock, epoch, log, out),
            &mut ReplRole::Terminus(ref mut t) => t.process_replication(clock, epoch, log, out),
            &mut ReplRole::Handshaking(ref mut h) => h.process_replication(clock, epoch, log, out),

        }
    }

    pub fn consume_requested<O: Outputs<Dest = D>>(&mut self,
                                                   out: &mut O,
                                                   token: O::Dest,
                                                   epoch: Epoch,
                                                   mark: Seqno) {
        match self {
            &mut ReplRole::Terminus(ref mut t) => t.consume_requested(out, token, epoch, mark),
            other => {
                error!("consume_requested of {:?}", other);
                out.respond_to(token,
                               &OpResp::Err(epoch,
                                            mark,
                                            "cannot consume from non-terminus".to_string()))
            }
        }
    }
    fn reset(&mut self) {
        match self {
            &mut ReplRole::Forwarder(ref mut f) => f.reset(),
            _ => (),
        }
    }

    fn move_handshaker(&mut self, epoch: Epoch, downstream: D) {
        let it = match self {
            &mut ReplRole::Forwarder(ref f) => f.to_handshaker(epoch, downstream),
            &mut ReplRole::Terminus(ref t) => t.to_handshaker(epoch, downstream),
            &mut ReplRole::Handshaking(ref h) => h.to_handshaker(epoch, downstream),
        };
        *self = ReplRole::Handshaking(it);
    }

    fn move_terminus(&mut self) {
        *self = ReplRole::Terminus(Terminus::new());
    }
}

impl<D: fmt::Debug + Clone + Eq + Hash> Forwarder<D> {
    fn new(downstream: D, pending: HashMap<Seqno, D>) -> Forwarder<D> {
        Forwarder {
            last_prepared_downstream: None,
            last_acked_downstream: None,
            last_committed_downstream: None,
            pending_operations: pending,
            downstream: downstream,
        }
    }

    fn process_operation<O: Outputs<Dest = D>>(&mut self,
                                               _output: &mut O,
                                               token: O::Dest,
                                               _epoch: Epoch,
                                               seqno: Seqno,
                                               _op: &[u8]) {
        trace!("Forwarder: process_operation: {:?}/{:?}", _epoch, seqno);
        self.pending_operations.insert(seqno, token);
    }

    fn process_downstream_response<O: Outputs<Dest = D>>(&mut self, out: &mut O, reply: &OpResp) {
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

    fn process_replication<L: Log, O: Outputs<Dest = D>>(&mut self,
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
    fn to_handshaker(&self, epoch: Epoch, downstream: D) -> Handshaker<D> {
        Handshaker::new(epoch, downstream, self.pending_operations.clone())
    }
}

impl<D: fmt::Debug + Clone + Eq + Hash> Terminus<D> {
    fn new() -> Terminus<D> {
        Terminus { consumers: HashMap::new() }
    }

    fn process_operation<O: Outputs<Dest = D>>(&mut self,
                                               output: &mut O,
                                               token: O::Dest,
                                               epoch: Epoch,
                                               seqno: Seqno,
                                               op: &[u8]) {
        trace!("Terminus: process_operation: {:?}/{:?}", epoch, seqno);
        output.respond_to(token, &OpResp::Ok(epoch, seqno, None))
    }
    pub fn consume_requested<O: Outputs<Dest = D>>(&mut self,
                                                   _out: &mut O,
                                                   token: O::Dest,
                                                   epoch: Epoch,
                                                   mark: Seqno) {
        debug!("Terminus: consume_requested: {:?} from {:?}@{:?}",
               token,
               mark,
               epoch);
        let consumer = self.consumers.entry(token).or_insert_with(|| Consumer::new());
        consumer.consume_requested(mark).expect("consume");
    }

    fn process_replication<L: Log, O: Outputs<Dest = D>>(&mut self,
                                                         _clock: &mut Clock<Wall>,
                                                         _epoch: Epoch,
                                                         log: &L,
                                                         out: &mut O)
                                                         -> bool {
        let mut changed = false;
        for (token, cons) in self.consumers.iter_mut() {
            changed |= cons.process(out, token.clone(), log)
        }
        changed
    }

    fn to_handshaker(&self, epoch: Epoch, downstream: D) -> Handshaker<D> {
        Handshaker::new(epoch, downstream, HashMap::new())
    }
}

impl<D: Eq + Hash + fmt::Debug + Clone> Handshaker<D> {
    fn new(epoch: Epoch, downstream: D, pending: HashMap<Seqno, D>) -> Handshaker<D> {
        Handshaker {
            epoch: epoch,
            is_sent: false,
            pending_operations: pending,
            downstream: downstream,
        }
    }

    fn process_operation<O: Outputs<Dest = D>>(&mut self,
                                               _output: &mut O,
                                               token: D,
                                               _epoch: Epoch,
                                               seqno: Seqno,
                                               _op: &[u8]) {
        trace!("Handshaker: process_operation: {:?}/{:?}", _epoch, seqno);
        self.pending_operations.insert(seqno, token);
    }


    fn process_downstream_response<O: Outputs<Dest = D>>(&mut self,
                                                         out: &mut O,
                                                         reply: &OpResp)
                                                         -> Option<ReplRole<D>> {
        trace!("Handshaker: {:?}", self);
        trace!("Handshaker: process_downstream_response: {:?}", reply);

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
                let forwarder = self.to_forwarder();
                info!("Handshaking: switched to {:?}", forwarder);
                Some(ReplRole::Forwarder(forwarder))
            }
        }
    }

    fn process_replication<L: Log, O: Outputs<Dest = D>>(&mut self,
                                                         clock: &mut Clock<Wall>,
                                                         epoch: Epoch,
                                                         _log: &L,
                                                         out: &mut O)
                                                         -> bool {
        // We should probaby remember that we have sent this...
        if !self.is_sent {
            out.forward_downstream(clock.now(), epoch, PeerMsg::HelloDownstream);
            self.is_sent = true;
            true
        } else {
            false
        }
    }

    fn to_handshaker(&self, epoch: Epoch, downstream: D) -> Handshaker<D> {
        Handshaker::new(epoch, downstream, self.pending_operations.clone())
    }
    fn to_forwarder(&self) -> Forwarder<D> {
        Forwarder::new(self.downstream.clone(), self.pending_operations.clone())
    }
}

impl<L: Log, D: Eq + Hash + Clone + fmt::Debug> ReplModel<L, D> {
    pub fn new(log: L) -> ReplModel<L, D> {
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

    pub fn borrow_log(&self) -> &L {
        &self.log
    }

    pub fn seqno(&self) -> Seqno {
        self.log.seqno()
    }

    fn next_seqno(&self) -> Seqno {
        self.log.seqno()
    }

    fn run_from(&mut self, rx: Receiver<ReplOp<L, D>>, mut tx: Notifier)
        where Notifier: Outputs<Dest = D>
    {
        loop {
            let mut cmd = rx.recv().expect("recv model command");
            cmd(self, &mut tx);

            while self.process_replication(&mut tx) {
                // Nothing
            }
        }
    }

    // If we can avoid coupling the ingest clock to the replicator; we can
    // factor this out into the client proxy handler.
    pub fn process_client<O: Outputs<Dest = D>>(&mut self,
                                                output: &mut O,
                                                token: O::Dest,
                                                op: &[u8]) {
        let seqno = self.next_seqno();
        let epoch = self.current_epoch;
        assert!(self.is_head);
        self.process_operation(output, token, seqno, epoch, op)
    }

    pub fn process_operation<O: Outputs<Dest = D>>(&mut self,
                                                   output: &mut O,
                                                   token: O::Dest,
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

    pub fn process_downstream_response<O: Outputs<Dest = D>>(&mut self,
                                                             out: &mut O,
                                                             reply: &OpResp) {
        trace!("ReplModel: process_downstream_response: {:?}", reply);
        self.next.process_downstream_response(out, reply)
    }

    pub fn process_replication<O: Outputs<Dest = D>>(&mut self, out: &mut O) -> bool {
        trace!("process_replication: {:?}", self);
        let mut res = self.next
                          .process_replication(&mut self.clock, self.current_epoch, &self.log, out);
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
            }
            _ => false,
        }
    }

    pub fn hello_downstream<O: Outputs<Dest = D>>(&mut self,
                                                  out: &mut O,
                                                  token: O::Dest,
                                                  at: Timestamp<WallT>,
                                                  epoch: Epoch) {
        debug!("{}; hello_downstream: {:?}; {:?}", at, token, epoch);

        let msg = OpResp::HelloIWant(at, self.log.seqno());
        info!("Inform upstream about our current version, {:?}!", msg);
        out.respond_to(token, &msg)
    }

    pub fn consume_requested<O: Outputs<Dest = D>>(&mut self,
                                                   out: &mut O,
                                                   token: O::Dest,
                                                   mark: Seqno) {
        self.next.consume_requested(out, token, self.current_epoch, mark)
    }

    fn configure_forwarding(&mut self, view: &ConfigurationView<D>) {
        let downstreamp = view.should_connect_downstream();
        match (downstreamp, &mut self.next) {
            (Some(downstream), role) => {
                info!("Handshaking: switched to greeting from {:?}", role);
                role.move_handshaker(view.epoch, downstream.clone());
            }

            (None, role @ &mut ReplRole::Forwarder(_)) |
            (None, role @ &mut ReplRole::Handshaking(_)) => {
                info!("Switched to terminating from {:?}", role);
                role.move_terminus()
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

    pub fn reconfigure(&mut self, view: &ConfigurationView<D>) {
        info!("Reconfiguring from: {:?}", view);
        self.set_epoch(view.epoch);
        self.configure_forwarding(view);
        self.set_is_head(view.is_head());
        info!("Reconfigured from: {:?}", view);
    }
}

type ReplOp<L, D> = Box<FnMut(&mut ReplModel<L, D>, &mut Notifier) + Send>;

pub struct ReplProxy<L: Log + Send + 'static, D: Send + 'static + Eq + Hash> {
    _inner_thread: thread::JoinHandle<()>,
    tx: Sender<ReplOp<L, D>>,
}

impl<L: Log + Send + 'static, D: Send + 'static + Eq + Hash + fmt::Debug + Clone> ReplProxy<L, D>
    where Notifier: Outputs<Dest = D>
{
    pub fn build(mut inner: ReplModel<L, D>, notifications: Notifier) -> Self {
        let (tx, rx) = channel();
        let inner_thread = thread::Builder::new()
                               .name("replmodel".to_string())
                               .spawn(move || inner.run_from(rx, notifications))
                               .expect("spawn model");
        ReplProxy {
            _inner_thread: inner_thread,
            tx: tx,
        }
    }

    fn invoke<F: FnMut(&mut ReplModel<L, D>, &mut Notifier) + Send + 'static>(&mut self, f: F) {
        self.tx.send(Box::new(f)).expect("send to inner thread")
    }

    pub fn process_operation(&mut self, token: D, epoch: Epoch, seqno: Seqno, op: Buf) {
        self.invoke(move |m, tx| m.process_operation(tx, token.clone(), seqno, epoch, &op))
    }
    pub fn client_operation(&mut self, token: D, op: Buf) {
        self.invoke(move |m, tx| m.process_client(tx, token.clone(), &op))
    }
    pub fn commit_observed(&mut self, _epoch: Epoch, seq: Seqno) {
        self.invoke(move |m, _| m.commit_observed(seq))
    }
    pub fn response_observed(&mut self, resp: OpResp) {
        self.invoke(move |m, tx| m.process_downstream_response(tx, &resp))
    }
    pub fn new_configuration(&mut self, view: ConfigurationView<D>) {
        self.invoke(move |m, _| m.reconfigure(&view))
    }
    pub fn hello_downstream(&mut self, token: D, at: Timestamp<WallT>, epoch: Epoch) {
        self.invoke(move |m, tx| m.hello_downstream(tx, token.clone(), at, epoch))
    }
    pub fn consume_requested(&mut self, token: D, mark: Seqno) {
        self.invoke(move |m, tx| m.consume_requested(tx, token.clone(), mark))
    }
    pub fn reset(&mut self) {
        self.invoke(move |m, _| m.reset())
    }
}
