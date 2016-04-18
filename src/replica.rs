use std::cmp;
use std::mem;
use std::collections::BTreeMap;
use mio;

use data::{Operation, OpResp, PeerMsg, Seqno, NodeViewConfig};
use config::{ConfigurationView, Epoch};
use spki_sexp;

#[derive(Debug)]
struct Forwarder {
    last_prepared_downstream: Option<Seqno>,
    last_committed_downstream: Option<Seqno>,
    last_acked_downstream: Option<Seqno>,
    pending_operations: BTreeMap<Seqno, mio::Token>,
}
#[derive(Debug)]
struct Register {
    content: String,
}

#[derive(Debug)]
struct Terminus {
    app: Register,
}

#[derive(Debug)]
enum ReplRole {
    Forwarder(Forwarder),
    Terminus(Terminus),
}

pub trait Log {
    fn seqno(&self) -> Seqno;
    fn read_prepared(&self) -> Option<Seqno>;
    fn read_committed(&self) -> Option<Seqno>;
    fn read(&self, Seqno) -> Option<Vec<u8>>;
    fn prepare(&mut self, Seqno, &[u8]);
    fn commit_to(&mut self, Seqno) -> bool;
}

#[derive(Debug)]
pub struct ReplModel<L> {
    next: ReplRole,
    log: L,
    current_epoch: Epoch,
    upstream_commited: Option<Seqno>,
    auto_commits: bool,
}

impl ReplRole {
    fn process_operation(&mut self,
                         token: mio::Token,
                         epoch: Epoch,
                         seqno: Seqno,
                         op: &[u8])
                         -> Option<OpResp> {
        match self {
            &mut ReplRole::Forwarder(ref mut f) => f.process_operation(token, epoch, seqno, op),
            &mut ReplRole::Terminus(ref mut t) => t.process_operation(token, epoch, seqno, op),
        }
    }
    pub fn process_downstream_response(&mut self, reply: &OpResp) -> Option<mio::Token> {
        match self {
            &mut ReplRole::Forwarder(ref mut f) => f.process_downstream_response(reply),
            _ => None,
        }
    }

    pub fn process_replication<L: Log, F: FnMut(PeerMsg)>(&mut self, log: &L, forward: F) -> bool {
        match self {
            &mut ReplRole::Forwarder(ref mut f) => f.process_replication(log, forward),
            _ => false,
        }
    }

    fn has_pending(&self, token: mio::Token) -> bool {
        match self {
            &ReplRole::Forwarder(ref f) => f.has_pending(token),
            _ => false,
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
            pending_operations: BTreeMap::new(),
        }
    }

    fn process_operation(&mut self,
                         token: mio::Token,
                         _epoch: Epoch,
                         seqno: Seqno,
                         _op: &[u8])
                         -> Option<OpResp> {
        self.pending_operations.insert(seqno, token);
        None
    }

    fn process_downstream_response(&mut self, reply: &OpResp) -> Option<mio::Token> {
        match reply {
            &OpResp::Ok(_epoch, seqno, _) | &OpResp::Err(_epoch, seqno, _) => {
                self.last_acked_downstream = Some(seqno);
                if let Some(token) = self.pending_operations.remove(&seqno) {
                    trace!("Found in-flight op {:?} for client token {:?}",
                           seqno,
                           token);
                    Some(token)
                } else {
                    warn!("Unexpected response for seqno: {:?}", seqno);
                    None
                }
            }
            &OpResp::HelloIWant(last_prepared_downstream) => {
                info!("Downstream has {:?}", last_prepared_downstream);
                // assert!(last_prepared_downstream <= self.seqno());
                self.last_acked_downstream = Some(last_prepared_downstream);
                self.last_prepared_downstream = Some(last_prepared_downstream);
                None
            }
        }
    }

    pub fn process_replication<L: Log, F: FnMut(PeerMsg)>(&mut self,
                                                          log: &L,
                                                          mut forward: F)
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
                        forward(PeerMsg::Prepare(i, op.into()));
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
            if ds_committed.map(|c| c < our_committed).unwrap_or(true) {
                let max_to_commit_now = cmp::min(ds_prepared, our_committed);
                debug!("Window commit {:?} - {:?}; committed locally: {:?}",
                       ds_committed,
                       max_to_commit_now,
                       our_committed);

                forward(PeerMsg::CommitTo(max_to_commit_now));
                self.last_committed_downstream = Some(max_to_commit_now);
                changed = true
            }
        }
        changed
    }

    fn has_pending(&self, token: mio::Token) -> bool {
        self.pending_operations.values().all(|tok| tok != &token)
    }

    fn reset(&mut self) {
        self.last_prepared_downstream = None
    }
}

impl Register {
    fn new() -> Register {
        Register { content: "".to_string() }
    }
    fn apply(&mut self, op: Operation) -> Option<Vec<u8>> {
        match op {
            Operation::Set(s) => {
                self.content = s;
                None
            }
            Operation::Get => Some(self.content.as_bytes().to_vec()),
        }
    }
}

impl Terminus {
    fn new() -> Terminus {
        Terminus { app: Register::new() }
    }

    fn process_operation(&mut self,
                         _token: mio::Token,
                         epoch: Epoch,
                         seqno: Seqno,
                         op: &[u8])
                         -> Option<OpResp> {
        let op = spki_sexp::from_bytes(&op).expect("decode operation");
        debug!("Terminus! {:?}/{:?}", seqno, op);
        let resp = self.app.apply(op);
        Some(OpResp::Ok(epoch, seqno, resp.map(From::from)))
    }
}

impl<L: Log> ReplModel<L> {
    pub fn new(log: L) -> ReplModel<L> {
        ReplModel {
            log: log,
            current_epoch: Default::default(),
            next: ReplRole::Terminus(Terminus::new()),
            upstream_commited: None,
            auto_commits: false,
        }
    }

    pub fn seqno(&self) -> Seqno {
        self.log.seqno()
    }

    fn next_seqno(&self) -> Seqno {
        self.log.seqno()
    }

    // If we can avoid coupling the ingest clock to the replicator; we can
    // factor this out into the client proxy handler.
    pub fn process_client(&mut self, token: mio::Token, op: &[u8]) -> Option<OpResp> {
        let seqno = self.next_seqno();
        let epoch = self.current_epoch;
        self.process_operation(token, seqno, epoch, op)
    }

    pub fn process_operation(&mut self,
                             token: mio::Token,
                             seqno: Seqno,
                             epoch: Epoch,
                             op: &[u8])
                             -> Option<OpResp> {

        if epoch != self.current_epoch {
            warn!("Operation epoch ({:?}) differers from our last observed configuration: ({:?})",
                  epoch,
                  self.current_epoch);
            let resp = OpResp::Err(epoch,
                                   seqno,
                                   format!("BadEpoch: {:?}; last seen config: {:?}",
                                           epoch,
                                           self.current_epoch));
            return Some(resp);
        }

        self.log.prepare(seqno, &op);

        self.next.process_operation(token, epoch, seqno, &op)
    }

    pub fn process_downstream_response(&mut self, reply: &OpResp) -> Option<mio::Token> {
        self.next.process_downstream_response(reply)
    }

    pub fn process_replication<F: FnMut(Epoch, PeerMsg)>(&mut self, mut forward: F) -> bool {
        let epoch = self.current_epoch;
        self.next.process_replication(&self.log, |msg| forward(epoch, msg))
    }

    pub fn has_pending(&self, token: mio::Token) -> bool {
        self.next.has_pending(token)
    }

    pub fn reset(&mut self) {
        self.next.reset()
    }

    pub fn commit_observed(&mut self, seqno: Seqno) -> bool {
        debug!("Observed upstream commit point: {:?}; current: {:?}",
               seqno,
               self.upstream_commited);
        assert!(self.upstream_commited.map(|committed| committed <= seqno).unwrap_or(true));
        self.upstream_commited = Some(seqno);
        true
    }

    pub fn flush(&mut self) -> bool {
        if self.auto_commits {
            self.upstream_commited = self.log.read_prepared();
            debug!("Auto committing to: {:?}", self.upstream_commited);
        } else {
            debug!("Flush to upstream commit point: {:?}",
                   self.upstream_commited);
        }
        if let Some(seqno) = self.upstream_commited {
            trace!("Commit for {:?}", seqno);
            self.log.commit_to(seqno)
        } else {
            false
        }
    }

    fn epoch_changed(&mut self, epoch: Epoch) {
        self.current_epoch = epoch;
    }

    fn set_has_downstream(&mut self, is_forwarder: bool) {
        // XXX: Replays?
        match (is_forwarder, &mut self.next) {
            (true, role @ &mut ReplRole::Terminus(_)) => {
                let prev = mem::replace(role, ReplRole::Forwarder(Forwarder::new()));
                info!("Switched to forwarding from {:?}", prev);
            }

            (false, role @ &mut ReplRole::Forwarder(_)) => {
                let prev = mem::replace(role, ReplRole::Terminus(Terminus::new()));
                info!("Switched to terminating from {:?}", prev);
            }
            _ => {
                info!("No change of config");
            }
        }
    }

    fn set_should_auto_commit(&mut self, auto_commits: bool) {
        self.auto_commits = auto_commits;
    }

    pub fn reconfigure(&mut self, view: &ConfigurationView<NodeViewConfig>) {
        self.set_should_auto_commit(view.is_head());
        self.set_has_downstream(view.should_connect_downstream().is_some());
        self.epoch_changed(view.epoch);
    }

}

#[cfg(test)]
mod test {
    use data::{Operation, OpResp, PeerMsg, Seqno, Buf};
    use quickcheck::{self, Arbitrary, Gen, TestResult};
    use super::{ReplModel, Register};
    use std::sync::mpsc::channel;
    use replication_log::{VecLog};
    use replication_log::test::TestLog;
    use env_logger;
    use spki_sexp;
    use std::collections::{VecDeque};
    use mio::Token;

    // impl<L: Log> ReplModel<L>
    // fn new(log: L) -> ReplModel<L>
    // fn seqno(&self) -> Seqno
    // fn process_operation(&mut self, token: Token, seqno: Option<Seqno>, epoch: Option<Epoch>, op: Operation) -> Option<OpResp>
    // fn process_downstream_response(&mut self, reply: &OpResp) -> Option<Token>
    // fn process_replication<F: FnMut(Epoch, PeerMsg)>(&mut self, forward: F) -> bool
    // fn epoch_changed(&mut self, epoch: Epoch)
    // fn has_pending(&self, token: Token) -> bool
    // fn reset(&mut self)
    // fn commit_observed(&mut self, seqno: Seqno) -> bool
    // fn flush(&mut self) -> bool
    // fn set_has_downstream(&mut self, is_forwarder: bool)
    // fn set_should_auto_commit(&mut self, auto_commits: bool)
    //

    #[derive(Debug,PartialEq,Eq,Clone)]
    enum ReplicaCommand {
        ClientOperation(Token, Operation), // UpstreamOperation(Token, Seqno, Epoch, Operation),
    }

    impl Arbitrary for ReplicaCommand {
        fn arbitrary<G: Gen>(g: &mut G) -> ReplicaCommand {
            let case = u64::arbitrary(g) % 1;
            let res = match case {
                0 => {
                    ReplicaCommand::ClientOperation(Token(Arbitrary::arbitrary(g)),
                                                    Arbitrary::arbitrary(g))
                }
                _ => unimplemented!(),
            };
            res
        }
        fn shrink(&self) -> Box<Iterator<Item = Self> + 'static> {
            match self {
                &ReplicaCommand::ClientOperation(ref token, ref op) => {
                    Box::new((token.as_usize(), op.clone())
                                 .shrink()
                                 .map(|(t, op)| ReplicaCommand::ClientOperation(Token(t), op)))
                }
            }
        }
    }

    // Simulate a single node in a Chain. We mostly just end up verifing that
    // results are as our trivial register model.

    fn simulate_single_node_chain_prop<L: TestLog>(vals: Vec<ReplicaCommand>) -> TestResult {

        debug!("commands: {:?}", vals);
        let (tx, rx) = channel();
        let log = L::new(move |seq| {
            info!("committed: {:?}", seq);
            tx.send(seq).expect("send")
            // TODO: Verify me.
        });

        let mut replication = ReplModel::new(log);
        let mut observed_responses = VecDeque::new();

        for cmd in vals.iter() {
            match cmd {
                &ReplicaCommand::ClientOperation(ref token, ref op) => {
                    let data_bytes = spki_sexp::as_bytes(op).expect("encode operation");
                    if let Some(resp) = replication.process_client(token.clone(),
                                                                   &data_bytes) {
                        observed_responses.push_back(resp);
                    }
                }
            }
        }
        debug!("Stopping Log");
        drop(replication);
        debug!("Stopped Log");

        let expected_responses = vals.iter()
                                     .filter_map(|c| {
                                         let &ReplicaCommand::ClientOperation(ref t, ref op) =
                                             c;
                                         Some((t.clone(), op.clone()))
                                     })
                                     .scan(Register::new(),
                                           |reg, (_tok, cmd)| Some(reg.apply(cmd).map(Into::into)))
                                     .collect::<Vec<Option<Buf>>>();
        trace!("Expected: {:?}", expected_responses);
        trace!("Observed: {:?}", observed_responses);
        assert_eq!(expected_responses.len(), observed_responses.len());
        let result = expected_responses.iter()
                                       .zip(observed_responses.iter())
                                       .all(|(exp, obs)| {
                                           match obs {
                                               &OpResp::Ok(_, _, ref val) => val == exp,
                                               _ => false,
                                           }
                                       });

        TestResult::from_bool(result)
    }

    #[test]
    fn simulate_single_node_chain_mem() {
        env_logger::init().unwrap_or(());
        quickcheck::quickcheck(simulate_single_node_chain_prop::<VecLog> as fn(vals: Vec<ReplicaCommand>) -> TestResult)
    }

    fn should_forward_all_requests_downstream_when_present_prop<L: TestLog>(
        log_prefix: Vec<Operation>,
        downstream_has: usize,
        commands: Vec<ReplicaCommand>,
        ) -> TestResult {
        let downstream_has = if log_prefix.is_empty() {
            0
        } else {
            downstream_has % log_prefix.len() as usize
        };

        debug!("should_forward_all_requests_downstream_when_present_prop:");
        debug!("log_prefix: {:?}", log_prefix);
        debug!("downstream_has: {:?}", downstream_has);
        debug!("commands: {:?}", commands);
        let mut log = L::new(move |seq| {
            info!("committed: {:?}", seq);
            // TODO: Verify me.
        });

        for (n, it) in log_prefix.iter().enumerate() {
            let seq = log.seqno();
            let data_bytes = spki_sexp::as_bytes(it).expect("encode operation");
            log.prepare(seq, &data_bytes)
        }

        let mut replication = ReplModel::new(log);
        let mut observed_responses = VecDeque::new();
        let mut forwarded = VecDeque::new();

        replication.set_has_downstream(true);
        replication.process_downstream_response(&OpResp::HelloIWant(Seqno::new(downstream_has as u64)));

        while replication.process_replication(|epoch, msg| {
            debug!("Forward @{:?}: {:?}", epoch, msg);
            forwarded.push_back(msg)
        }) {
            debug!("iterated replication");
        }

        for cmd in commands.iter() {
            debug!("apply: {:?}", cmd);
            match cmd {
                &ReplicaCommand::ClientOperation(ref token, ref op) => {
                    let data_bytes = spki_sexp::as_bytes(op).expect("encode operation");
                    if let Some(resp) = replication.process_client(token.clone(),
                                                                      &data_bytes) {
                        observed_responses.push_back(resp);
                    }
                },
            };
            while replication.process_replication(|epoch, msg| {
                debug!("Forward @{:?}: {:?}", epoch, msg);
                forwarded.push_back(msg)
            }) {
                debug!("iterated replication");
            }
        }

        debug!("Stopping Log");
        drop(replication);
        debug!("Stopped Log");

        let prepares =  forwarded.into_iter()
            .filter_map(|op| match op {
                PeerMsg::Prepare(seq, op) => Some((seq.offset() as usize, op)), _ => None
            })
            .collect::<Vec<_>>();
        let expected = log_prefix.into_iter()
            .map(|op| spki_sexp::as_bytes(&op).expect("encode operation"))
            .skip(downstream_has)
            .chain(commands.into_iter()
            .filter_map(|c| match c {
                ReplicaCommand::ClientOperation(_, op) => Some(spki_sexp::as_bytes(&op).expect("encode operation")),
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
            fn(Vec<Operation>, usize, Vec<ReplicaCommand>) -> TestResult)
    }
}
