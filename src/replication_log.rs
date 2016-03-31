use std::fmt;
use rocksdb::{DB, Writable, Options, WriteBatch, WriteOptions};
use rocksdb::ffi::DBCFHandle;
use tempdir::TempDir;
use spki_sexp;
use data::Operation;
use data::Seqno;
use replica::Log;
use time::PreciseTime;
use std::thread;
use std::sync::mpsc;
use std::sync::Arc;

// Approximate structure of the log.
//
// The fun part is that each sequence number is Some(n) where n points to
// the *next* unwritten log slot. Unless it's unset, in which case we use
// None. This is ... suspect, and what happens when you just bang out code. It
// originally originated though, because at the time, the `Log#seqno` method
// was used solely to figure out the next slot that we should be writing to.

// However, we also use this very same logic to understand where we are in the
// replication stream. When we attempt to write say, two entries, we end up
// publishing the following messages:
//
// Prepare(0, "a")
// Prepare(1, "b")
// CommitTo(2)
//
// Which seems great, but we've just told the downstream replica to commit to
// an entry that hasn't been written yet. So've just told the downstream to
// commit an unwritten slot, triggers an `assert!` (the nearest I get to Hoare
// triples), and the whole castle comes tumbling down.

// Log Entries
//
//        R R R                        Replicas
//  ┌──┐  0 1 2
//  │  │        ◀─┐                    0   Head
//  │00│  X X O   │                    1 Middle
//  │ 1│  X X O   └───R2.Commit        2   Tail
//  │ 2│  X X O
//  │ 3│  X X O
//  │ 4│  X X   ◀─────R2.Prepare
//  │ 5│  X X
//  │ 6│  X X
//  │ 7│  X X
//  │ 8│  X O   ◀─────R1.Commit
//  │ 9│  X O
//  │10│  X O
//  │ 1│  X O
//  │ 2│  O O   ◀─────R0.Commit
//  │ 3│  O O
//  │ 4│  O O
//  │ 5│  O O
//  │ 6│  O     ◀─────R1.Prepare
//  │ 7│  O
//  │ 8│  O
//  │ 9│  O
//  └──┘        ◀─────R0.Prepare

pub struct RocksdbLog {
    dir: TempDir,
    db: Arc<DB>,
    meta: DBCFHandle,
    data: DBCFHandle,
    flush_tx: mpsc::SyncSender<Seqno>,
    flush_thread: thread::JoinHandle<()>,
}

const META: &'static str = "meta";
const DATA: &'static str = "data";
const META_PREPARED: &'static str = "prepared";
const META_COMMITTED: &'static str = "committed";

impl fmt::Debug for RocksdbLog {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("RocksdbLog").field("dir", &self.dir.path()).finish()
    }
}

impl RocksdbLog {
    pub fn new<F: Fn(Seqno) + Send + 'static>(committed: F) -> RocksdbLog {
        let d = TempDir::new("rocksdb-log").expect("new log");
        info!("DB path: {:?}", d.path());
        let mut db = DB::open_default(&d.path().to_string_lossy()).expect("open db");
        let meta = db.create_cf(META, &Options::new()).expect("open meta cf");
        let data = db.create_cf(DATA, &Options::new()).expect("open data cf");
        let db = Arc::new(db);
        let (flush_tx, flush_rx) = mpsc::sync_channel(42);

        let flusher = {
            let db = db.clone();
            thread::Builder::new()
                .name("flusher".to_string())
                .spawn(move || Self::flush_thread_loop(db, flush_rx, committed))
                .expect("spawn flush thread")
        };
        RocksdbLog {
            dir: d,
            db: db,
            meta: meta,
            data: data,
            flush_tx: flush_tx,
            flush_thread: flusher,
        }
    }

    fn flush_thread_loop<F: Fn(Seqno)>(db: Arc<DB>, rx: mpsc::Receiver<Seqno>, committed: F) {
        let meta = db.cf_handle(META).expect("open meta cf").clone();
        debug!("Awaiting flush");
        let mut prev_commit = None;
        while let Ok(seqno) = rx.recv() {
            debug!("Got commit: {:?}", seqno);
            if prev_commit.map(|p| p < seqno).unwrap_or(true) {
                debug!("Flushing: {:?} -> {:?}", prev_commit, seqno);
                Self::do_commit_to(db.clone(), meta, seqno);
                debug!("Flushed: {:?}", seqno);
                prev_commit = Some(seqno);
                committed(seqno);
            } else {
                debug!("Redundant commit: {:?} -> {:?}", prev_commit, seqno);
            }
        }
        debug!("Exiting flush thread");
    }

    fn do_read_seqno(db: &DB, meta: DBCFHandle, name: &str) -> Option<Seqno> {
        match db.get_cf(meta, name.as_bytes()) {
            Ok(Some(val)) => Some(Seqno::fromkey(&val)),
            Ok(None) => None,
            Err(e) => panic!("Unexpected error getting commit point: {:?}", e),
        }
    }

    fn read_seqno(&self, name: &str) -> Option<Seqno> {
        Self::do_read_seqno(&self.db, self.meta, name)
    }

    fn do_commit_to(db: Arc<DB>, meta: DBCFHandle, commit_seqno: Seqno) {
        debug!("Commit {:?}", commit_seqno);
        let key = Seqno::tokey(&commit_seqno);

        let prepared = Self::do_read_seqno(&db, meta, META_PREPARED);
        let committed = Self::do_read_seqno(&db, meta, META_COMMITTED);

        debug!("Committing: {:?}, committed, {:?}, prepared: {:?}",
               committed,
               committed,
               prepared);
        assert!(prepared.map(|p| p >= commit_seqno).unwrap_or(true));

        if committed == Some(commit_seqno) {
            debug!("Skipping, commits up to date");
            return;
        }

        let t0 = PreciseTime::now();
        let mut opts = WriteOptions::new();
        opts.set_sync(true);
        db.put_cf_opt(meta, META_COMMITTED.as_bytes(), &key.as_ref(), &opts)
          .expect("Persist commit point");
        let t1 = PreciseTime::now();
        debug!("Committed {:?} in: {}", commit_seqno, t0.to(t1));
    }

    pub fn auto_commit(&mut self) -> bool {
        let prepared = self.read_seqno(META_PREPARED);
        let committed = self.read_seqno(META_COMMITTED);
        let needed = match (prepared, committed) {
            (Some(prepared), Some(committed)) if committed < prepared => Some(prepared),
            (Some(prepared), None) => Some(prepared),
            _ => None,
        };
        if let Some(pt) = needed {
            info!("Flushing ({:?}->{:?})", committed, pt);
            self.commit_to(pt);
            true
        } else {
            false
        }
    }
    pub fn stop(self) {
        let RocksdbLog { db, flush_thread, flush_tx, .. } = self;
        drop(flush_tx);
        flush_thread.join().expect("Join flusher thread");
        drop(db);
    }
}

impl Log for RocksdbLog {
    fn seqno(&self) -> Seqno {
        self.read_prepared().as_ref().map(Seqno::succ).unwrap_or_else(Seqno::zero)
    }

    fn read_prepared(&self) -> Option<Seqno> {
        self.read_seqno(META_PREPARED)
    }

    fn read_committed(&self) -> Option<Seqno> {
        self.read_seqno(META_COMMITTED)
    }

    fn read(&self, seqno: Seqno) -> Option<Operation> {
        let key = Seqno::tokey(&seqno);
        let ret = match self.db.get_cf(self.data, &key.as_ref()) {
            Ok(Some(val)) => Some(spki_sexp::from_bytes(&val).expect("decode operation")),
            Ok(None) => None,
            Err(e) => panic!("Unexpected error reading index: {:?}: {:?}", seqno, e),
        };
        debug!("Read: {:?} => {:?}", seqno, ret);
        ret
    }


    fn prepare(&mut self, seqno: Seqno, op: &Operation) {
        let data_bytes = spki_sexp::as_bytes(op).expect("encode operation");
        debug!("Prepare {:?}", seqno);
        let key = Seqno::tokey(&seqno);

        let current = self.seqno();
        if !(current <= seqno) {
            panic!("Hole in history: saw {:?}; have: {:?}", seqno, current);
        }

        // match self.db.get_cf(self.data, &key.as_ref()) {
        // Ok(None) => (),
        // Err(e) => panic!("Unexpected error reading index: {:?}: {:?}", seqno, e),
        // Ok(_) => panic!("Unexpected entry at seqno: {:?}", seqno),
        // };

        let t0 = PreciseTime::now();
        let batch = WriteBatch::new();
        batch.put_cf(self.data, &key.as_ref(), &data_bytes).expect("Persist operation");
        batch.put_cf(self.meta, META_PREPARED.as_bytes(), &key.as_ref())
             .expect("Persist prepare point");
        self.db.write(batch).expect("Write batch");
        let t1 = PreciseTime::now();
        debug!("Prepare: {}", t0.to(t1));
        trace!("Watermarks: prepared: {:?}; committed: {:?}",
               self.read_seqno(META_PREPARED),
               self.read_seqno(META_COMMITTED));
    }

    fn commit_to(&mut self, seqno: Seqno) -> bool {
        debug!("Request commit upto: {:?}", seqno);
        let committed = self.read_seqno(META_COMMITTED);
        if committed.map(|c| c < seqno).unwrap_or(true) {
            debug!("Request to commit {:?} -> {:?}", committed, seqno);
            self.flush_tx.send(seqno).expect("Send to flusher");
            true
        } else {
            debug!("Request to commit {:?} -> {:?}; no-op", committed, seqno);
            false
        }
    }
}

pub struct VecLog {
    log: Vec<Operation>,
    on_committed: Box<Fn(Seqno)>,
    commit_point: Option<Seqno>,
}

impl Log for VecLog {
    fn seqno(&self) -> Seqno {
        Seqno::new(self.log.len() as u64)
    }
    fn read_prepared(&self) -> Option<Seqno> {
        self.log.iter().enumerate().rev().map(|(i, _)| Seqno::new(i as u64)).next()
    }

    fn read_committed(&self) -> Option<Seqno> { None }
    fn read(&self, pos: Seqno) -> Option<Operation> {
        self.log.get(pos.offset() as usize).map(|o| o.clone())
    }
    fn prepare(&mut self, pos: Seqno, op: &Operation) {
        assert_eq!(pos, self.seqno());
        self.log.push(op.clone())
    }
    fn commit_to(&mut self, pos: Seqno) -> bool {
        if Some(pos) > self.commit_point {
            (self.on_committed)(pos);
            self.commit_point = Some(pos);
        }
        false
    }
}

#[cfg(test)]
pub mod test {
    use data::{Operation, Seqno};
    use quickcheck::{Arbitrary, Gen, TestResult};
    use std::sync::mpsc::channel;
    use std::sync::atomic::{AtomicUsize,Ordering};
    use super::{VecLog,RocksdbLog};
    use replica::Log;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    // pub trait Log {
    // fn seqno(&self) -> Seqno;
    // fn read_prepared(&self) -> Seqno;
    // fn read_committed(&self) -> Seqno;
    // fn read(&self, Seqno) -> Option<Operation>;
    // fn prepare(&mut self, Seqno, &Operation);
    // fn commit_to(&mut self, Seqno) -> bool;
    // }
    //

    pub trait TestLog : Log {
        fn new<F: Fn(Seqno) + Send + 'static>(committed: F) -> Self;
        fn stop(self);
    }

    impl TestLog for RocksdbLog {
        fn new<F: Fn(Seqno) + Send + 'static>(committed: F) -> Self {
            RocksdbLog::new(committed)
        }
        fn stop(self) {
            RocksdbLog::stop(self)
        }
    }

    impl TestLog for VecLog {
        fn new<F: Fn(Seqno) + Send + 'static>(committed: F) -> Self {
            VecLog { log: Vec::new(), on_committed: Box::new(committed), commit_point: None }
        }
        fn stop(self) {
        }
    }

    #[derive(Debug,PartialEq,Eq,Clone)]
    enum LogCommand {
        PrepareNext(Operation),
        CommitTo(Seqno),
        ReadAt(Seqno),
    }

    impl Arbitrary for LogCommand {
        fn arbitrary<G: Gen>(g: &mut G) -> LogCommand {
            let case = u64::arbitrary(g) % 11;
            let res = match case {
                n if n < 5 => LogCommand::PrepareNext(Arbitrary::arbitrary(g)),
                n if 5 <= n && n < 10 => LogCommand::ReadAt(Arbitrary::arbitrary(g)),
                10 => LogCommand::CommitTo(Arbitrary::arbitrary(g)),
                _ => unimplemented!(),
            };
            res
        }
        fn shrink(&self) -> Box<Iterator<Item = Self> + 'static> {
            match self {
                &LogCommand::PrepareNext(ref op) => {
                    Box::new(op.shrink().map(LogCommand::PrepareNext))
                }
                &LogCommand::CommitTo(ref s) => Box::new(s.shrink().map(LogCommand::CommitTo)),
                &LogCommand::ReadAt(ref s) => Box::new(s.shrink().map(LogCommand::ReadAt)),
            }
        }
    }

    #[derive(Debug,PartialEq,Eq,Clone)]
    struct Commands(Vec<LogCommand>);

    fn precondition(model: &VecLog, cmd: &LogCommand) -> bool {
        true
    }

    fn next_state(model: &mut VecLog, cmd: &LogCommand) -> () {
        match cmd {
            &LogCommand::PrepareNext(ref op) => {
                let seq = model.seqno();
                model.prepare(seq, op)
            },
            &LogCommand::CommitTo(ref s) => { model.commit_to(s.clone()); },
            &LogCommand::ReadAt(ref s) => (),
        }
    }

    fn apply_cmd(model: &RocksdbLog, cmd: &LogCommand) -> () {
    }

    fn postcondition(model: &RocksdbLog, cmd: &LogCommand, ret: &()) -> bool {
        true
    }


    impl Arbitrary for Commands {
        fn arbitrary<G: Gen>(g: &mut G) -> Commands {
            let model_committed = Arc::new(AtomicUsize::new(0));
            let mut model_log = {
                let model_committed = model_committed.clone();
                VecLog::new(move |seq| model_committed.store(seq.offset() as usize, Ordering::SeqCst))
            };
            let slots : Vec<()> = Arbitrary::arbitrary(g);
            let mut commands : Vec<LogCommand> = Vec::new();

            for _ in slots {
                let cmd = (0..).map(|_| { let cmd : LogCommand = Arbitrary::arbitrary(g); cmd })
                    .skip_while(|cmd| !precondition(&model_log, cmd))
                    .next().expect("Some valid command");

                next_state(&mut model_log, &cmd);
                commands.push(cmd);
            };
            Commands(commands)
        }
        /* fn shrink(&self) -> Box<Iterator<Item = Self> + 'static> {
            match self {
                &LogCommand::PrepareNext(ref op) => {
                    Box::new(op.shrink().map(LogCommand::PrepareNext))
                }
                &LogCommand::CommitTo(ref s) => Box::new(s.shrink().map(LogCommand::CommitTo)),
                &LogCommand::ReadAt(ref s) => Box::new(s.shrink().map(LogCommand::ReadAt)),
            }
        }*/
    }

    fn should_be_correct_prop(cmds: Commands) -> bool {
        let Commands(cmds) = cmds;
        debug!("Command sequence: {:?}", cmds);

        let model_committed = Arc::new(AtomicUsize::new(0));
        let mut model_log = {
            let model_committed = model_committed.clone();
            VecLog::new(move |seq| model_committed.store(seq.offset() as usize, Ordering::SeqCst))
        };

        let actual_committed = Arc::new(AtomicUsize::new(0));
        let mut actual_log = {
            let actual_committed = actual_committed.clone();
            RocksdbLog::new(move |seq| actual_committed.store(seq.offset() as usize, Ordering::SeqCst))
        };

        for cmd in cmds {
            // This really should pass.
            assert!(precondition(&model_log, &cmd));
            let ret = apply_cmd(&mut actual_log, &cmd);
            debug!("Apply: {:?} => {:?}", cmd, ret);
            assert!(postcondition(&actual_log, &cmd, &ret));

        }
        true
    }
    #[test]
    fn should_be_correct() {
        use quickcheck;
        use env_logger;

        env_logger::init().unwrap_or(());

        quickcheck::quickcheck(should_be_correct_prop as fn(Commands) -> bool)
    }


    fn test_can_read_prepared_values_prop<L: TestLog>(mut vals: Vec<LogCommand>) -> TestResult {

        {
            let seq: Option<Seqno> = None;
            for cmd in vals.iter_mut() {
                match (seq, cmd) {
                    (ref mut seq, &mut LogCommand::PrepareNext(_)) => {
                        *seq = Some(seq.as_ref().map(Seqno::succ).unwrap_or_else(Seqno::zero))
                    }
                    _ => (),
                }
            }
        }


        let mut log = L::new(move |_seq| ());
        let mut seq = None;
        let mut prepared = BTreeMap::new();
        for cmd in vals {
            debug!("apply: {:?}", cmd);
            match cmd {
                LogCommand::PrepareNext(op) => {
                    assert_eq!(seq, log.read_prepared());

                    seq = Some(seq.as_ref().map(Seqno::succ).unwrap_or_else(Seqno::zero));
                    assert!(seq.is_some());
                    debug!("seqs: saw: {:?}, expected: {:?}", log.seqno(), seq);
                    if let Some(seq) = seq {
                        assert_eq!(log.seqno(), seq);
                        log.prepare(seq, &op);
                        let _ = prepared.insert(seq, op);
                    }
                }
                LogCommand::ReadAt(ref read_seq) => {
                    let result = log.read(*read_seq);
                    assert_eq!(result.as_ref(), prepared.get(read_seq))
                }
                LogCommand::CommitTo(_) => (),
            }
        }
        debug!("Stopping Log");
        log.stop();
        debug!("Stopped Log");
        TestResult::passed()
    }

    fn test_can_commit_prepared_values_prop<L: TestLog>(vals: Vec<LogCommand>) -> TestResult {

        debug!("commands: {:?}", vals);
        let vals: Vec<LogCommand> = vals.into_iter()
                                        .scan(None, |seq, cmd| {
                                            match (seq, cmd) {
                                                (seq, cmd @ LogCommand::PrepareNext(_)) => {
                                                    *seq = Some(seq.as_ref()
                                                                   .map(Seqno::succ)
                                                                   .unwrap_or_else(Seqno::zero));
                                                    Some(cmd)
                                                }
                                                (&mut None, LogCommand::CommitTo(commit)) => {
                                                    debug!("Commit to {:?} before prepare", commit);
                                                    None
                                                }
                                                (&mut Some(seq),
                                                 LogCommand::CommitTo(ref commit))
                                                    if *commit > seq => {
                                                    Some(LogCommand::CommitTo(seq))
                                                }
                                                (_, cmd) => Some(cmd),
                                            }
                                        })
                                        .collect();

        let expected_commit = vals.iter()
                                  .filter_map(|v| {
                                      if let &LogCommand::CommitTo(ref pt) = v {
                                          Some(*pt)
                                      } else {
                                          None
                                      }
                                  })
                                  .max();
        debug!("Expected commit: {:?}", expected_commit);
        if expected_commit.is_none() {
            debug!("Discard: {:?}", vals);
            return TestResult::discard();
        }


        let (tx, rx) = channel();
        let mut log = L::new(move |seq| {
            info!("committed: {:?}", seq);
            tx.send(seq).expect("send")
        });
        let mut seq = None;
        let mut prepared = BTreeMap::new();
        for cmd in vals {
            debug!("Apply: {:?}", cmd);
            match cmd {
                LogCommand::PrepareNext(op) => {
                    seq = Some(seq.as_ref().map(Seqno::succ).unwrap_or_else(Seqno::zero));
                    assert!(seq.is_some());
                    if let Some(seq) = seq {
                        assert_eq!(log.seqno(), seq);
                        log.prepare(seq, &op);
                        let _ = prepared.insert(seq, op);
                    }
                }
                LogCommand::CommitTo(ref commit) => {
                    let _ = log.commit_to(*commit);
                }
                LogCommand::ReadAt(_) => (),
            }
        }
        debug!("Stopping Log");
        let read_commit = log.read_committed();
        let read_prepared = log.read_prepared();
        log.stop();
        debug!("Stopped Log");

        let mut observed = None;

        debug!("Fetching commits");
        while let Ok(c) = rx.recv() {
            observed = Some(c)
        }
        debug!("observed: {:?}; expect: {:?}", observed, expected_commit);
        assert_eq!(observed, expected_commit);
        debug!("read_commit: {:?}; read_prepared:{:?}, expect: {:?}",
            read_commit, read_prepared, expected_commit);
        assert!(read_commit <= read_prepared);
        assert!(read_commit <= expected_commit);

        TestResult::passed()
    }

    mod rocksdb {
        use quickcheck::{self, TestResult};
        use super::super::RocksdbLog;
        use super::{test_can_commit_prepared_values_prop, test_can_read_prepared_values_prop,
                    LogCommand};
        use env_logger;

        #[test]
        fn test_can_read_prepared_values() {
            env_logger::init().unwrap_or(());
            quickcheck::quickcheck(test_can_read_prepared_values_prop::<RocksdbLog>
                    as fn(vals: Vec<LogCommand>) -> TestResult)
        }

        #[test]
        fn test_can_commit_prepared_values() {
            env_logger::init().unwrap_or(());
            quickcheck::quickcheck(test_can_commit_prepared_values_prop::<RocksdbLog> as fn(vals: Vec<LogCommand>) -> TestResult)
        }
    }

    mod vec {
        use rand;
        use data::{Operation, Seqno};
        use quickcheck::{self, Arbitrary, Gen, StdGen, TestResult};
        use std::io::Write;
        use std::sync::mpsc::channel;
        use super::super::VecLog;
        use super::{test_can_commit_prepared_values_prop, test_can_read_prepared_values_prop,
                    LogCommand};
        use replica::Log;
        use env_logger;
        use std::collections::BTreeMap;

        #[test]
        fn test_can_read_prepared_values() {
            env_logger::init().unwrap_or(());
            quickcheck::quickcheck(test_can_read_prepared_values_prop::<VecLog>
                    as fn(vals: Vec<LogCommand>) -> TestResult)
        }

        #[test]
        fn test_can_commit_prepared_values() {
            env_logger::init().unwrap_or(());
            quickcheck::quickcheck(test_can_commit_prepared_values_prop::<VecLog> as fn(vals: Vec<LogCommand>) -> TestResult)
        }
    }
}
