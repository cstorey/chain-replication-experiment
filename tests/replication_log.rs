
use vastatrix::data::Seqno;
use quickcheck::{Arbitrary, Gen, TestResult};
use std::fmt;
use vastatrix::replication_log::RocksdbLog;
use vastatrix::replica::Log;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher, SipHasher};
use std::iter;
use std::rc::Rc;
use std::cell::RefCell;

#[cfg(feature = "benches")]
use test::Bencher;

pub struct VecLog {
    log: Rc<RefCell<Vec<Vec<u8>>>>,
    commit_point: Option<Seqno>,
}

pub struct VecCursor(usize, Rc<RefCell<Vec<Vec<u8>>>>);

impl fmt::Debug for VecLog {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {

        fmt.debug_struct("VecLog")
           .field("mark/prepared", &self.log.borrow().len())
           .field("mark/committed", &self.commit_point)
           .finish()
    }
}

impl Log for VecLog {
    type Cursor = VecCursor;

    fn seqno(&self) -> Seqno {
        Seqno::new(self.log.borrow().len() as u64)
    }
    fn read_prepared(&self) -> Option<Seqno> {
        self.log.borrow().iter().enumerate().rev().map(|(i, _)| Seqno::new(i as u64)).next()
    }

    fn read_committed(&self) -> Option<Seqno> {
        self.commit_point
    }

    fn read_from(&self, pos: Seqno) -> Self::Cursor {
        VecCursor(pos.offset() as usize, self.log.clone())
    }

    fn prepare(&mut self, pos: Seqno, op: &[u8]) {
        assert_eq!(pos, self.seqno());
        self.log.borrow_mut().push(op.to_vec())
    }
    fn commit_to(&mut self, pos: Seqno) -> bool {
        if Some(pos) > self.commit_point {
            self.commit_point = Some(pos);
            true
        } else {
            false
        }
    }
}

impl iter::Iterator for VecCursor {
    type Item = (Seqno, Vec<u8>);
    fn next(&mut self) -> Option<Self::Item> {
        let res = self.1.borrow().get(self.0).map(|x| (Seqno::new(self.0 as u64), x.clone()));
        self.0 += 1;
        res
    }
}

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
    fn new() -> Self;
    fn quiesce(&self);
    fn stop(self);
    }

impl TestLog for RocksdbLog {
    fn new() -> Self {
        RocksdbLog::new()
    }
    fn quiesce(&self) {
        RocksdbLog::quiesce(self)
    }
    fn stop(self) {
        RocksdbLog::stop(self)
    }
}

impl TestLog for VecLog {
    fn new() -> Self {
        VecLog {
            log: Rc::new(RefCell::new(Vec::new())),
            commit_point: None,
        }
    }
    fn quiesce(&self) {}
    fn stop(self) {}
}

#[derive(Debug,PartialEq,Eq,Clone, Hash)]
pub enum LogCommand {
    PrepareNext(Vec<u8>),
    CommitTo(Seqno),
    ReadFrom(Seqno),
}

impl LogCommand {
    pub fn apply_to<L: Log>(&self, model: &mut L) {
        match self {
            &LogCommand::PrepareNext(ref op) => {
                let seq = model.seqno();
                model.prepare(seq, op)
            }
            &LogCommand::CommitTo(ref s) => {
                model.commit_to(s.clone());
            }
            &LogCommand::ReadFrom(_) => (),
        }
    }
    pub fn satisfies_precondition(&self, model: &VecLog) -> bool {
        match self {
            &LogCommand::CommitTo(ref s) => model.read_prepared().map(|p| &p >= s).unwrap_or(false),
            &LogCommand::ReadFrom(ref s) => model.read_prepared().map(|p| &p >= s).unwrap_or(false),
            _ => true,
        }
    }
}

impl Arbitrary for LogCommand {
    fn arbitrary<G: Gen>(g: &mut G) -> LogCommand {
        let case = u64::arbitrary(g) % 15;
        let res = match case {
            0...5 => LogCommand::PrepareNext(Arbitrary::arbitrary(g)),
            5...10 => LogCommand::ReadFrom(Arbitrary::arbitrary(g)),
            _ => LogCommand::CommitTo(Arbitrary::arbitrary(g)),
        };
        res
    }

    fn shrink(&self) -> Box<Iterator<Item = Self> + 'static> {
        let h = hash(self);
        trace!("LogCommand#shrink {:x}: {:?}", h, self);
        match self {
            &LogCommand::PrepareNext(ref op) => {
                Box::new(op.shrink()
                           .map(LogCommand::PrepareNext)
                           .inspect(move |it| trace!("LogCommand#shrink {:x}: => {:?}", h, it)))
            }
            &LogCommand::CommitTo(ref s) => {
                Box::new(s.shrink()
                          .map(LogCommand::CommitTo)
                          .inspect(move |it| trace!("LogCommand#shrink {:x}: => {:?}", h, it)))
            }
            &LogCommand::ReadFrom(ref s) => Box::new(s.shrink().map(LogCommand::ReadFrom)),
        }
    }
}

#[derive(Debug,PartialEq,Eq,Clone)]
pub struct LogCommands(Vec<LogCommand>);
#[derive(Debug,PartialEq,Eq,Clone)]
enum CommandReturn {
    Done,
    Read(Vec<Vec<u8>>),
}

fn next_state(model: &mut VecLog, cmd: &LogCommand) -> () {
    cmd.apply_to(model)
}

fn apply_cmd(actual: &mut RocksdbLog, cmd: &LogCommand) -> CommandReturn {
    match cmd {
        &LogCommand::PrepareNext(ref op) => {
            let seq = actual.seqno();
            actual.prepare(seq, op);
            CommandReturn::Done
        }
        &LogCommand::CommitTo(ref s) => {
            actual.commit_to(s.clone());
            CommandReturn::Done
        }
        &LogCommand::ReadFrom(ref s) => {
            CommandReturn::Read(actual.read_from(s.clone()).map(|(_, v)| v).collect())
        }
    }
}

fn postcondition(model: &RocksdbLog, cmd: &LogCommand, ret: &CommandReturn) -> bool {
    trace!("Check postcondition: {:?} -> {:?}", cmd, ret);
    match (cmd, ret) {
        (&LogCommand::PrepareNext(_), &CommandReturn::Done) => {
            // Assert that seqno has advanced
            true
        }
        (&LogCommand::CommitTo(_), &CommandReturn::Done) => {
            // Well, this happens at some time in the future.
            true
        }
        (&LogCommand::ReadFrom(ref seq),
         &CommandReturn::Read(ref val)) => {
            &model.read_from(seq.clone()).map(|(_, v)| v).collect::<Vec<_>>() == val
        }
        (cmd, ret) => {
            warn!("Unexpected command / return combination: {:?} -> {:?}",
                  cmd,
                  ret);
            false
        }
    }
}

pub fn arbitrary_given<G: Gen, T: Arbitrary, F: Fn(&T) -> bool>(g: &mut G, f: F) -> T {
    (0..)
        .map(|_| T::arbitrary(g))
        .skip_while(|cmd| !f(cmd))
        .next()
        .expect("Some valid command")
}

impl Arbitrary for LogCommands {
    fn arbitrary<G: Gen>(g: &mut G) -> LogCommands {
        let mut model_log = VecLog::new();
        let slots: Vec<()> = Arbitrary::arbitrary(g);
        let mut commands: Vec<LogCommand> = Vec::new();

        for _ in slots {
            let cmd = arbitrary_given(g, |cmd: &LogCommand| cmd.satisfies_precondition(&model_log));
            next_state(&mut model_log, &cmd);
            commands.push(cmd);
        }
        LogCommands(commands)
    }
    fn shrink(&self) -> Box<Iterator<Item = Self> + 'static> {
        // TODO: Filter out invalid sequences.
        let ret = Arbitrary::shrink(&self.0).map(LogCommands).filter(validate_commands);
        Box::new(ret)
    }
}
fn validate_commands(cmds: &LogCommands) -> bool {
    let model_log = VecLog::new();
    cmds.0
        .iter()
        .scan(model_log, |model_log, cmd| {
            let ret = cmd.satisfies_precondition(&model_log);
            next_state(model_log, &cmd);
            Some(ret)
        })
        .all(|p| p)
}

fn should_be_correct_prop(cmds: LogCommands) -> TestResult {
    let LogCommands(cmds) = cmds;
    debug!("Command sequence: {:?}", cmds);

    let model_log = VecLog::new();

    let mut actual_log = RocksdbLog::new();

    for cmd in cmds {
        if !cmd.satisfies_precondition(&model_log) {
            // we have produced an invalid sequence.
            return TestResult::discard();
        }
        let ret = apply_cmd(&mut actual_log, &cmd);
        trace!("Apply: {:?} => {:?}", cmd, ret);
        assert!(postcondition(&actual_log, &cmd, &ret));
    }

    actual_log.stop();

    TestResult::passed()
}
#[test]
fn should_be_correct() {
    use quickcheck;
    use env_logger;

    env_logger::init().unwrap_or(());

    quickcheck::quickcheck(should_be_correct_prop as fn(LogCommands) -> TestResult)
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


    let mut log = L::new();
    let mut seq = None;
    let mut prepared = BTreeMap::new();
    for cmd in vals {
        trace!("apply: {:?}", cmd);
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
            LogCommand::ReadFrom(ref read_seq) => {
                let result = log.read_from(*read_seq).map(|(_, v)| v).next();
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
                                            (&mut Some(seq), LogCommand::CommitTo(ref commit))
                                                if *commit > seq => Some(LogCommand::CommitTo(seq)),
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


    let mut log = L::new();
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
            LogCommand::ReadFrom(_) => (),
        }
    }
    debug!("Stopping Log");
    log.quiesce();

    let read_commit = log.read_committed();
    let read_prepared = log.read_prepared();
    log.stop();
    debug!("Stopped Log");

    debug!("observed: {:?}; expect: {:?}", read_commit, expected_commit);
    debug!("read_commit: {:?}; read_prepared:{:?}, expect: {:?}",
           read_commit,
           read_prepared,
           expected_commit);
    assert!(read_commit <= read_prepared);
    assert!(read_commit <= expected_commit);
    assert_eq!(expected_commit, read_commit);

    TestResult::passed()
}

#[cfg(feature = "benches")]
fn bench_prepare<L: TestLog>(b: &mut Bencher) {
    let mut l = L::new(|_| ());
    let op = b"foobar!";
    b.iter(|| {
        let seq = l.seqno();
        l.prepare(seq, &*op)
    });
    l.stop();
}

mod rocksdb {
    use quickcheck::{self, TestResult};
    use vastatrix::replication_log::RocksdbLog;
    use super::{LogCommand, test_can_commit_prepared_values_prop,
                test_can_read_prepared_values_prop};
    use env_logger;
    #[cfg(feature = "benches")]
    use test::Bencher;

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

    #[cfg(feature = "benches")]
    #[bench]
    fn bench_prepare(b: &mut Bencher) {
        super::bench_prepare::<RocksdbLog>(b)
    }
}

mod vec {
    use quickcheck::{self, TestResult};
    use super::VecLog;
    use super::{LogCommand, test_can_commit_prepared_values_prop,
                test_can_read_prepared_values_prop};
    use env_logger;
    #[cfg(feature = "benches")]
    use test::Bencher;

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

    #[cfg(feature = "benches")]
    #[bench]
    fn bench_prepare(b: &mut Bencher) {
        super::bench_prepare::<VecLog>(b)
    }
}

pub fn hash<T: Hash>(t: &T) -> u64 {
    let mut s = SipHasher::new();
    t.hash(&mut s);
    s.finish()
}
