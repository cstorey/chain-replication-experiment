/*
Each client talks to a single consumer object, which references the shared log.

Consumer:
Client LWM -------------- Sent point -------------- Client HWM
*/

use data::Seqno;
use replica::{Log, Outputs};
use mio;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        InvalidRange(lwm: Option<Seqno>, sent: Option<Seqno>, mark: Seqno)
    }
}

#[derive(Debug)]
pub struct Consumer {
    low_water_mark: Option<Seqno>,
    sent: Option<Seqno>,
}

impl Consumer {
    pub fn new() -> Consumer {
        Consumer {
            low_water_mark: None,
            sent: None,
        }
    }

    pub fn consume_requested(&mut self, mark: Seqno) -> Result<(), Error> {
        // It would be odd for a client to request an item they have not seen
        // in the current session.
        let lwm = match (self.low_water_mark, self.sent) {
            (None, None) => Some(mark),
            (Some(m), None) if mark >= m => {
                Some(m)
            },
            (Some(m), Some(s)) if mark >= m && mark <= s => {
                Some(mark)
            },

            (None, Some(_)) => unreachable!(),
            (m, s) => return Err(Error::InvalidRange(m, s, mark)),
        };
        debug!("consume_requested: {:?}; mark:{:?} => {:?}", self, mark, lwm);
        self.low_water_mark = lwm;
        Ok(())
    }

    pub fn process<L: Log, O: Outputs>(&mut self,
            out: &mut O, token: mio::Token, log: &L) -> bool {
        let mut changed = false;
        let next = self.low_water_mark;
        let committed = log.read_committed();
        trace!("Consumer#process: committed: {:?}; next: {:?}", committed, next);

        if let (Some(next), Some(committed)) = (next, committed) {
            for (i, op) in log.read_from(next).take_while(|&(i, _)| i <= committed) {
                debug!("Consume seq:{:?}/{:?}; ds/seqno: {:?}", i, op, self.low_water_mark);
                out.consumer_message(token, i, op.into());
                self.sent = Some(i);
                self.low_water_mark = Some(i.succ());
                changed = true
            }
        }
        changed
    }
}

#[cfg(test)]
mod test {
    use consumer::Consumer;
    use data::Seqno;
    use replica::Log;
    use replica::test::{Outs,OutMessage};
    use replication_log::test::{VecLog,TestLog, LogCommand, arbitrary_given, hash};
    use slab::Index;
    use mio;
    use quickcheck::{self, Arbitrary, Gen, TestResult};
    use std::cmp;

    #[derive(Debug, Clone, Hash)]
    enum ConsOp {
        RequestMark(Seqno),
        Log(LogCommand),
        RunProcess,
    }

    impl Arbitrary for ConsOp {
        fn arbitrary<G: Gen>(g: &mut G) -> ConsOp {
            let case = u64::arbitrary(g) % 100;
            let res = match case {
                0 ... 20 => ConsOp::RequestMark(Arbitrary::arbitrary(g)),
                20 ... 50 => ConsOp::Log(Arbitrary::arbitrary(g)),
                _ => ConsOp::RunProcess,
            };
            res
        }
        fn shrink(&self) -> Box<Iterator<Item = Self> + 'static> {
            let h = hash(self);
            trace!("ConsOp#shrink {:x}: {:?}", h, self);
            match self {
                &ConsOp::RequestMark(ref s) => Box::new(s.shrink().map(ConsOp::RequestMark).inspect(move |it| trace!("ConsOp#shrink {:x}: => {:?}", h, it))),
                &ConsOp::Log(ref cmd) => Box::new(cmd.shrink().map(ConsOp::Log).inspect(move |it| trace!("ConsOp#shrink {:x}: => {:?}", h, it))),
                &ConsOp::RunProcess => Box::new(vec![].into_iter().inspect(move |it| debug!("ConsOp#shrank {:x}: => {:?}", h, it))),
            }
        }
    }

    impl ConsOp {
        pub fn apply_to<L: Log>(&self, model: &mut L) {
            match self {
                &ConsOp::Log(ref c) => c.apply_to(model),
                _ => (),
            }
        }

        fn satisfies_precondition(&self, model: &VecLog) -> bool {
            let ret = match self {
                &ConsOp::Log(ref c) => c.satisfies_precondition(model),
                &ConsOp::RequestMark(m) => Some(m) <= model.read_committed(),
                _ => true,
            };
            trace!("ConsOp#satisfies_precondition: {:?}/{:?} -> {:?}", self, model, ret);
            ret
        }
    }

    #[derive(Debug, Clone, Hash)]
    struct Commands(Vec<ConsOp>);

    impl Commands {
        fn validate_commands(&self) -> bool {
            let (_, okayp) = self.0.iter().fold((VecLog::new(move |_| ()), true),
                    |(model_log, okayp), cmd| {
                if !okayp {
                    (model_log, false)
                } else  {
                    let okayp = cmd.satisfies_precondition(&model_log);
                    (model_log, okayp)
                }
            });

            okayp
        }
    }

    impl Arbitrary for Commands {
        fn arbitrary<G: Gen>(g: &mut G) -> Commands {
            let sz = usize::arbitrary(g);
            let mut commands : Vec<ConsOp> = Vec::with_capacity(sz);
            let mut model_log = VecLog::new(move |seq| ());

            for _ in 0..sz {
                let cmd = arbitrary_given(g, |cmd: &ConsOp| cmd.satisfies_precondition(&model_log));
                debug!("Generated command: {:?}", cmd);
                cmd.apply_to(&mut model_log);
                commands.push(cmd);
            };
            Commands(commands)
        }
        fn shrink(&self) -> Box<Iterator<Item = Self> + 'static> {
            let h = hash(self);
            trace!("Commands#shrink {:x}: {:?}", h, self);
            let ret = Arbitrary::shrink(&self.0).map(Commands).filter(Commands::validate_commands).inspect(move |it| trace!("Commands#shrink {:x}: => {:?}", h, it));
            Box::new(ret)
        }
    }

    fn can_totally_do_the_thing_prop<L: TestLog>(cmds: Commands) -> TestResult {
        debug!("commands {:x}: {:?}", hash(&cmds), cmds);
        let Commands(cmds) = cmds;
        let mut actual = Consumer::new();
        let mut log = VecLog::new(move |_| ());

        let token = mio::Token::from_usize(0);
        let mut observed = Outs::new();

        let mut min_seq = None;
        let mut prev_seq = None;

        for cmd in cmds {
            debug!("apply: {:?}", cmd);
            match cmd {
                ConsOp::RequestMark(s) => {
                    let res = actual.consume_requested(s);
                    min_seq = Some(min_seq.unwrap_or(s));
                    if Some(s) < prev_seq { assert!(res.is_err()) }

                    if let Some(sent) = observed.borrow().iter().rev().filter_map(|x| {
                        match x {
                            &OutMessage::ConsumerMessage(_, seq, _) => Some(seq),
                            _ => None,
                        }
                    }).next() {
                        debug!("found sent: {:?}; mark: {:?}", sent, s);
                        if s > sent { assert!(res.is_err()) }
                    }
                    prev_seq = Some(s)
                    
                },
                ConsOp::Log(cmd) => {
                    cmd.apply_to(&mut log)
                },
                ConsOp::RunProcess => {
                    actual.process(&mut observed, token, &log);
                }
            }
        }

        while actual.process(&mut observed, token, &log) { /* noop */ }

        log.quiesce();

        let expected_msgs = if let (Some(min), Some(committed)) = (min_seq, log.read_committed()) {
                log.read_from(min).take_while(|&(s, _)| s <= committed)
                .inspect(|&(s, ref v)| debug!("Read: {:?} -> {:?}", s, v))
                .map(|(s, v)| (s, hash(&v)))
                .collect::<Vec<_>>()
        } else {
            vec![]
        };

        let observed = observed.inner();
        debug!("obs: {:?}", observed);

        let consumer_msgs = observed.into_iter().filter_map(|x| match x {
            OutMessage::ConsumerMessage(_tok, seq, m) => Some((seq, hash(&m))),
            _ => None,
        }).collect::<Vec<_>>();

        debug!("Expected: {:?}", expected_msgs);
        debug!("observed: {:?}", consumer_msgs);
        assert_eq!(expected_msgs, consumer_msgs);

        TestResult::passed()
    }

    #[test]
    fn can_totally_do_the_thing() {
        use env_logger;

        env_logger::init().unwrap_or(());
        quickcheck::quickcheck(can_totally_do_the_thing_prop::<VecLog> as fn(Commands) -> TestResult);
    }
}

