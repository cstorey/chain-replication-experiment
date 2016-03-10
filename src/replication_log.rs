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
        let data = db.cf_handle(DATA).expect("open data cf").clone();
        loop {
            debug!("Awaiting flush");
            let mut seqno = rx.recv().expect("recive flush seqno");
            loop {
                match rx.try_recv() {
                    Ok(n) => seqno = n,
                    Err(_) => break,
                }
            }
            debug!("Flushing: {:?}", seqno);
            Self::do_commit_to(db.clone(), meta, seqno);
            debug!("Flushed: {:?}", seqno);
            committed(seqno);
        }
    }
    fn do_read_seqno(db: &DB, meta: DBCFHandle, name: &str) -> Seqno {
        match db.get_cf(meta, name.as_bytes()) {
            Ok(Some(val)) => Seqno::fromkey(&val),
            Ok(None) => Seqno::none(),
            Err(e) => panic!("Unexpected error getting commit point: {:?}", e),
        }
    }

    fn read_seqno(&self, name: &str) -> Seqno {
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
        assert!(prepared >= commit_seqno);

        if committed == commit_seqno {
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
        if committed < prepared {
            info!("Flushing ({:?}->{:?})", committed, prepared);
            self.commit_to(prepared);
            true
        } else {
            false
        }
    }
}

impl Log for RocksdbLog {
    fn seqno(&self) -> Seqno {
        let current = self.read_prepared();
        current.succ()
    }

    fn read_prepared(&self) -> Seqno {
        self.read_seqno(META_PREPARED)
    }

    fn read_committed(&self) -> Seqno {
        self.read_seqno(META_PREPARED)
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
        if !(seqno.is_none() || current <= seqno) {
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
        if committed < seqno {
            debug!("Request to commit {:?} -> {:?}", committed, seqno);
            self.flush_tx.send(seqno).expect("Send to flusher");
            true
        } else {
            debug!("Request to commit {:?} -> {:?}; no-op", committed, seqno);
            false
        }
    }
}
