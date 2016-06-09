use std::fmt;
use std::io;
use rocksdb::{DB, DBIterator, Direction, IteratorMode, Options, Writable, WriteBatch, WriteOptions};
use rocksdb::ffi::DBCFHandle;
use tempdir::TempDir;
use data::Seqno;
use replica::Log;
use time::PreciseTime;
use std::sync::Arc;
use std::iter;

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

error_chain! {
    types {
        Error, ErrorKind, ChainErr, Result;
    }
    links {}
    foreign_links {
        io::Error, Io, "I/O error";
    }
    errors {
        BadSequence(saw: Seqno, expected: Seqno)
        BadCommit(saw: Seqno, expected: Seqno)
        MissingCf(cf: String)
    }
}


pub struct RocksdbLog {
    dir: TempDir,
    db: Arc<DB>,
    seqno_prepared: Option<Seqno>,
}

pub struct RocksdbCursor(DBIterator);

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
    pub fn new() -> Result<RocksdbLog> {
        let d = try!(TempDir::new("rocksdb-log"));
        info!("DB path: {:?}", d.path());
        let mut db = try!(DB::open_default(&d.path().to_string_lossy()));
        let _meta = try!(db.create_cf(META, &Options::new()));
        let _data = try!(db.create_cf(DATA, &Options::new()));
        let db = Arc::new(db);

        let seqno_prepared = try!(Self::do_read_seqno(&db, META_PREPARED));
        Ok(RocksdbLog {
            dir: d,
            db: db,
            seqno_prepared: seqno_prepared,
        })
    }

    fn meta(db: &DB) -> Result<DBCFHandle> {
        db.cf_handle(META).map(Clone::clone).ok_or(ErrorKind::MissingCf(META.to_string()).into())
    }

    fn data(db: &DB) -> Result<DBCFHandle> {
        db.cf_handle(DATA).map(Clone::clone).ok_or(ErrorKind::MissingCf(DATA.to_string()).into())
    }

    fn do_read_seqno(db: &DB, name: &str) -> Result<Option<Seqno>> {
        match try!(db.get_cf(try!(Self::meta(&db)), name.as_bytes())) {
            Some(val) => Ok(Some(Seqno::fromkey(&val))),
            None => Ok(None),
        }
    }

    fn read_seqno(&self, name: &str) -> Result<Option<Seqno>> {
        Self::do_read_seqno(&self.db, name)
    }

    fn do_commit_to(db: &DB, commit_seqno: Seqno) -> Result<()> {
        debug!("Commit {:?}", commit_seqno);
        let meta = try!(Self::meta(&db));
        let key = Seqno::tokey(&commit_seqno);

        let prepared = try!(Self::do_read_seqno(&db, META_PREPARED));
        let committed = try!(Self::do_read_seqno(&db, META_COMMITTED));

        debug!("Committing: {:?}, committed, {:?}, prepared: {:?}",
               committed,
               committed,
               prepared);

        if let Some(p) = prepared {
            if p < commit_seqno {
                return Err(ErrorKind::BadCommit(p, commit_seqno).into());
            }
        }

        if committed == Some(commit_seqno) {
            debug!("Skipping, commits up to date");
            return Ok(());
        }

        let t0 = PreciseTime::now();
        let mut opts = WriteOptions::new();
        opts.set_sync(true);
        db.put_cf_opt(meta, META_COMMITTED.as_bytes(), &key.as_ref(), &opts)
          .expect("Persist commit point");
        let t1 = PreciseTime::now();
        debug!("Committed {:?} in: {}", commit_seqno, t0.to(t1));
        Ok(())
    }

    pub fn stop(self) {
        let RocksdbLog { db, .. } = self;
        drop(db);
    }

    pub fn quiesce(&self) {}
}

impl Log for RocksdbLog {
    type Cursor = RocksdbCursor;

    fn seqno(&self) -> Result<Seqno> {
        let prepared = try!(self.read_prepared());
        Ok(prepared.as_ref().map(Seqno::succ).unwrap_or_else(Seqno::zero))
    }

    fn read_prepared(&self) -> Result<Option<Seqno>> {
        Ok(self.seqno_prepared.clone())
    }

    fn read_committed(&self) -> Result<Option<Seqno>> {
        self.read_seqno(META_COMMITTED)
    }

    fn read_from(&self, seqno: Seqno) -> Result<RocksdbCursor> {
        let key = Seqno::tokey(&seqno);
        let datacf = try!(Self::data(&self.db));
        let iter = try!(self.db
                            .iterator_cf(datacf,
                                         IteratorMode::From(&key.as_ref(), Direction::Forward)));
        Ok(RocksdbCursor(iter))
    }


    fn prepare(&mut self, seqno: Seqno, data_bytes: &[u8]) -> Result<()> {
        trace!("Prepare {:?}", seqno);
        let key = Seqno::tokey(&seqno);

        let next = try!(self.seqno());
        if seqno != next {
            return Err(ErrorKind::BadSequence(seqno, next).into());
        }

        // match self.db.get_cf(Self::data(&db), &key.as_ref()) {
        // Ok(None) => (),
        // Err(e) => panic!("Unexpected error reading index: {:?}: {:?}", seqno, e),
        // Ok(_) => panic!("Unexpected entry at seqno: {:?}", seqno),
        // };

        let t0 = PreciseTime::now();
        let batch = WriteBatch::new();
        try!(batch.put_cf(try!(Self::data(&self.db)), &key.as_ref(), &data_bytes));
        try!(batch.put_cf(try!(Self::meta(&self.db)),
                          META_PREPARED.as_bytes(),
                          &key.as_ref()));
        try!(self.db.write(batch));
        let t1 = PreciseTime::now();
        trace!("Prepare: {}", t0.to(t1));
        self.seqno_prepared = Some(seqno);

        trace!("Watermarks: prepared: {:?}; committed: {:?}",
               self.read_seqno(META_PREPARED),
               self.read_seqno(META_COMMITTED));
        Ok(())
    }

    fn commit_to(&mut self, seqno: Seqno) -> Result<bool> {
        trace!("Request commit upto: {:?}", seqno);
        let committed = try!(self.read_seqno(META_COMMITTED));
        if committed.map(|c| c < seqno).unwrap_or(true) {
            trace!("Request to commit {:?} -> {:?}", committed, seqno);
            try!(Self::do_commit_to(&self.db, seqno));
            Ok(true)
        } else {
            trace!("Request to commit {:?} -> {:?}; no-op", committed, seqno);
            Ok(false)
        }
    }
}

impl iter::Iterator for RocksdbCursor {
    type Item = (Seqno, Vec<u8>);
    fn next(&mut self) -> Option<Self::Item> {
        let &mut RocksdbCursor(ref mut iter) = self;
        if let Some((key, val)) = iter.next() {
            let seqno = Seqno::fromkey(&key);
            Some((seqno, val.to_vec()))
        } else {
            None
        }
    }
}
