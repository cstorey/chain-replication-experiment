use std::fmt;
use std::io;
use lmdb_zero::{self, ConstAccessor, Database, EnvBuilder, Environment, ReadTransaction,
                WriteTransaction, error, open, put};

use hex_slice::AsHex;
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
        lmdb_zero::Error, Lmdb, "Db error";
    }
    errors {
        BadSequence(saw: Seqno, expected: Seqno)
        BadCommit(saw: Seqno, expected: Seqno)
        MissingCf(cf: String)
    }
}

// 1TGB. That'll be enough, right?
const ARBITARILY_LARGE: usize = 1 << 24;

pub struct RocksdbLog {
    dir: TempDir,
    env: Arc<Environment>,
    seqno_prepared: Option<Seqno>,
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

fn open_db<'a>(env: &'a Environment, name: &str) -> Result<Database<'a>> {
    let db = try!(Database::open(env,
                                 Some(name),
                                 &lmdb_zero::DatabaseOptions::new(lmdb_zero::db::CREATE)));
    Ok(db)
}

impl RocksdbLog {
    pub fn new() -> Result<RocksdbLog> {
        let d = try!(TempDir::new("rocksdb-log"));
        info!("DB path: {:?}", d.path());
        let mut b = try!(EnvBuilder::new());
        try!(b.set_maxdbs(3));
        try!(b.set_mapsize(ARBITARILY_LARGE));
        let env = unsafe {
            try!(b.open(d.path().to_str().expect("string"),
                        open::Flags::empty(),
                        0o777))
        };

        let seqno_prepared = {
            let meta = try!(open_db(&env, META));
            let _ = try!(open_db(&env, DATA));

            let txn = try!(ReadTransaction::new(&env));
            try!(Self::do_read_seqno(&meta, &txn.access(), META_PREPARED))
        };
        Ok(RocksdbLog {
            dir: d,
            env: Arc::new(env),
            seqno_prepared: seqno_prepared,
        })
    }

    fn meta(env: &Environment) -> Result<Database> {
        Ok(try!(open_db(&env, META)))
    }

    fn data(env: &Environment) -> Result<Database> {
        Ok(try!(open_db(&env, DATA)))
    }

    fn do_read_seqno(meta: &Database, txn: &ConstAccessor, name: &str) -> Result<Option<Seqno>> {
        let val = {
            let val = match txn.get(meta, name) {
                Ok(val) => Some(Seqno::fromkey(val)),
                Err(e) if e.code == error::NOTFOUND => None,
                Err(e) => return Err(e.into()),
            };
            val
        };

        Ok(val)
    }


    fn read_seqno(&self, name: &str) -> Result<Option<Seqno>> {
        let meta = Self::meta(&self.env);
        let txn = try!(ReadTransaction::new(&self.env));
        Self::do_read_seqno(&try!(meta), &txn.access(), name)
    }

    fn do_commit_to(env: &Environment, commit_seqno: Seqno) -> Result<()> {
        debug!("Commit {:?}", commit_seqno);
        let meta = try!(Self::meta(&env));
        let key = Seqno::tokey(&commit_seqno);

        let t0 = PreciseTime::now();
        let txn = try!(WriteTransaction::new(&env));

        {
            let mut accessor = txn.access();
            let prepared = try!(Self::do_read_seqno(&meta, &accessor, META_PREPARED));
            let committed = try!(Self::do_read_seqno(&meta, &accessor, META_COMMITTED));

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

            try!(accessor.put(&meta, META_COMMITTED.as_bytes(), &key, put::Flags::empty()));
        }
        try!(txn.commit());
        let t1 = PreciseTime::now();
        debug!("Committed {:?} in: {}", commit_seqno, t0.to(t1));
        Ok(())
    }

    pub fn stop(self) {}

    pub fn quiesce(&self) {}
}

impl Log for RocksdbLog {
    type Cursor = RocksdbCursor;
    type Error = Error;

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
        Ok(RocksdbCursor(self.env.clone(), seqno))
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
        let data = try!(Self::data(&self.env));
        let meta = try!(Self::meta(&self.env));
        let txn = try!(WriteTransaction::new(&self.env));
        {
            let mut access = txn.access();
            try!(access.put(&data, &key, data_bytes, put::NOOVERWRITE));
            try!(access.put(&meta, META_PREPARED, &key, put::Flags::empty()));
        }
        try!(txn.commit());
        drop(meta);
        drop(data);
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
            try!(Self::do_commit_to(&self.env, seqno));
            Ok(true)
        } else {
            trace!("Request to commit {:?} -> {:?}; no-op", committed, seqno);
            Ok(false)
        }
    }
}

#[derive(Debug)]
pub struct RocksdbCursor(Arc<Environment>, Seqno);

impl RocksdbCursor {
    fn read_next(&mut self) -> Result<Option<(Seqno, Vec<u8>)>> {
        let &mut RocksdbCursor(ref env, ref mut seqno) = self;
        let data = try!(RocksdbLog::data(&env));
        let txn = try!(ReadTransaction::new(&env));
        let key = Seqno::tokey(&seqno);
        let mut cursor = try!(txn.cursor(&data).chain_err(|| "get cursor"));
        debug!("Attempt read from: {:?}/{:x}", seqno, key.as_hex());
        let ret = match try!(mdb_maybe(cursor.seek_range_k::<[u8], [u8]>(&txn.access(), &key))) {
            Some((k, v)) => {
                let read_seq = Seqno::fromkey(k);
                debug!("Read from: {:?}/{:x}", read_seq, key.as_hex());
                *seqno = read_seq.succ();
                (read_seq, v.to_vec())
            }
            None => return Ok(None),
        };

        Ok(Some(ret))
        // if let Some((key, val)) = iter.next() {
        // let seqno = Seqno::fromkey(&key);
        // Some((seqno, val.to_vec()))
        // } else {
        // None
        // }
        //
    }
}
impl iter::Iterator for RocksdbCursor {
    type Item = Result<(Seqno, Vec<u8>)>;
    fn next(&mut self) -> Option<Self::Item> {
        return self.read_next()
                   .map(|ot| ot.map(Ok))
                   .unwrap_or_else(|e| Some(Err(e)));
    }
}


fn mdb_maybe<T>(res: ::std::result::Result<T, lmdb_zero::Error>)
                -> ::std::result::Result<Option<T>, lmdb_zero::Error> {
    match res {
        Ok(kv) => Ok(Some(kv)),
        Err(e) if e.code == error::NOTFOUND => Ok(None),
        Err(e) => Err(e),
    }
}
