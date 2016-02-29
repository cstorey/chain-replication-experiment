use std::fmt;
use rocksdb::{DB, Writable, Options, WriteBatch, WriteOptions};
use rocksdb::ffi::DBCFHandle;
use tempdir::TempDir;
use byteorder::{ByteOrder, BigEndian};
use spki_sexp;
use super::Operation;
use time::{Duration, PreciseTime};
use std::thread;
use std::sync::mpsc;
use std::sync::Arc;

pub struct Log {
    dir: TempDir,
    db: Arc<DB>,
    meta: DBCFHandle,
    data: DBCFHandle,
    flush_tx: mpsc::SyncSender<u64>,
}

const META: &'static str = "meta";
const DATA: &'static str = "data";
const META_PREPARED: &'static str = "prepared";
const META_COMMITTED: &'static str = "committed";

impl fmt::Debug for Log {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("Log").field("dir", &self.dir.path()).finish()
    }
}

impl Log {
    pub fn new() -> Log {
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
                .spawn(move || Self::flush_thread_loop(db, flush_rx))
                .expect("spawn flush thread")
        };
        Log {
            dir: d,
            db: db,
            meta: meta,
            data: data,
            flush_tx: flush_tx,
        }
    }

    fn flush_thread_loop(db: Arc<DB>, rx: mpsc::Receiver<u64>) {
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
            Self::do_commit_to(db.clone(), meta, data, seqno)
        }
    }

    fn tokey(seq: u64) -> [u8; 8] {
        let mut buf: [u8; 8] = [0, 0, 0, 0, 0, 0, 0, 0];
        BigEndian::write_u64(&mut buf, seq);
        buf
    }

    fn fromkey(key: &[u8]) -> u64 {
        BigEndian::read_u64(&key)
    }

    pub fn seqno(&self) -> u64 {
        let current = self.read_seqno(META_PREPARED);
        let next = current + 1;
        next
    }

    fn do_read_seqno(db: &DB, meta: DBCFHandle, name: &str) -> u64 {
        match db.get_cf(meta, name.as_bytes()) {
            Ok(Some(val)) => Self::fromkey(&val),
            Ok(None) => 0,
            Err(e) => panic!("Unexpected error getting commit point: {:?}", e),
        }
    }

    fn read_seqno(&self, name: &str) -> u64 {
        Self::do_read_seqno(&self.db, self.meta, name)
    }

    pub fn read(&self, seqno: u64) -> Option<Operation> {
        let key = Self::tokey(seqno);
        let ret = match self.db.get_cf(self.data, &key.as_ref()) {
            Ok(Some(val)) => Some(spki_sexp::from_bytes(&val).expect("decode operation")),
            Ok(None) => None,
            Err(e) => panic!("Unexpected error reading index: {:?}: {:?}", seqno, e),
        };
        debug!("Read: {:?} => {:?}", seqno, ret);
        ret
    }


    pub fn verify_sequential(&self, seqno: u64) -> bool {
        let current = self.seqno();
        if !(seqno == 0 || current <= seqno) {
            warn!("Hole in history: saw {}; have: {:?}", seqno, current);
            false
        } else {
            true
        }
    }

    pub fn prepare(&mut self, seqno: u64, op: &Operation) {
        let data_bytes = spki_sexp::as_bytes(op).expect("encode operation");
        debug!("Prepare {:?}", seqno);
        let key = Self::tokey(seqno);
        // match self.db.get_cf(self.data, &key.as_ref()) {
        // Ok(None) => (),
        // Err(e) => panic!("Unexpected error reading index: {:?}: {:?}", seqno, e),
        // Ok(_) => panic!("Unexpected entry at seqno: {:?}", seqno),
        // };

        let t0 = PreciseTime::now();
        let mut batch = WriteBatch::new();
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

    pub fn commit_to(&mut self, seqno: u64) {
        debug!("Request commit upto: {:?}", seqno);
        self.flush_tx.send(seqno).expect("Send to flusher")
    }

    fn do_commit_to(db: Arc<DB>, meta: DBCFHandle, data: DBCFHandle, commit_seqno: u64) {
        debug!("Commit {:?}", commit_seqno);
        let key = Self::tokey(commit_seqno);

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
        debug!("Committed {:?} in: {}", commit_seqno - committed, t0.to(t1));
    }

    pub fn flush(&mut self) -> bool {
        let prepared = self.read_seqno(META_PREPARED);
        let committed = self.read_seqno(META_COMMITTED);
        if committed < prepared {
            info!("Flushing {} ({:?}->{:?})",
                  prepared - committed,
                  committed,
                  prepared);
            self.commit_to(prepared);
            true
        } else {
            false
        }
    }
}
