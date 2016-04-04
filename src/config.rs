use etcd;
use std::thread;
use std::sync::atomic::{Ordering, AtomicBool};
use std::fmt;
use std::sync::Arc;
use std::collections::BTreeMap;
use time::{SteadyTime, Duration};
use serde_json as json;
use serde::ser::Serialize;
use serde::de::Deserialize;

#[cfg(feature = "serde_macros")]
include!("config_data.rs.in");

#[cfg(not(feature = "serde_macros"))]
include!(concat!(env!("OUT_DIR"), "/config_data.rs"));

impl ConfigSequencer {
    fn update_from_keys(&mut self, current_keys: Vec<String>) {
        self.keys = current_keys;
        self.epoch = self.epoch.succ();
    }
}

pub struct ConfigClient<T> {
    client: Arc<InnerClient<T>>,
    #[allow(dead_code)]
    lease_mgr: thread::JoinHandle<()>,
    #[allow(dead_code)]
    watcher: thread::JoinHandle<()>,
}

#[derive(Clone,Debug, Default)]
pub struct ConfigurationView<T> {
    pub epoch: Epoch,
    ord: usize,
    next: Option<T>,
}

struct InnerClient<T> {
    etcd: etcd::Client,
    data: T,
    lease_time: Duration,
    callback: Box<Fn(ConfigurationView<T>) + Send + Sync + 'static>,
    on_exit: Box<Fn() + Send + Sync + 'static>,
    lease_key: Option<String>,
    lease_alive: AtomicBool,
    watcher_alive: AtomicBool,
}

impl Epoch {
    fn succ(&self) -> Epoch {
        Epoch(self.0 + 1)
    }
}

struct DeathWatch<'a, F>(&'a AtomicBool, F) where F: Fn();


impl<'a, F> DeathWatch<'a, F>
    where F: Fn()
{
    fn new(ctr: &'a AtomicBool, f: F) -> DeathWatch<'a, F> {
        ctr.store(true, Ordering::SeqCst);
        DeathWatch(ctr, f)
    }
    fn is_alive(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }
}
impl<'a, F> Drop for DeathWatch<'a, F>
    where F: Fn()
{
    fn drop(&mut self) {
        warn!("Exiting: {:?}", self);
        let &mut DeathWatch(ref alivep, ref mut cb) = self;
        alivep.store(false, Ordering::Relaxed);
        (cb)();
    }
}

impl<'a, F> fmt::Debug for DeathWatch<'a, F>
    where F: Fn()
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("DeathWatch").field("alive", &self.is_alive()).finish()
    }
}

impl<T: 'static + Deserialize + Serialize + fmt::Debug + Eq + Clone + Send + Sync> ConfigClient<T> {
    pub fn new<F: Fn(ConfigurationView<T>) + Send + Sync + 'static, S: Fn() + Send + Sync + 'static>
        (addr: &str,
         data: T,
         lease_time: Duration,
         callback: F,
         on_exit: S)
         -> Result<ConfigClient<T>, ()> {
        let etcd = etcd::Client::new(&[addr]).expect("etcd client");
        let mut client = InnerClient {
            etcd: etcd,
            data: data,
            lease_time: lease_time,
            callback: Box::new(callback),
            on_exit: Box::new(on_exit),
            lease_key: None,
            lease_alive: AtomicBool::new(true),
            watcher_alive: AtomicBool::new(true),
        };
        let lease = client.setup_lease();

        let client = Arc::new(client);
        let lease_mgr = {
            let client = client.clone();
            thread::Builder::new()
                .name("etcd-config".to_string())
                .spawn(move || client.run_lease(lease))
                .expect("etcd thread")
        };
        let watcher = {
            let client = client.clone();
            thread::Builder::new()
                .name("etcd-watcher".to_string())
                .spawn(move || client.run_watch())
                .expect("etcd watcher")
        };
        Ok(ConfigClient {
            client: client,
            lease_mgr: lease_mgr,
            watcher: watcher,
        })
    }

    pub fn is_running(&self) -> bool {
        self.client.is_running()
    }
}

const MEMBERS: &'static str = "/chain/members";
const SEQUENCER: &'static str = "/chain/config_seq";
const KEY_EXISTS: u64 = 105;
const KEY_NOT_EXISTS: u64 = 100;
const COMPARE_FAILED: u64 = 101;

fn has_error(es: &[etcd::Error], code: u64) -> bool {
    es.iter()
        .filter_map(|e| match e { &etcd::Error::Api(ref e) => Some(e), _ => None })
        .any(|e| e.error_code == code)
}

impl<T: Deserialize + Serialize + fmt::Debug + Eq + Clone> InnerClient<T> {
    fn setup_lease(&mut self) -> u64 {
        match self.etcd.create_dir(MEMBERS, None) {
            Ok(res) => info!("Created dir: {}: {:?}: ", MEMBERS, res),
            Err(ref e) if has_error(e, KEY_EXISTS) => info!("Dir exists: {:?}: ", MEMBERS),
            Err(e) => panic!("Unexpected error creating {}: {:?}", MEMBERS, e)
        }

        let seq: ConfigSequencer = Default::default();
        match self.etcd.create(SEQUENCER,
                               &json::to_string(&seq).expect("encode epoch"),
                               None) {
            Ok(res) => info!("Created seq: {}: {:?}: ", SEQUENCER, res),
            Err(ref es) if has_error(es, KEY_EXISTS) => info!("Sequencer exists: {:?}: ", SEQUENCER),
            Err(e) => panic!("Unexpected error creating {}: {:?}", SEQUENCER, e),
        }

        let me = self.etcd
                     .create_in_order(MEMBERS,
                                      &self.data_json(),
                                      Some(self.lease_time.num_seconds() as u64))
                     .expect("Create unique node");
        info!("My node! {:?}", me);
        let node = me.node.expect("lease node");
        self.lease_key = Some(node.key.expect("Key name"));
        node.modified_index.expect("Lease node version")
    }

    fn data_json(&self) -> String {
        json::to_string(&self.data).expect("json encode")
    }

    fn run_lease(&self, mut lease_index: u64) {
        fn std_time(t: ::time::Duration) -> ::std::time::Duration {
            ::std::time::Duration::from_millis(t.num_milliseconds() as u64)
        }

        let _watch = DeathWatch::new(&self.lease_alive, || (*self.on_exit)());
        let update_interval = self.lease_time / 2;
        let lease_key = self.lease_key.as_ref().expect("Should have created lease node");

        loop {
            trace!("touch node: {:?}={:?}", lease_key, self.data);
            let start_time = SteadyTime::now();
            let next_wakeup = start_time + update_interval;
            let res = match self.etcd
                                .compare_and_swap(&lease_key,
                                                  &self.data_json(),
                                                  Some(self.lease_time.num_seconds() as u64),
                                                  None,
                                                  Some(lease_index)) {
                Ok(res) => {
                    trace!("refreshed lease:: {}: {:?}: ",
                           lease_key,
                           res.node.as_ref().map(|n| n.modified_index));
                    res
                }
                /* Err(etcd::Error::Etcd(ref e)) if e.error_code == KEY_NOT_EXISTS => {
                    panic!("Could not update lease: expired");
                } */
                Err(e) => panic!("Unexpected error updating lease {}: {:?}", lease_key, e),
            };
            lease_index = res.node.as_ref().expect("lease node").modified_index.expect("lease version");
            let end_time = SteadyTime::now();
            trace!("Updated in {}: {:?}", end_time - start_time, res);
            let pausetime = next_wakeup - end_time;
            trace!("Pause for {}", pausetime);
            if pausetime > Duration::zero() {
                thread::sleep(std_time(pausetime));
            }
        }
    }

    fn list_members(&self) -> BTreeMap<String, T> {
        let current_listing = self.etcd.get(MEMBERS, true, true, true).expect("List members");
        trace!("Listing: {:?}", current_listing);
        current_listing.node.expect("lease dir")
                       .nodes
                       .unwrap_or_else(|| Vec::new())
                       .into_iter()
                       .filter_map(|n| {
                           if let (Some(k), Some(v)) = (n.key, n.value) {
                               Some((k, json::from_str(&v).expect("decode json")))
                           } else {
                               None
                           }
                       })
                       .collect::<BTreeMap<String, T>>()
    }

    fn run_watch(&self) {
        let _watch = DeathWatch::new(&self.watcher_alive, || (*self.on_exit)());
        info!("Starting etcd watcher");
        let lease_key = self.lease_key.as_ref().expect("Should have created lease node");

        let current_listing = self.etcd.get(MEMBERS, true, true, true).expect("List members");
        debug!("Listing: {:?}", current_listing);
        let mut last_observed_index = current_listing.node.expect("lease node")
                                                     .nodes
                                                     .unwrap_or_else(|| Vec::new())
                                                     .into_iter()
                                                     .filter_map(|x| x.modified_index)
                                                     .max();

        let mut curr_members = BTreeMap::new();
        loop {
            trace!("Awaiting for {} from etcd index {:?}", MEMBERS, last_observed_index);
            let res = self.etcd.watch(MEMBERS, last_observed_index, true).expect("watch");
            trace!("Watch: {:?}", res);
            last_observed_index = res.node.expect("lease node").modified_index.map(|x| x + 1);

            let members = self.list_members();
            let seq = self.verify_epoch(&members);

            trace!("Members: {:?}; seq: {:?}", members, seq);
            if curr_members != members {
                curr_members = members;
                info!("Membership change! {:?}", curr_members);
                if let Some(view) = ConfigurationView::of_membership(seq.epoch,
                                                                     lease_key,
                                                                     curr_members.clone()) {
                    (self.callback)(view)
                } else {
                    warn!("I seem to have died: {:?}", lease_key);
                    (self.on_exit)();
                    break;
                }
            }
        }

    }

    fn verify_epoch(&self, members: &BTreeMap<String, T>) -> ConfigSequencer {
        loop {
            let (seq_index, mut seq): (u64, ConfigSequencer) = match self.etcd.get(SEQUENCER,
                                                                                   false,
                                                                                   false,
                                                                                   false) {
                Ok(etcd::KeySpaceInfo {
                    node: Some(etcd::Node {
                        modified_index: Some(modified_index),
                        value: Some(value),
                        ..
                    }),
                    ..
                }) => {
                    (modified_index,
                     json::from_str(&value).expect("decode epoch"))
                }
                Ok(e) => panic!("Unexpected response reading {}: {:?}", SEQUENCER, e),
                Err(e) => panic!("Unexpected error reading {}: {:?}", SEQUENCER, e),
            };
            trace!("Sequencer: {}/{:?}", seq_index, seq);
            let current_keys = members.keys().cloned().collect::<Vec<_>>();
            if current_keys != seq.keys {
                debug!("verify_epoch: Stale! {:?}", seq);

                seq.update_from_keys(current_keys);

                match self.etcd.compare_and_swap(SEQUENCER,
                                                 &json::to_string(&seq).expect("encode epoch"),
                                                 None,
                                                 None,
                                                 Some(seq_index)) {
                    Ok(_) => {
                        debug!("verify_epoch: Updated seq: {}: {:?}: ", SEQUENCER, seq);
                        return seq;
                    }
                    Err(ref es) if has_error(es, COMPARE_FAILED) => debug!("verify_epoch: Raced out; retry"),
                    Err(e) => panic!("Unexpected error creating {}: {:?}", SEQUENCER, e),
                }
            } else {
                debug!("verify_epoch: Fresh! {:?}", seq);
                return seq;
            }
        }
    }

    fn is_running(&self) -> bool {
        self.lease_alive.load(Ordering::Relaxed) && self.watcher_alive.load(Ordering::Relaxed)
    }
}

impl<T: Clone> ConfigurationView<T> {
    fn of_membership(epoch: Epoch,
                     this_node: &str,
                     members: BTreeMap<String, T>)
                     -> Option<ConfigurationView<T>> {
        let ordp = members.keys().position(|x| *x == this_node);
        ordp.map(|ord| {
            let next = members.values().nth(ord + 1).map(|v| v.clone());
            ConfigurationView {
                epoch: epoch,
                ord: ord,
                next: next,
            }
        })
    }

    pub fn is_head(&self) -> bool {
        self.ord == 0
    }

    pub fn should_listen_for_clients(&self) -> bool {
        self.is_head()
    }
    pub fn should_listen_for_upstream(&self) -> bool {
        !self.is_head()
    }

    pub fn should_connect_downstream<'a>(&'a self) -> Option<&'a T> {
        self.next.as_ref()
    }
}
