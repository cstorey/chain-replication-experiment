use etcd;
use std::thread;
use std::fmt;
use std::time::Duration;
use std::sync::Arc;
use std::collections::BTreeMap;
use rustc_serialize::json;
use rustc_serialize::{Decodable,Encodable};

pub struct ConfigClient<T> {
    client: Arc<InnerClient<T>>,
    lease_mgr: thread::JoinHandle<()>,
    watcher: thread::JoinHandle<()>,
}

#[derive(Clone,Debug, Default)]
pub struct ConfigurationView<T> {
    this_node: String,
    members: BTreeMap<String, T>,
    sequencer: u64,
}

struct InnerClient<T> {
    etcd: etcd::Client,
    data: T,
    lease_time: Duration,
    callback: Box<Fn(ConfigurationView<T>) + Send + Sync + 'static>,
    lease_key: Option<String>,
}

impl<T: 'static + Decodable + Encodable + fmt::Debug + Eq + Clone + Send + Sync> ConfigClient<T> {
    pub fn new<F: Fn(ConfigurationView<T>) + Send + Sync + 'static>(addr: &str, data: T, lease_time: Duration, callback: F)
        -> Result<ConfigClient<T>, ()> {
        let etcd = etcd::Client::new(addr).expect("etcd client");
        let mut client = InnerClient {
            etcd: etcd,
            data: data,
            lease_time: lease_time,
            callback: Box::new(callback),
            lease_key: None,
        };
        let lease = client.setup_lease();

        let client = Arc::new(client);
        let lease_mgr = { let client = client.clone();
            thread::Builder::new().name("etcd config".to_string()).spawn(move || {
                client.run_lease(lease)
            }).expect("etcd thread")
        };
        let watcher = {
            let client = client.clone();
            thread::Builder::new().name("etcd watcher".to_string()).spawn(move || {
                client.run_watch()
            }).expect("etcd watcher")
        };
        Ok(ConfigClient { client: client, lease_mgr: lease_mgr, watcher: watcher })
    }
}

const MEMBERS : &'static str = "/chain/members";
const SEQUENCER : &'static str = "/chain/config_seq";
const KEY_EXISTS : u64 = 105;
const KEY_NOT_EXISTS : u64 = 100;
const COMPARE_FAILED : u64 = 101;

#[derive(Debug,PartialEq,Eq,Default,Clone, RustcEncodable, RustcDecodable)]
struct ConfigSequencer {
    keys: Vec<String>,
    epoch: u64,
}

impl<T: Decodable + Encodable + fmt::Debug + Eq + Clone> InnerClient<T> {

    fn setup_lease(&mut self) -> u64 {
        match self.etcd.create_dir(MEMBERS, None) {
            Ok(res) => info!("Created dir: {}: {:?}: ", MEMBERS, res),
            Err(etcd::Error::Etcd (ref e)) if e.error_code == KEY_EXISTS => info!("Dir exists: {:?}: ", MEMBERS),
            Err(e) => panic!("Unexpected error creating {}: {:?}", MEMBERS, e),
        }

        let seq : ConfigSequencer = Default::default();
        match self.etcd.create(SEQUENCER, &json::encode(&seq).expect("encode sequencer"), None) {
            Ok(res) => info!("Created seq: {}: {:?}: ", SEQUENCER, res),
            Err(etcd::Error::Etcd (ref e)) if e.error_code == KEY_EXISTS => info!("Sequencer exists: {:?}: ", SEQUENCER),
            Err(e) => panic!("Unexpected error creating {}: {:?}", SEQUENCER, e),
        }

        let me = self.etcd.create_in_order(MEMBERS, &self.data_json(), Some(self.lease_time.as_secs())).expect("Create unique node");
        info!("My node! {:?}", me);
        self.lease_key = Some(me.node.key.expect("Key name"));
        me.node.modified_index.expect("Lease node version")
    }

    fn data_json(&self) -> String {
        json::encode(&self.data).expect("json encode")
    }

    fn run_lease(&self, mut lease_index: u64) {
        let pausetime = self.lease_time / 2;
        let lease_key = self.lease_key.as_ref().expect("Should have created lease node");

        loop {

            thread::sleep(pausetime);
            trace!("touch node: {:?}={:?}", lease_key, self.data);
            let res = self.etcd.compare_and_swap(&lease_key, &self.data_json(),
                    Some(self.lease_time.as_secs()), None, Some(lease_index))
                .expect("Update lease");
            trace!("Update: {:?}", res);
            lease_index = res.node.modified_index.expect("lease version");
        }
    }

    fn list_members(&self) -> BTreeMap<String, T> {
        let current_listing = self.etcd.get(MEMBERS, true, true, true).expect("List members");
        trace!("Listing: {:?}", current_listing);
        current_listing.node.nodes.expect("Node listing").into_iter()
                .filter_map(|n|
                    if let (Some(k), Some(v)) = (n.key, n.value) {
                        Some((k, json::decode(&v).expect("decode json"))) } else { None })
                .collect::<BTreeMap<String, T>>()
    }

    fn run_watch(&self) {
        info!("Starting etcd watcher");
        let lease_key = self.lease_key.as_ref().expect("Should have created lease node");

        let current_listing = self.etcd.get(MEMBERS, true, true, true).expect("List members");
        debug!("Listing: {:?}", current_listing);
        let mut last_observed_index = current_listing.node.nodes.unwrap_or_else(|| Vec::new()).into_iter()
            .filter_map(|x| x.modified_index).max();

        let mut curr_members = BTreeMap::new();
        loop {
            trace!("Awaiting for {} from {:?}", MEMBERS, last_observed_index);
            let res = self.etcd.watch(MEMBERS, last_observed_index, true).expect("watch");
            trace!("Watch: {:?}", res);
            last_observed_index = res.node.modified_index.map(|x| x+1);

            let members = self.list_members();
            assert!(members.contains_key(lease_key), "I am ostensibly alive");

            let seq = self.verify_sequencer(&members);

            trace!("Members: {:?}; seq: {:?}", members, seq);
            if curr_members != members {
                curr_members = members;
                info!("Membership change! {:?}", curr_members);
                (self.callback)(ConfigurationView {
                    this_node: lease_key.clone(),
                    members: curr_members.clone(),
                    sequencer: seq.epoch,
                })
            }
        }

    }

    fn verify_sequencer(&self, members: &BTreeMap<String, T>) -> ConfigSequencer {
        loop {
            let (seq_index, mut seq) : (u64, ConfigSequencer) = match self.etcd.get(SEQUENCER, false, false, false) {
                Ok(etcd::KeySpaceInfo {
                    node: etcd::keys::Node {
                        modified_index: Some(modified_index),
                        value: Some(value),
                        ..
                    } ,
                    ..
                }) => (modified_index, json::decode(&value).expect("decode sequencer")),
                Ok(e) => panic!("Unexpected response reading {}: {:?}", SEQUENCER, e),
                Err(e) => panic!("Unexpected error reading {}: {:?}", SEQUENCER, e),
            };
            trace!("Sequencer: {}/{:?}", seq_index, seq);
            let current_keys = members.keys().cloned().collect::<Vec<_>>();
            if current_keys != seq.keys {
                debug!("Stale! {:?}", seq);

                seq.keys = current_keys;
                seq.epoch += 1;
                match self.etcd.compare_and_swap(SEQUENCER, &json::encode(&seq).expect("encode sequencer"), None, None, Some(seq_index)) {
                        Ok(res) => {
                            debug!("verify_sequencer: Updated seq: {}: {:?}: ", SEQUENCER, seq);
                            return seq;
                        },
                        Err(etcd::Error::Etcd (ref e)) if e.error_code == COMPARE_FAILED => debug!("verify_sequencer: Raced out; retry"),
                        Err(e) => panic!("Unexpected error creating {}: {:?}", SEQUENCER, e),
                }
            } else {
                debug!("verify_sequencer: Fresh! {:?}", seq);
                return seq;
            }
        }
    }
}

impl<T: Clone> ConfigurationView<T> {
    fn head_key(&self) -> Option<&str> {
        self.members.keys().next().map(|s| &**s)
    }

    pub fn should_listen_for_clients(&self) -> bool {
        self.head_key() == Some(&self.this_node)
    }
    pub fn should_listen_for_upstream(&self) -> bool {
        self.head_key() != Some(&self.this_node)
    }
    pub fn should_connect_downstream(&self) -> Option<T> {
        let next = self.members.iter().filter_map(|(k, v)| if *k > self.this_node { Some((k, v)) } else { None }).next();
        next.map(|(_, val)| val.clone())
    }
}
