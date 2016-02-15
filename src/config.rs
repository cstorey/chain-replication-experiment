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
}

struct InnerClient<T> {
    etcd: etcd::Client,
    data: T,
    lease_time: Duration,
    callback: Box<Fn(ConfigurationView<T>) + Send + Sync + 'static>,
}

impl<T: 'static + Decodable + Encodable + fmt::Debug + Eq + Clone + Send + Sync> ConfigClient<T> {
    pub fn new<F: Fn(ConfigurationView<T>) + Send + Sync + 'static>(addr: &str, data: T, lease_time: Duration, callback: F) 
        -> Result<ConfigClient<T>, ()> {
        let etcd = etcd::Client::new(addr).expect("etcd client");
        let client = Arc::new(InnerClient {
            etcd: etcd,
            data: data,
            lease_time: lease_time,
            callback: Box::new(callback),
        });
        client.setup();

        let lease_mgr = { let client = client.clone(); 
            thread::Builder::new().name("etcd config".to_string()).spawn(move || {
                client.run_lease()
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

const DIR : &'static str = "/chain";
const KEY_EXISTS : u64 = 105;

impl<T: Decodable + Encodable + fmt::Debug + Eq + Clone> InnerClient<T> {

    fn setup(&self) {
        match self.etcd.create_dir(DIR, None) { 
            Ok(res) => info!("Created dir: {}: {:?}: ", DIR, res),
            Err(etcd::Error::Etcd (ref e)) if e.error_code == KEY_EXISTS => info!("Dir exists: {:?}: ", DIR),
            Err(e) => panic!("Unexpected error creating {}: {:?}", DIR, e),
        }
    }

    fn run_lease(&self) {
        let node_id = (&self) as *const _ as usize;
        let myttl = Some(self.lease_time.as_secs());
        let pausetime = self.lease_time / 2;
        let myvalue = json::encode(&self.data).expect("json encode");
        let me = self.etcd.create_in_order(DIR, &myvalue, myttl).expect("Create unique node");
        info!("My node! {:?}", me);
        let mykey = me.node.key.expect("Key name");
        let mut index = me.node.modified_index;
        let mut next_index = me.node.modified_index.map(|x| x+1);
        
        let mut curr_members = BTreeMap::new(); 
        loop {
            let members = self.list_members();

            trace!("Members: {:?}", members);
            if curr_members != members {
                curr_members = members;
                info!("Membership change! {:?}", curr_members);
                (self.callback)(ConfigurationView { this_node: mykey.clone(), members: curr_members.clone() })
            }

            thread::sleep(pausetime);
            trace!("touch node: {:?}={:?}", mykey, myvalue);
            let res = self.etcd.compare_and_swap(&mykey, &myvalue, myttl, Some(&myvalue), index).expect("Update lease");
            trace!("Update: {:?}", res);
            index = res.node.modified_index;

        }
    }
    fn list_members(&self) -> BTreeMap<String, T> {
        let current_listing = self.etcd.get(DIR, true, true, true).expect("List members");
        trace!("Listing: {:?}", current_listing);
        current_listing.node.nodes.expect("Node listing").into_iter()
                .filter_map(|n|
                    if let (Some(k), Some(v)) = (n.key, n.value) {
                        Some((k, json::decode(&v).expect("decode json"))) } else { None })
                .collect::<BTreeMap<String, T>>()
    }
    fn run_watch(&self) {
        info!("Starting etcd watcher");

        let current_listing = self.etcd.get(DIR, true, true, true).expect("List members");
        debug!("Listing: {:?}", current_listing);
        let mut last_observed_index = current_listing.node.nodes.unwrap_or_else(|| Vec::new()).into_iter()
            .filter_map(|x| x.modified_index).max();

        loop {
            debug!("Awaiting for {} from {:?}", DIR, last_observed_index);
            let res = self.etcd.watch(DIR, last_observed_index, true).expect("watch");
            info!("Watch: {:?}", res);
            last_observed_index = res.node.modified_index.map(|x| x+1);
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
