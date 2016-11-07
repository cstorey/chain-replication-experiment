use futures::stream::Stream;
use futures::{Async, Poll};
use futures_cpupool::CpuPool;
use etcd;

use Error;

pub struct EtcdViewManager {
    etcd: etcd::Client,
    value: String,
    pool: CpuPool,
}

impl EtcdViewManager {
    fn new(_url: &str, value: &str) -> Self {
        let client = etcd::Client::default();
        let pool = CpuPool::new(1);
        EtcdViewManager {
            etcd: client,
            value: value.to_string(),
            pool: pool,
        }
    }
}

impl Stream for EtcdViewManager {
    type Item = (u64, Vec<String>);
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        unimplemented!()
    }
}

#[cfg(test)]
mod test {
    use tokio::reactor::Core;
    use super::EtcdViewManager;
    use futures::Future;
    use futures::stream::Stream;

    const ETCD_URL: &'static str = "http://localhost/";

    // This module is responsible for registering itself as a node in eg: etcd,
    // and sending heart-beats when we are alive, and shutting down on removal.

    // For now, we'll test this by creating a process that a) sends heartbeats b)
    // yields a stream of config changes.

    #[test]
    #[ignore]
    fn should_yield_single_node_for_single_item() {
        let mut core = Core::new().expect("core::new");
        let self_config = "23";
        let me = EtcdViewManager::new(ETCD_URL, self_config.clone());
        let (next, me) = core.run(me.into_future().map_err(|(e, _)| e)).expect("run one");
        let (vers, config) = next.expect("next value");

        assert_eq!(config.into_iter().collect::<Vec<String>>(),
                   vec![self_config.to_string()]);
    }


    #[test]
    #[ignore]
    fn should_add_new_members_to_tail() {
        let mut core = Core::new().expect("core::new");
        let first_config = "23";
        let second_config = "42";
        let first = EtcdViewManager::new(ETCD_URL, first_config.clone());
        let second = EtcdViewManager::new(ETCD_URL, second_config.clone());
        let (next, first) = core.run(first.into_future().map_err(|(e, _)| e)).expect("run one");
        // let (vers, config) = next.expect("next value");
        core.handle().spawn(first.for_each(|e|
                    Ok(println!("should_add_new_members_to_tail::first: {:?}", e)))
                    .map_err(|e| panic!("first: {:?}", e)));

        let (next, me) = core.run(second.into_future().map_err(|(e, _)| e)).expect("run one");
        let (vers, config) = next.expect("next value");
        assert_eq!(config.into_iter().collect::<Vec<_>>(),
                   vec![first_config.to_string(), second_config.to_string()]);
    }

    #[test]
    #[ignore]
    fn should_remove_dead_members() {
        let mut core = Core::new().expect("core::new");
        let first_config = "23";
        let second_config = "42";
        let first = EtcdViewManager::new(ETCD_URL, first_config.clone());
        let second = EtcdViewManager::new(ETCD_URL, second_config.clone());

        let (next, first) = core.run(first.into_future().map_err(|(e, _)| e)).expect("run one");
        let (vers, config) = next.expect("next value");
        core.handle().spawn(first.for_each(|e|
                    Ok(println!("should_add_new_members_to_tail::first: {:?}", e)))
                    .map_err(|e| panic!("first: {:?}", e)));

        let (next, me) = core.run(second.into_future().map_err(|(e, _)| e)).expect("run one");
        let (vers, config) = next.expect("next value");
        assert_eq!(config.into_iter().collect::<Vec<_>>(),
                   vec![first_config.to_string(), second_config.to_string()]);

        // Run both until quiescent
        // forcibly remove one instance.
        // Run until new config

    }
}
