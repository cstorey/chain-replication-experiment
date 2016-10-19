use tokio::reactor::Handle;
use {RamStore, sexp_proto, TailService, ServerService};
use service::{Service, NewService, simple_service};
use tokio::io::FramedIo;
use futures::{self, Poll, Async, Future, stream, BoxFuture};
use proto::pipeline::{self, Frame};
use proto::{self, Message};

use std::net::SocketAddr;
use std::io;
use std::sync::{Mutex, Arc};

use std::collections::{BTreeMap, VecDeque};
use void::Void;
use take::Take;
use std::marker::PhantomData;

pub trait SchedHandle {}

pub trait Host<H: SchedHandle>: Sized {
    type Addr;

    fn build_server(&mut self,
                    service: CoreService,
                    handle: &H,
                    head_addr: Self::Addr,
                    tail_addr: Self::Addr)
                    -> Result<HostConfig<Self::Addr>, io::Error>;
}

#[derive(Debug, Clone)]
pub struct HostConfig<A> {
    pub head: A,
    pub tail: A,
}

#[derive(Debug)]
pub struct CoreService {
    head: ServerService<RamStore>,
    tail: TailService<RamStore>,
}

pub struct SexpHost;

impl CoreService {
    pub fn new() -> Self {
        let store = RamStore::new();

        CoreService {
            head: ServerService::new(store.clone()),
            tail: TailService::new(store.clone()),
        }
    }
}

impl Host<Handle> for SexpHost {
    type Addr = SocketAddr;

    fn build_server(&mut self,
                    service: CoreService,
                    handle: &Handle,
                    head_addr: Self::Addr,
                    tail_addr: Self::Addr)
                    -> Result<HostConfig<Self::Addr>, io::Error> {
        let CoreService { head, tail } = service;
        let head_host = try!(sexp_proto::server::serve(handle, head_addr, head));
        let tail_host = try!(sexp_proto::server::serve(handle, tail_addr, tail));

        Ok(HostConfig {
            head: head_host.local_addr().clone(),
            tail: tail_host.local_addr().clone(),
        })
    }
}

impl SchedHandle for Handle {}
