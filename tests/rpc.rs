extern crate cannyls;
extern crate cannyls_rpc;
extern crate fibers;
extern crate fibers_rpc;
extern crate futures;
#[macro_use]
extern crate slog;
extern crate tempdir;
#[macro_use]
extern crate trackable;

use cannyls::device::DeviceBuilder;
use cannyls::lump::{LumpData, LumpId};
use cannyls::nvm::MemoryNvm;
use cannyls::storage::StorageBuilder;
use cannyls_rpc::{Client, DeviceId, DeviceRegistry, Server};
use fibers::{Executor, InPlaceExecutor, Spawn};
use fibers_rpc::client::ClientService;
use fibers_rpc::server::ServerBuilder;
use futures::{Async, Future};
use slog::{Discard, Logger};
use std::thread;

macro_rules! wait {
    ($future:expr) => {{
        let mut f = $future;
        loop {
            if let Async::Ready(item) = track_try_unwrap!(f.poll()) {
                break item;
            }
        }
    }};
}

#[test]
fn basic_rpc_works() {
    fn device_id() -> DeviceId {
        DeviceId::new("foo")
    }

    fn lump_id(n: u128) -> LumpId {
        LumpId::new(n)
    }

    // Executor
    let executor = track_try_unwrap!(track_any_err!(InPlaceExecutor::new()));

    // Device Registry
    let registry = DeviceRegistry::new(Logger::root(Discard, o!()));
    let registry_handle = registry.handle();

    let nvm = MemoryNvm::new(vec![0; 100 * 1024 * 1024]);
    let storage = track_try_unwrap!(StorageBuilder::new().create(nvm));
    let device = DeviceBuilder::new().spawn(|| Ok(storage));
    track_try_unwrap!(registry_handle.put_device(device_id(), device));

    executor.spawn(registry.map_err(|e| panic!("{}", e)));

    // Server
    let server_addr = "127.0.0.1:1920".parse().unwrap();
    let mut builder = ServerBuilder::new(server_addr);
    Server::new(registry_handle).register(&mut builder);
    let server = builder.finish(executor.handle());
    executor.spawn(server.map_err(|e| panic!("{}", e)));

    // RPC service
    let service = ClientService::new(executor.handle());
    let service_handle = service.handle();
    executor.spawn(service.map_err(|e| panic!("{}", e)));

    thread::spawn(move || {
        if let Err(e) = executor.run() {
            panic!("{}", e);
        }
    });

    // Client
    let client = Client::new(server_addr, service_handle);
    let request = client.request();
    assert_eq!(wait!(request.list_lumps(device_id())), vec![]);
    assert_eq!(wait!(request.get_lump(device_id(), lump_id(0))), None);
    assert_eq!(
        wait!(request.put_lump(
            device_id(),
            lump_id(0),
            LumpData::new("bar".into()).unwrap()
        )),
        true
    );
    assert_eq!(
        wait!(request.get_lump(device_id(), lump_id(0))),
        Some(Vec::from("bar"))
    );
    assert_eq!(
        wait!(request.head_lump(device_id(), lump_id(0))).map(|h| h.approximate_data_size),
        Some(3)
    );
    assert_eq!(wait!(request.list_lumps(device_id())), vec![lump_id(0)]);
    assert_eq!(wait!(request.delete_lump(device_id(), lump_id(0))), true);
    assert_eq!(wait!(request.list_lumps(device_id())), vec![]);
}
