extern crate cannyls;
extern crate cannyls_rpc;
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
use fibers_rpc::client::ClientService;
use fibers_rpc::server::ServerBuilder;
use slog::{Discard, Logger};
use trackable::result::TestResult;

#[tokio::test(flavor = "multi_thread")]
async fn basic_rpc_works() -> TestResult {
    fn device_id() -> DeviceId {
        DeviceId::new("foo")
    }

    fn lump_id(n: u128) -> LumpId {
        LumpId::new(n)
    }

    // Device Registry
    let registry = DeviceRegistry::new(Logger::root(Discard, o!()));
    let registry_handle = registry.handle();

    let nvm = MemoryNvm::new(vec![0; 100 * 1024 * 1024]);
    let storage = track_try_unwrap!(StorageBuilder::new().create(nvm));
    let device = DeviceBuilder::new().spawn(|| Ok(storage));
    track_try_unwrap!(registry_handle.put_device(device_id(), device));

    tokio::spawn(async {
        track!(registry.await).unwrap_or_else(|e| panic!("{}", e));
    });

    // Server
    let server_addr = "127.0.0.1:1920".parse().unwrap();
    let mut builder = ServerBuilder::new(server_addr);
    Server::new(registry_handle).register(&mut builder);
    let server = builder.finish();
    tokio::spawn(async {
        track!(server.await).unwrap_or_else(|e| panic!("{}", e));
    });

    // RPC service
    let service = ClientService::new();
    let service_handle = service.handle();
    tokio::spawn(async {
        track!(service.await).unwrap_or_else(|e| panic!("{}", e));
    });

    // Client
    let client = Client::new(server_addr, service_handle);
    let request = client.request();
    assert_eq!(track!(request.list_lumps(device_id()).await)?, vec![]);
    assert_eq!(
        track!(request.get_lump(device_id(), lump_id(0)).await)?,
        None
    );
    assert_eq!(
        request
            .put_lump(
                device_id(),
                lump_id(0),
                LumpData::new("bar".into()).unwrap()
            )
            .await?,
        true
    );
    assert_eq!(
        track!(request.get_lump(device_id(), lump_id(0)).await)?,
        Some(Vec::from("bar"))
    );
    assert_eq!(
        track!(request.head_lump(device_id(), lump_id(0)).await)?.map(|h| h.approximate_data_size),
        Some(3)
    );
    assert_eq!(
        track!(request.list_lumps(device_id()).await)?,
        vec![lump_id(0)]
    );
    assert_eq!(
        track!(request.delete_lump(device_id(), lump_id(0)).await)?,
        true
    );
    assert_eq!(track!(request.list_lumps(device_id()).await)?, vec![]);

    Ok(())
}
