use fibers_rpc::server::{HandleCall, Reply, ServerBuilder};
use futures::Future;

use protobuf::PutLumpRequestDecoderFactory;
use registry::DeviceRegistryHandle;
use rpc;

macro_rules! rpc_try {
    ($expr:expr) => {
        match $expr {
            Err(e) => return Reply::done(Err(track!(e))),
            Ok(v) => v,
        }
    };
}

/// RPCサーバ.
#[derive(Debug)]
pub struct Server {
    registry: DeviceRegistryHandle,
}
impl Server {
    /// 指定されたレジストリを操作するための、新しいRPCサーバインスタンスを生成する.
    pub fn new(registry: DeviceRegistryHandle) -> Self {
        Server { registry }
    }

    /// RPCサーバを登録して、利用可能な状態にする.
    pub fn register(self, builder: &mut ServerBuilder) {
        let registry = self.registry.clone();
        let clone = move || Server {
            registry: registry.clone(),
        };
        builder.add_call_handler::<rpc::GetLumpRpc, _>(clone());
        builder.add_call_handler::<rpc::HeadLumpRpc, _>(clone());
        builder.add_call_handler_with_decoder::<rpc::PutLumpRpc, _, _>(
            clone(),
            PutLumpRequestDecoderFactory::new(self.registry),
        );
        builder.add_call_handler::<rpc::DeleteLumpRpc, _>(clone());
        builder.add_call_handler::<rpc::ListLumpRpc, _>(clone());
        builder.add_call_handler::<rpc::UsageRangeRpc, _>(clone());
        builder.add_call_handler::<rpc::DeleteRangeRpc, _>(clone());
    }
}
impl HandleCall<rpc::GetLumpRpc> for Server {
    fn handle_call(&self, request: rpc::LumpRequest) -> Reply<rpc::GetLumpRpc> {
        let device = rpc_try!(self.registry.get_device(&request.device_id));
        let future = request.options.with(&device).get(request.lump_id).then(Ok);
        Reply::future(future)
    }
}
impl HandleCall<rpc::HeadLumpRpc> for Server {
    fn handle_call(&self, request: rpc::LumpRequest) -> Reply<rpc::HeadLumpRpc> {
        let device = rpc_try!(self.registry.get_device(&request.device_id));
        let future = request.options.with(&device).head(request.lump_id).then(Ok);
        Reply::future(future)
    }
}
impl HandleCall<rpc::PutLumpRpc> for Server {
    fn handle_call(&self, request: rpc::PutLumpRequest) -> Reply<rpc::PutLumpRpc> {
        let device = rpc_try!(self.registry.get_device(&request.device_id));
        let lump_data = request.lump_data;
        let future = request
            .options
            .with(&device)
            .put(request.lump_id, lump_data)
            .then(Ok);
        Reply::future(future)
    }
}
impl HandleCall<rpc::DeleteLumpRpc> for Server {
    fn handle_call(&self, request: rpc::LumpRequest) -> Reply<rpc::DeleteLumpRpc> {
        let device = rpc_try!(self.registry.get_device(&request.device_id));
        let future = request
            .options
            .with(&device)
            .delete(request.lump_id)
            .then(Ok);
        Reply::future(future)
    }
}
impl HandleCall<rpc::ListLumpRpc> for Server {
    fn handle_call(&self, request: rpc::DeviceRequest) -> Reply<rpc::ListLumpRpc> {
        let device = rpc_try!(self.registry.get_device(&request.device_id));
        let future = request.options.with(&device).list().then(Ok);
        Reply::future(future)
    }
}
impl HandleCall<rpc::UsageRangeRpc> for Server {
    fn handle_call(&self, request: rpc::UsageRangeRequest) -> Reply<rpc::UsageRangeRpc> {
        let device = rpc_try!(self.registry.get_device(&request.device_id));
        let future = request
            .options
            .with(&device)
            .usage_range(request.range)
            .then(Ok);
        Reply::future(future)
    }
}
impl HandleCall<rpc::DeleteRangeRpc> for Server {
    fn handle_call(&self, request: rpc::RangeLumpRequest) -> Reply<rpc::DeleteRangeRpc> {
        let device = rpc_try!(self.registry.get_device(&request.device_id));
        let future = request
            .options
            .with(&device)
            .delete_range(request.range)
            .then(Ok);
        Reply::future(future)
    }
}
