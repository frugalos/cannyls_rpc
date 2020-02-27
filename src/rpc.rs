use cannyls::deadline::Deadline;
use cannyls::device::{self, DeviceHandle};
use cannyls::lump::{LumpData, LumpHeader, LumpId};
use cannyls::storage::StorageUsage;
use cannyls::Result;
use fibers_rpc::{Call, ProcedureId};
use std::ops::Range;

use device::DeviceId;
use protobuf::{
    DeleteLumpRequestDecoder, DeleteLumpRequestEncoder, DeviceRequestDecoder, DeviceRequestEncoder,
    GetLumpResponseDecoder, GetLumpResponseEncoder, HeadLumpResponseDecoder,
    HeadLumpResponseEncoder, ListLumpResponseDecoder, ListLumpResponseEncoder, LumpRequestDecoder,
    LumpRequestEncoder, PutLumpRequestDecoder, PutLumpRequestEncoder, PutLumpResponseDecoder,
    PutLumpResponseEncoder, UsageRangeRequestDecoder, UsageRangeRequestEncoder,
    UsageRangeResponseDecoder, UsageRangeResponseEncoder,
};

const NS_CANNYLS: u32 = 0x0001_0000; // cannyls用のRPCの名前空間(ID範囲)

#[derive(Debug)]
pub struct GetLumpRpc;
impl Call for GetLumpRpc {
    const ID: ProcedureId = ProcedureId(NS_CANNYLS | 0x0001);
    const NAME: &'static str = "cannyls.lump.get";

    type Req = LumpRequest;
    type ReqDecoder = LumpRequestDecoder;
    type ReqEncoder = LumpRequestEncoder;

    type Res = Result<Option<LumpData>>;
    type ResDecoder = GetLumpResponseDecoder;
    type ResEncoder = GetLumpResponseEncoder;
}

#[derive(Debug)]
pub struct HeadLumpRpc;
impl Call for HeadLumpRpc {
    const ID: ProcedureId = ProcedureId(NS_CANNYLS | 0x0002);
    const NAME: &'static str = "cannyls.lump.head";

    type Req = LumpRequest;
    type ReqDecoder = LumpRequestDecoder;
    type ReqEncoder = LumpRequestEncoder;

    type Res = Result<Option<LumpHeader>>;
    type ResDecoder = HeadLumpResponseDecoder;
    type ResEncoder = HeadLumpResponseEncoder;
}

#[derive(Debug)]
pub struct PutLumpRpc;
impl Call for PutLumpRpc {
    const ID: ProcedureId = ProcedureId(NS_CANNYLS | 0x0003);
    const NAME: &'static str = "cannyls.lump.put";

    type Req = PutLumpRequest;
    type ReqDecoder = PutLumpRequestDecoder;
    type ReqEncoder = PutLumpRequestEncoder;

    type Res = Result<bool>;
    type ResDecoder = PutLumpResponseDecoder;
    type ResEncoder = PutLumpResponseEncoder;
}

#[derive(Debug)]
pub struct DeleteLumpRpc;
impl Call for DeleteLumpRpc {
    const ID: ProcedureId = ProcedureId(NS_CANNYLS | 0x0004);
    const NAME: &'static str = "cannyls.lump.delete";

    type Req = LumpRequest;
    type ReqDecoder = LumpRequestDecoder;
    type ReqEncoder = LumpRequestEncoder;

    type Res = Result<bool>;
    type ResDecoder = DeleteLumpRequestDecoder;
    type ResEncoder = DeleteLumpRequestEncoder;
}

#[derive(Debug)]
pub struct ListLumpRpc;
impl Call for ListLumpRpc {
    const ID: ProcedureId = ProcedureId(NS_CANNYLS | 0x0005);
    const NAME: &'static str = "cannyls.lump.list";

    type Req = DeviceRequest;
    type ReqDecoder = DeviceRequestDecoder;
    type ReqEncoder = DeviceRequestEncoder;

    type Res = Result<Vec<LumpId>>;
    type ResDecoder = ListLumpResponseDecoder;
    type ResEncoder = ListLumpResponseEncoder;
}

#[derive(Debug)]
pub struct UsageRangeRpc;
impl Call for UsageRangeRpc {
    const ID: ProcedureId = ProcedureId(NS_CANNYLS | 0x0006);
    const NAME: &'static str = "cannyls.lump.usage_range";

    type Req = UsageRangeRequest;
    type ReqDecoder = UsageRangeRequestDecoder;
    type ReqEncoder = UsageRangeRequestEncoder;

    type Res = Result<StorageUsage>;
    type ResDecoder = UsageRangeResponseDecoder;
    type ResEncoder = UsageRangeResponseEncoder;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestOptions {
    pub deadline: Deadline,
    pub max_queue_len: Option<usize>,
}
impl RequestOptions {
    pub fn with<'a>(&self, device: &'a DeviceHandle) -> device::DeviceRequest<'a> {
        let mut request = device.request();
        request.deadline(self.deadline);
        if let Some(n) = self.max_queue_len {
            request.max_queue_len(n);
        }
        request
    }
}

#[derive(Debug)]
pub struct DeviceRequest {
    pub device_id: DeviceId,
    pub options: RequestOptions,
}

#[derive(Debug)]
pub struct LumpRequest {
    pub device_id: DeviceId,
    pub lump_id: LumpId,
    pub options: RequestOptions,
}

#[derive(Debug)]
pub struct PutLumpRequest {
    pub device_id: DeviceId,
    pub lump_id: LumpId,
    pub lump_data: LumpData,
    pub options: RequestOptions,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageRangeRequest {
    pub device_id: DeviceId,
    pub range: Range<LumpId>,
    pub options: RequestOptions,
}
