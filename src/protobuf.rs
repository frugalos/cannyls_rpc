//! `package cannyls_rpc.protobuf;`
//!
//! メッセージ定義は[cannyls_rpc.proto]を参照のこと.
//!
//! [cannyls_rpc.proto]: https://github.com/frugalos/cannyls_rpc/blob/master/protobuf/cannyls_rpc.proto
#![cfg_attr(feature = "cargo-clippy", allow(clippy::type_complexity))]
use bytecodec::bytes::BytesDecoder as BytecodecBytesDecoder;
use bytecodec::combinator::{Peekable, PreEncode};
use bytecodec::{self, ByteCount, Decode, Encode, Eos, ErrorKind, Result, SizedEncode};
use cannyls;
use cannyls::block::BlockSize;
use cannyls::deadline::Deadline;
use cannyls::device::DeviceHandle;
use cannyls::lump::{LumpData, LumpHeader, LumpId};
use cannyls::storage::StorageUsage;
use factory::Factory;
use protobuf_codec::field::branch::Branch2;
use protobuf_codec::field::num::{F1, F2, F3, F4};
use protobuf_codec::field::{
    FieldDecode, FieldDecoder, FieldEncoder, Fields, MaybeDefault, MessageFieldDecoder,
    MessageFieldEncoder, Oneof, Optional, Repeated,
};
use protobuf_codec::message::{MessageDecode, MessageDecoder, MessageEncode, MessageEncoder};
use protobuf_codec::scalar::{
    BoolDecoder, BoolEncoder, BytesDecoder, BytesEncoder, CustomBytesDecoder, Fixed64Decoder,
    Fixed64Encoder, StringDecoder, StringEncoder, Uint32Decoder, Uint32Encoder, Uint64Decoder,
    Uint64Encoder,
};
use protobuf_codec::wellknown::google::protobuf::{StdDurationDecoder, StdDurationEncoder};
use protobuf_codec::wellknown::protobuf_codec::protobuf::trackable;
use protobuf_codec::wire::Tag;
use std;
use std::ops::Range;
use trackable::error::{ErrorKindExt, TrackableError};

use rpc::{DeviceRequest, LumpRequest, PutLumpRequest, RequestOptions, UsageRangeRequest};
use {DeviceId, DeviceRegistryHandle};

macro_rules! impl_message_decode {
    ($decoder:ty, $item:ty, $map:expr) => {
        impl Decode for $decoder {
            type Item = $item;

            fn decode(&mut self, buf: &[u8], eos: Eos) -> Result<usize> {
                track!(self.inner.decode(buf, eos))
            }

            fn finish_decoding(&mut self) -> Result<Self::Item> {
                let item = track!(self.inner.finish_decoding())?;
                $map(item)
            }

            fn is_idle(&self) -> bool {
                self.inner.is_idle()
            }

            fn requiring_bytes(&self) -> ByteCount {
                self.inner.requiring_bytes()
            }
        }
        impl MessageDecode for $decoder {}
    };
}

macro_rules! impl_message_encode {
    ($encoder:ty, $item:ty, $map:expr) => {
        impl Encode for $encoder {
            type Item = $item;

            fn encode(&mut self, buf: &mut [u8], eos: Eos) -> Result<usize> {
                track!(self.inner.encode(buf, eos))
            }

            fn start_encoding(&mut self, item: Self::Item) -> Result<()> {
                track!(self.inner.start_encoding($map(item)))
            }

            fn is_idle(&self) -> bool {
                self.inner.is_idle()
            }

            fn requiring_bytes(&self) -> ByteCount {
                self.inner.requiring_bytes()
            }
        }
        impl MessageEncode for $encoder {}
    };
}

macro_rules! impl_sized_message_encode {
    ($encoder:ty, $item:ty, $map:expr) => {
        impl_message_encode!($encoder, $item, $map);
        impl SizedEncode for $encoder {
            fn exact_requiring_bytes(&self) -> u64 {
                self.inner.exact_requiring_bytes()
            }
        }
    };
}

#[derive(Debug, Default)]
pub struct LumpIdDecoder {
    inner: MessageDecoder<
        Fields<(
            FieldDecoder<F1, Fixed64Decoder>,
            FieldDecoder<F2, Fixed64Decoder>,
        )>,
    >,
}
impl_message_decode!(LumpIdDecoder, LumpId, |(high, low)| Ok(LumpId::new(
    (u128::from(high) << 64) | u128::from(low)
)));

#[derive(Debug, Default)]
pub struct LumpIdEncoder {
    inner: MessageEncoder<
        Fields<(
            FieldEncoder<F1, Fixed64Encoder>,
            FieldEncoder<F2, Fixed64Encoder>,
        )>,
    >,
}
impl_sized_message_encode!(LumpIdEncoder, LumpId, |item: Self::Item| {
    let id = item.as_u128();
    let high = (id >> 64) as u64;
    let low = id as u64;
    (high, low)
});

#[derive(Debug, Default)]
pub struct DeadlineDecoder {
    inner: MessageDecoder<
        Fields<(
            MaybeDefault<FieldDecoder<F1, Uint32Decoder>>,
            MaybeDefault<MessageFieldDecoder<F2, StdDurationDecoder>>,
        )>,
    >,
}
impl_message_decode!(DeadlineDecoder, Deadline, |(kind, duration)| Ok(
    match kind {
        0 => Deadline::Infinity,
        1 => Deadline::Within(duration),
        2 => Deadline::Immediate,
        _ => track_panic!(ErrorKind::InvalidInput, "Unknown deadline type: {}", kind),
    }
));

#[derive(Debug, Default)]
pub struct DeadlineEncoder {
    inner: MessageEncoder<
        Fields<(
            FieldEncoder<F1, Uint32Encoder>,
            Optional<MessageFieldEncoder<F2, StdDurationEncoder>>,
        )>,
    >,
}
impl_sized_message_encode!(DeadlineEncoder, Deadline, |item: Self::Item| match item {
    Deadline::Infinity => (0, None),
    Deadline::Within(d) => (1, Some(d)),
    Deadline::Immediate => (2, None),
});

#[derive(Debug, Default)]
pub struct StorageUsageDecoder {
    inner: MessageDecoder<
        Fields<(
            MaybeDefault<FieldDecoder<F1, Uint64Decoder>>,
            MaybeDefault<FieldDecoder<F2, Uint64Decoder>>,
        )>,
    >,
}
impl_message_decode!(StorageUsageDecoder, StorageUsage, |(kind, usage)| Ok(
    match kind {
        0 => StorageUsage::Unknown,
        1 => StorageUsage::Approximate(usage),
        _ => track_panic!(
            ErrorKind::InvalidInput,
            "Unknown storage usage type: {}",
            kind
        ),
    }
));

#[derive(Debug, Default)]
pub struct StorageUsageEncoder {
    inner: MessageEncoder<
        Fields<(
            FieldEncoder<F1, Uint64Encoder>,
            MaybeDefault<FieldEncoder<F2, Uint64Encoder>>,
        )>,
    >,
}
impl_sized_message_encode!(
    StorageUsageEncoder,
    StorageUsage,
    |item: Self::Item| match item {
        StorageUsage::Unknown => (0, Default::default()),
        StorageUsage::Approximate(n) => (1, n),
    }
);

#[derive(Debug, Default)]
pub struct RequestOptionsDecoder {
    inner: MessageDecoder<
        Fields<(
            MaybeDefault<MessageFieldDecoder<F1, DeadlineDecoder>>,
            MaybeDefault<FieldDecoder<F2, Uint32Decoder>>,
        )>,
    >,
}
impl_message_decode!(RequestOptionsDecoder, RequestOptions, |(
    deadline,
    queue_size_limit,
)| {
    let max_queue_len = if queue_size_limit == 0 {
        None
    } else {
        Some(queue_size_limit as usize - 1)
    };
    Ok(RequestOptions {
        deadline,
        max_queue_len,
    })
});

#[derive(Debug, Default)]
pub struct RequestOptionsEncoder {
    inner: MessageEncoder<
        Fields<(
            MessageFieldEncoder<F1, DeadlineEncoder>,
            MaybeDefault<FieldEncoder<F2, Uint32Encoder>>,
        )>,
    >,
}
impl_sized_message_encode!(RequestOptionsEncoder, RequestOptions, |item: Self::Item| {
    let queue_size_limit = item.max_queue_len.map_or(0, |n| n + 1);
    (item.deadline, queue_size_limit as u32)
});

#[derive(Debug, Default)]
pub struct LumpRequestDecoder {
    inner: MessageDecoder<
        Fields<(
            FieldDecoder<F1, StringDecoder>,
            MessageFieldDecoder<F2, LumpIdDecoder>,
            MessageFieldDecoder<F3, RequestOptionsDecoder>,
        )>,
    >,
}
impl_message_decode!(LumpRequestDecoder, LumpRequest, |(
    device_id,
    lump_id,
    options,
)| Ok(LumpRequest {
    device_id: DeviceId::new(device_id),
    lump_id,
    options,
}));

#[derive(Debug, Default)]
pub struct LumpRequestEncoder {
    inner: MessageEncoder<
        Fields<(
            FieldEncoder<F1, StringEncoder>,
            MessageFieldEncoder<F2, LumpIdEncoder>,
            MessageFieldEncoder<F3, RequestOptionsEncoder>,
        )>,
    >,
}
impl_sized_message_encode!(LumpRequestEncoder, LumpRequest, |item: Self::Item| (
    item.device_id.into_string(),
    item.lump_id,
    item.options
));

#[derive(Debug)]
pub struct PutLumpRequestDecoderFactory {
    registry: DeviceRegistryHandle,
}
impl PutLumpRequestDecoderFactory {
    pub fn new(registry: DeviceRegistryHandle) -> Self {
        PutLumpRequestDecoderFactory { registry }
    }
}
impl Factory for PutLumpRequestDecoderFactory {
    type Item = PutLumpRequestDecoder;

    fn create(&self) -> Self::Item {
        PutLumpRequestDecoder::new(self.registry.clone())
    }
}

#[derive(Debug)]
pub struct PutLumpRequestDecoder {
    inner: MessageDecoder<PutLumpRequestFieldsDecoder>,
}
impl PutLumpRequestDecoder {
    fn new(registry: DeviceRegistryHandle) -> Self {
        PutLumpRequestDecoder {
            inner: MessageDecoder::new(PutLumpRequestFieldsDecoder::new(registry)),
        }
    }
}
impl_message_decode!(PutLumpRequestDecoder, PutLumpRequest, Ok);

#[derive(Debug)]
struct PutLumpRequestFieldsDecoder {
    device_id: FieldDecoder<F1, Peekable<StringDecoder>>,
    lump_id: MessageFieldDecoder<F2, LumpIdDecoder>,
    lump_data: FieldDecoder<F3, CustomBytesDecoder<LumpDataDecoder>>,
    options: MessageFieldDecoder<F4, RequestOptionsDecoder>,
    index: usize,
}
impl PutLumpRequestFieldsDecoder {
    fn new(registry: DeviceRegistryHandle) -> Self {
        PutLumpRequestFieldsDecoder {
            device_id: Default::default(),
            lump_id: Default::default(),
            lump_data: FieldDecoder::new(
                F3,
                CustomBytesDecoder::new(LumpDataDecoder::new(registry)),
            ),
            options: Default::default(),
            index: 0,
        }
    }
}
impl Decode for PutLumpRequestFieldsDecoder {
    type Item = PutLumpRequest;

    fn decode(&mut self, buf: &[u8], eos: Eos) -> Result<usize> {
        match self.index {
            0 => Ok(0),
            1 => {
                let size = track!(self.device_id.decode(buf, eos))?;
                if let Some(device_id) = self.device_id.value_decoder_ref().peek() {
                    if self.device_id.is_idle() {
                        self.lump_data
                            .value_decoder_mut()
                            .inner_mut()
                            .device_hint(device_id);
                    }
                }
                Ok(size)
            }
            2 => track!(self.lump_id.decode(buf, eos)),
            3 => track!(self.lump_data.decode(buf, eos)),
            4 => track!(self.options.decode(buf, eos)),
            _ => unreachable!(),
        }
    }

    fn finish_decoding(&mut self) -> Result<Self::Item> {
        self.index = 0;
        let device_id = track!(self.device_id.finish_decoding())?;
        let lump_id = track!(self.lump_id.finish_decoding())?;
        let lump_data = track!(self.lump_data.finish_decoding())?;
        let options = track!(self.options.finish_decoding())?;
        Ok(PutLumpRequest {
            device_id: DeviceId::new(device_id),
            lump_id,
            lump_data,
            options,
        })
    }

    fn is_idle(&self) -> bool {
        match self.index {
            0 => true,
            1 => self.device_id.is_idle(),
            2 => self.lump_id.is_idle(),
            3 => self.lump_data.is_idle(),
            4 => self.options.is_idle(),
            _ => unreachable!(),
        }
    }

    fn requiring_bytes(&self) -> ByteCount {
        match self.index {
            0 => ByteCount::Finite(0),
            1 => self.device_id.requiring_bytes(),
            2 => self.lump_id.requiring_bytes(),
            3 => self.lump_data.requiring_bytes(),
            4 => self.options.requiring_bytes(),
            _ => unreachable!(),
        }
    }
}
impl FieldDecode for PutLumpRequestFieldsDecoder {
    fn start_decoding(&mut self, tag: Tag) -> Result<bool> {
        let started = track!(self.device_id.start_decoding(tag))?;
        if started {
            self.index = 1;
            return Ok(true);
        }

        let started = track!(self.lump_id.start_decoding(tag))?;
        if started {
            self.index = 2;
            return Ok(true);
        }

        let started = track!(self.lump_data.start_decoding(tag))?;
        if started {
            self.index = 3;
            return Ok(true);
        }

        let started = track!(self.options.start_decoding(tag))?;
        if started {
            self.index = 4;
            return Ok(true);
        }

        Ok(false)
    }
}

#[derive(Debug, Default)]
pub struct PutLumpRequestEncoder {
    inner: MessageEncoder<
        Fields<(
            FieldEncoder<F1, StringEncoder>,
            MessageFieldEncoder<F2, LumpIdEncoder>,
            FieldEncoder<F3, BytesEncoder<LumpData>>,
            MessageFieldEncoder<F4, RequestOptionsEncoder>,
        )>,
    >,
}
impl_sized_message_encode!(PutLumpRequestEncoder, PutLumpRequest, |item: Self::Item| (
    item.device_id.into_string(),
    item.lump_id,
    item.lump_data,
    item.options,
));

#[derive(Debug)]
struct LumpDataDecoder {
    is_first: bool,
    bytes: BytecodecBytesDecoder<LumpData>,
    registry: DeviceRegistryHandle,
    device_hint: Option<DeviceHandle>,
}
impl LumpDataDecoder {
    fn new(registry: DeviceRegistryHandle) -> Self {
        let empty = LumpData::new_embedded(Vec::new()).expect("never fails");
        LumpDataDecoder {
            is_first: true,
            bytes: BytecodecBytesDecoder::new(empty),
            registry,
            device_hint: None,
        }
    }

    fn is_data_to_be_embedded(&self, data_size: usize) -> bool {
        let block_size = self
            .device_hint
            .as_ref()
            .and_then(|device| device.metrics().storage().map(|s| s.header().block_size))
            .unwrap_or_else(BlockSize::min);
        data_size <= block_size.as_u16() as usize
    }

    fn device_hint(&mut self, device_id: &str) {
        self.device_hint = self.registry.get_device(device_id).ok();
    }
}
impl Decode for LumpDataDecoder {
    type Item = LumpData;

    fn decode(&mut self, buf: &[u8], eos: Eos) -> Result<usize> {
        if self.is_first {
            self.is_first = false;
            let remaining_bytes =
                track_assert_some!(eos.remaining_bytes().to_u64(), ErrorKind::InvalidInput);
            track_assert!(remaining_bytes <= 0xFFFF_FFFF, ErrorKind::InvalidInput; remaining_bytes);

            let data_size = buf.len() + remaining_bytes as usize;
            let data = if self.is_data_to_be_embedded(data_size) {
                track!(LumpData::new_embedded(vec![0; data_size]))
            } else if let Some(ref device) = self.device_hint {
                track!(device.allocate_lump_data(data_size))
            } else {
                track!(LumpData::new(vec![0; data_size]))
            }
            .map_err(|e| ErrorKind::InvalidInput.takes_over(e))?;
            self.bytes = BytecodecBytesDecoder::new(data);
        }
        track!(self.bytes.decode(buf, eos))
    }

    fn finish_decoding(&mut self) -> Result<Self::Item> {
        self.device_hint = None;
        track_assert!(!self.is_first, ErrorKind::IncompleteDecoding);
        track!(self.bytes.finish_decoding())
    }

    fn requiring_bytes(&self) -> ByteCount {
        if self.is_first {
            ByteCount::Unknown
        } else {
            self.bytes.requiring_bytes()
        }
    }

    fn is_idle(&self) -> bool {
        if self.is_first {
            false
        } else {
            self.bytes.is_idle()
        }
    }
}

#[derive(Debug, Default)]
pub struct ErrorDecoder {
    inner: MessageDecoder<MessageFieldDecoder<F1, trackable::ErrorDecoder>>,
}
impl_message_decode!(ErrorDecoder, cannyls::Error, |e: TrackableError<String>| {
    let kind = match e.kind().as_str() {
        "StorageFull" => cannyls::ErrorKind::StorageFull,
        "StorageCorrupted" => cannyls::ErrorKind::StorageCorrupted,
        "DeviceBusy" => cannyls::ErrorKind::DeviceBusy,
        "DeviceTerminated" => cannyls::ErrorKind::DeviceTerminated,
        "InvalidInput" => cannyls::ErrorKind::InvalidInput,
        "InconsistentState" => cannyls::ErrorKind::InconsistentState,
        _ => cannyls::ErrorKind::Other,
    };
    Ok(kind.takes_over(e).into())
});

#[derive(Debug, Default)]
pub struct ErrorEncoder {
    inner: MessageEncoder<MessageFieldEncoder<F1, PreEncode<trackable::ErrorEncoder>>>,
}
impl_sized_message_encode!(ErrorEncoder, cannyls::Error, |item: Self::Item| {
    // NOTE: `cannyls::ErrorKind`の定義変更を検出しやすくするために、あえて明示的に列挙している
    let kind = match *item.kind() {
        cannyls::ErrorKind::StorageFull => "StorageFull".to_owned(),
        cannyls::ErrorKind::StorageCorrupted => "StorageCorrupted".to_owned(),
        cannyls::ErrorKind::DeviceBusy => "DeviceBusy".to_owned(),
        cannyls::ErrorKind::DeviceTerminated => "DeviceTerminated".to_owned(),
        cannyls::ErrorKind::InvalidInput => "InvalidInput".to_owned(),
        cannyls::ErrorKind::InconsistentState => "InconsistentState".to_owned(),
        cannyls::ErrorKind::Other => "Other".to_owned(),
    };
    kind.takes_over(item)
});

#[derive(Debug, Default)]
pub struct PutLumpResponseDecoder {
    inner: MessageDecoder<
        Oneof<(
            FieldDecoder<F1, BoolDecoder>,
            MessageFieldDecoder<F2, ErrorDecoder>,
        )>,
    >,
}
impl_message_decode!(PutLumpResponseDecoder, cannyls::Result<bool>, |item| Ok(
    branch_into_result(item)
));

#[derive(Debug, Default)]
pub struct PutLumpResponseEncoder {
    inner: MessageEncoder<
        Oneof<(
            FieldEncoder<F1, BoolEncoder>,
            MessageFieldEncoder<F2, ErrorEncoder>,
        )>,
    >,
}
impl_sized_message_encode!(
    PutLumpResponseEncoder,
    cannyls::Result<bool>,
    |item: Self::Item| result_into_branch(item)
);

pub type DeleteLumpRequestDecoder = PutLumpResponseDecoder;
pub type DeleteLumpRequestEncoder = PutLumpResponseEncoder;

#[derive(Debug, Default)]
pub struct HeadLumpResponseDecoder {
    inner: MessageDecoder<
        Optional<
            Oneof<(
                FieldDecoder<F1, Uint32Decoder>,
                MessageFieldDecoder<F2, ErrorDecoder>,
            )>,
        >,
    >,
}
impl_message_decode!(
    HeadLumpResponseDecoder,
    cannyls::Result<Option<LumpHeader>>,
    |item| {
        let item = branch_into_optional_result(item);
        Ok(item.map(|v| {
            v.map(|n| LumpHeader {
                approximate_data_size: n,
            })
        }))
    }
);

#[derive(Debug, Default)]
pub struct HeadLumpResponseEncoder {
    inner: MessageEncoder<
        Optional<
            Oneof<(
                FieldEncoder<F1, Uint32Encoder>,
                MessageFieldEncoder<F2, ErrorEncoder>,
            )>,
        >,
    >,
}
impl_sized_message_encode!(
    HeadLumpResponseEncoder,
    cannyls::Result<Option<LumpHeader>>,
    |item: Self::Item| optional_result_into_branch(
        item.map(|h| h.map(|h| h.approximate_data_size))
    )
);

#[derive(Debug, Default)]
pub struct GetLumpResponseDecoder {
    inner: MessageDecoder<
        Optional<
            Oneof<(
                FieldDecoder<F1, BytesDecoder>,
                MessageFieldDecoder<F2, ErrorDecoder>,
            )>,
        >,
    >,
}
impl_message_decode!(
    GetLumpResponseDecoder,
    cannyls::Result<Option<LumpData>>,
    |item| {
        let item = branch_into_optional_result(item);
        match item {
            Ok(Some(data)) => {
                let data = track!(LumpData::new(data))
                    .map_err(|e| bytecodec::ErrorKind::InvalidInput.takes_over(e))?;
                Ok(Ok(Some(data)))
            }
            Ok(None) => Ok(Ok(None)),
            Err(e) => Ok(Err(e)),
        }
    }
);

#[derive(Debug, Default)]
pub struct GetLumpResponseEncoder {
    inner: MessageEncoder<
        Optional<
            Oneof<(
                FieldEncoder<F1, BytesEncoder<LumpData>>,
                MessageFieldEncoder<F2, ErrorEncoder>,
            )>,
        >,
    >,
}
impl_sized_message_encode!(
    GetLumpResponseEncoder,
    cannyls::Result<Option<LumpData>>,
    |item: Self::Item| optional_result_into_branch(item)
);

#[derive(Debug, Default)]
pub struct DeviceRequestDecoder {
    inner: MessageDecoder<
        Fields<(
            FieldDecoder<F1, StringDecoder>,
            MessageFieldDecoder<F2, RequestOptionsDecoder>,
        )>,
    >,
}
impl_message_decode!(DeviceRequestDecoder, DeviceRequest, |(
    device_id,
    options,
)| Ok(DeviceRequest {
    device_id: DeviceId::new(device_id),
    options,
}));

#[derive(Debug, Default)]
pub struct DeviceRequestEncoder {
    inner: MessageEncoder<
        Fields<(
            FieldEncoder<F1, StringEncoder>,
            MessageFieldEncoder<F2, RequestOptionsEncoder>,
        )>,
    >,
}
impl_sized_message_encode!(DeviceRequestEncoder, DeviceRequest, |item: Self::Item| (
    item.device_id.into_string(),
    item.options
));

#[derive(Debug, Default)]
pub struct UsageRangeRequestDecoder {
    inner: MessageDecoder<
        Fields<(
            MaybeDefault<FieldDecoder<F1, StringDecoder>>,
            MessageFieldDecoder<F2, LumpIdDecoder>,
            MessageFieldDecoder<F3, LumpIdDecoder>,
            MessageFieldDecoder<F4, RequestOptionsDecoder>,
        )>,
    >,
}
impl_message_decode!(UsageRangeRequestDecoder, UsageRangeRequest, |(
    device_id,
    start,
    end,
    options,
)| Ok(
    UsageRangeRequest {
        device_id: DeviceId::new(device_id),
        range: Range { start, end },
        options,
    }
));

#[derive(Debug, Default)]
pub struct UsageRangeRequestEncoder {
    inner: MessageEncoder<
        Fields<(
            MaybeDefault<FieldEncoder<F1, StringEncoder>>,
            MessageFieldEncoder<F2, LumpIdEncoder>,
            MessageFieldEncoder<F3, LumpIdEncoder>,
            MessageFieldEncoder<F4, RequestOptionsEncoder>,
        )>,
    >,
}
impl_sized_message_encode!(
    UsageRangeRequestEncoder,
    UsageRangeRequest,
    |item: Self::Item| (
        item.device_id.into_string(),
        item.range.start,
        item.range.end,
        item.options,
    )
);

#[derive(Debug, Default)]
pub struct ListLumpResponseDecoder {
    inner: MessageDecoder<
        Fields<(
            Repeated<MessageFieldDecoder<F1, LumpIdDecoder>, Vec<LumpId>>,
            Optional<MessageFieldDecoder<F2, ErrorDecoder>>,
        )>,
    >,
}
impl_message_decode!(ListLumpResponseDecoder, cannyls::Result<Vec<LumpId>>, |(
    ids,
    error,
)| if let Some(error) =
    error
{
    Ok(Err(error))
} else {
    Ok(Ok(ids))
});

#[derive(Debug, Default)]
pub struct ListLumpResponseEncoder {
    inner: MessageEncoder<
        Fields<(
            Repeated<MessageFieldEncoder<F1, LumpIdEncoder>, Vec<LumpId>>,
            Optional<MessageFieldEncoder<F2, ErrorEncoder>>,
        )>,
    >,
}
impl_message_encode!(
    ListLumpResponseEncoder,
    cannyls::Result<Vec<LumpId>>,
    |item: Self::Item| match item {
        Err(e) => (Vec::new(), Some(e)),
        Ok(ids) => (ids, None),
    }
);

#[derive(Debug, Default)]
pub struct UsageRangeResponseDecoder {
    inner: MessageDecoder<
        Fields<(
            MaybeDefault<MessageFieldDecoder<F1, StorageUsageDecoder>>,
            Optional<MessageFieldDecoder<F2, ErrorDecoder>>,
        )>,
    >,
}
impl_message_decode!(
    UsageRangeResponseDecoder,
    cannyls::Result<StorageUsage>,
    |(usage, error)| if let Some(error) = error {
        Ok(Err(error))
    } else {
        Ok(Ok(usage))
    }
);

#[derive(Debug, Default)]
pub struct UsageRangeResponseEncoder {
    inner: MessageEncoder<
        Fields<(
            MessageFieldEncoder<F1, StorageUsageEncoder>,
            Optional<MessageFieldEncoder<F2, ErrorEncoder>>,
        )>,
    >,
}
impl_sized_message_encode!(
    UsageRangeResponseEncoder,
    cannyls::Result<StorageUsage>,
    |item: Self::Item| match item {
        Err(e) => (Default::default(), Some(e)),
        Ok(usage) => (usage, None),
    }
);

fn result_into_branch<T, E>(result: std::result::Result<T, E>) -> Branch2<T, E> {
    match result {
        Ok(a) => Branch2::A(a),
        Err(b) => Branch2::B(b),
    }
}

fn optional_result_into_branch<T, E>(
    result: std::result::Result<Option<T>, E>,
) -> Option<Branch2<T, E>> {
    match result {
        Ok(Some(a)) => Some(Branch2::A(a)),
        Ok(None) => None,
        Err(b) => Some(Branch2::B(b)),
    }
}

fn branch_into_result<T, E>(branch: Branch2<T, E>) -> std::result::Result<T, E> {
    match branch {
        Branch2::A(a) => Ok(a),
        Branch2::B(b) => Err(b),
    }
}

fn branch_into_optional_result<T, E>(
    branch: Option<Branch2<T, E>>,
) -> std::result::Result<Option<T>, E> {
    match branch {
        None => Ok(None),
        Some(Branch2::A(a)) => Ok(Some(a)),
        Some(Branch2::B(b)) => Err(b),
    }
}

#[cfg(test)]
mod tests {
    use bytecodec::{DecodeExt, EncodeExt};
    use cannyls::deadline::Deadline;
    use std::time::Duration;

    use super::*;

    macro_rules! assert_encdec {
        ($encoder:ty, $decoder:ty, $value:expr) => {
            let mut encoder: $encoder = Default::default();
            let mut decoder: $decoder = Default::default();

            let bytes = track_try_unwrap!(encoder.encode_into_bytes($value()));
            let decoded = track_try_unwrap!(track!(decoder.decode_from_bytes(&bytes); bytes));
            assert_eq!(decoded, $value());
        };
    }

    #[test]
    fn lump_id_encdec_works() {
        assert_encdec!(LumpIdEncoder, LumpIdDecoder, || LumpId::new(0));
        assert_encdec!(LumpIdEncoder, LumpIdDecoder, || LumpId::new(
            (123 << 64) | 345
        ));
    }

    #[test]
    fn deadline_encdec_works() {
        assert_encdec!(DeadlineEncoder, DeadlineDecoder, || Deadline::Immediate);
        assert_encdec!(DeadlineEncoder, DeadlineDecoder, || Deadline::Infinity);
        assert_encdec!(DeadlineEncoder, DeadlineDecoder, || Deadline::Within(
            Duration::from_secs(0)
        ));
        assert_encdec!(DeadlineEncoder, DeadlineDecoder, || Deadline::Within(
            Duration::from_millis(123)
        ));
    }

    #[test]
    fn request_options_encdec_works() {
        assert_encdec!(RequestOptionsEncoder, RequestOptionsDecoder, || {
            RequestOptions {
                deadline: Deadline::Immediate,
                max_queue_len: None,
            }
        });
        assert_encdec!(RequestOptionsEncoder, RequestOptionsDecoder, || {
            RequestOptions {
                deadline: Deadline::Infinity,
                max_queue_len: Some(0),
            }
        });
        assert_encdec!(RequestOptionsEncoder, RequestOptionsDecoder, || {
            RequestOptions {
                deadline: Deadline::Infinity,
                max_queue_len: Some(123),
            }
        });
    }

    #[test]
    fn usage_range_request_encdec_works() {
        let request = UsageRangeRequest {
            device_id: DeviceId::new("device"),
            range: Range {
                start: LumpId::new(1),
                end: LumpId::new(3),
            },
            options: RequestOptions {
                deadline: Deadline::Infinity,
                max_queue_len: Some(123),
            },
        };
        assert_encdec!(UsageRangeRequestEncoder, UsageRangeRequestDecoder, || {
            request.clone()
        });
    }
}
