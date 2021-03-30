//! [cannyls]用のRPCライブラリ.
//!
//! # 参考
//!
//! - [cannyls_rpc.proto][cannyls_rpc.proto]: RPC用のメッセージ定義
//! - [protobuf_codec][protobuf_codec]: RPCメッセージをProtocolBuffers形式でエンコード・デコードするためのライブラリ
//! - [fibers_rpc][fibers_rpc]: RPCの通信層用のライブラリ
//!
//! [cannyls]: https://github.com/frugalos/cannyls
//! [cannyls_rpc.proto]: https://github.com/frugalos/cannyls_rpc/blob/master/protobuf/cannyls_rpc.proto
//! [protobuf_codec]: https://crates.io/crates/protobuf_codec
//! [fibers_rpc]: https://crates.io/crates/fibers_rpc
#![warn(missing_docs)]
extern crate atomic_immut;
extern crate bytecodec;
extern crate cannyls;
extern crate factory;
extern crate fibers_rpc;
extern crate futures;
#[macro_use]
extern crate pin_project;
extern crate protobuf_codec;
#[macro_use]
extern crate slog;
#[macro_use]
extern crate trackable;

pub use crate::client::{Client, RequestBuilder};
pub use crate::device::DeviceId;
pub use crate::registry::{DeviceRegistry, DeviceRegistryHandle};
pub use crate::server::Server;

mod client;
mod device;
mod protobuf;
mod registry;
mod rpc;
mod server;
