cannyls_rpc
============

[![Crates.io: cannyls_rpc](https://img.shields.io/crates/v/cannyls_rpc.svg)](https://crates.io/crates/cannyls_rpc)
[![Documentation](https://docs.rs/cannyls_rpc/badge.svg)](https://docs.rs/cannyls_rpc)
[![Build Status](https://travis-ci.org/frugalos/cannyls_rpc.svg?branch=master)](https://travis-ci.org/frugalos/cannyls_rpc)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

RPC library for operating [`cannyls`]'s devices from remote nodes.

[Documentation](https://docs.rs/cannyls_rpc/)


Procedure ID Namespace
-----------------------

`cannyls_rpc` provides RPC functionalities by using the [`fibers_rpc`] crate.
And `cannyls_rpc` uses some procedure IDs ([`ProcedureId`]) in the range between `0x0001_0000` and `0x0001_FFFF`.
Thus, for ensuring to avoid ID conflict with `cannyls_rpc`, other RPCs that will be registered with the same RPC server must select procedure IDs that are not included in the above range.

[`cannyls`]: https://github.com/frugalos/cannyls
[`fibers_rpc`]: https://github.com/sile/fibers_rpc
[`ProcedureId`]: https://docs.rs/fibers_rpc/0.2/fibers_rpc/struct.ProcedureId.html
