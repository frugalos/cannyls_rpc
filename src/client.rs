use cannyls::deadline::Deadline;
use cannyls::lump::{LumpData, LumpHeader, LumpId};
use cannyls::storage::StorageUsage;
use cannyls::{Error, ErrorKind, Result};
use fibers_rpc::{self, Call};
use futures::{Async, Future, Poll};
use std::net::SocketAddr;
use std::ops::Range;
use trackable::error::ErrorKindExt;

use device::DeviceId;
use rpc;

/// RPCクライアント.
#[derive(Debug, Clone)]
pub struct Client {
    server: SocketAddr,
    rpc_service: fibers_rpc::client::ClientServiceHandle,
}
impl Client {
    /// 新しい`Client`インスタンスを生成する.
    pub fn new(server: SocketAddr, rpc_service: fibers_rpc::client::ClientServiceHandle) -> Self {
        Client {
            server,
            rpc_service,
        }
    }

    /// RPCリクエスト発行用のビルダを返す.
    pub fn request(&self) -> RequestBuilder {
        RequestBuilder::new(self)
    }
}

/// RPCリクエストビルダ.
#[derive(Debug)]
pub struct RequestBuilder<'a> {
    client: &'a Client,
    deadline: Option<Deadline>,
    max_queue_len: Option<usize>,
    rpc_options: fibers_rpc::client::Options,
}
impl<'a> RequestBuilder<'a> {
    /// リクエスト処理のデッドライン(優先度)を指定する.
    ///
    /// デフォルト値は`Deadline::Infinity`.
    pub fn deadline(&mut self, deadline: Deadline) -> &mut Self {
        self.deadline = Some(deadline);
        self
    }

    /// リクエスト処理時のデバイスのキューの長さ制限を指定する.
    ///
    /// このリクエストの処理時に、デバイスの要求キューの長さが、指定された値を超えている場合には、
    /// リクエストは処理されずに`ErrorKind::DeviceBusy`エラーが返される.
    ///
    /// デフォルトでは制限なし.
    pub fn max_queue_len(&mut self, n: usize) -> &mut Self {
        self.max_queue_len = Some(n);
        self
    }

    /// RPCレベルのオプションを指定する.
    ///
    /// デフォルト値は`fibers_rpc::client::Options::default()`.
    pub fn rpc_options(&mut self, options: fibers_rpc::client::Options) -> &mut Self {
        self.rpc_options = options;
        self
    }

    /// Lumpデータの取得を行う.
    ///
    /// 指定されたlumpが存在しない場合には`Ok(None)`が返される.
    ///
    /// # Errors
    ///
    /// 例えば、以下のようなエラーが返されることがある:
    /// - 指定されたデバイスが存在しない場合には`ErrorKind::InvalidInput`
    /// - 指定されたデバイスが現在利用不可能な場合には`ErrorKind::DeviceBusy`
    pub fn get_lump(
        &self,
        device_id: DeviceId,
        lump_id: LumpId,
    ) -> impl Future<Item = Option<Vec<u8>>, Error = Error> {
        let mut client = rpc::GetLumpRpc::client(&self.client.rpc_service);
        *client.options_mut() = self.rpc_options.clone();

        let request = self.lump_request(device_id, lump_id);
        let future = Response::new(self.client.server, client.call(self.client.server, request));
        future.map(|data| data.map(|d| d.into_bytes()))
    }

    /// Lumpヘッダ(要約情報)の取得を行う.
    ///
    /// 指定されたlumpが存在しない場合には`Ok(None)`が返される.
    ///
    /// # Errors
    ///
    /// 例えば、以下のようなエラーが返されることがある:
    /// - 指定されたデバイスが存在しない場合には`ErrorKind::InvalidInput`
    /// - 指定されたデバイスが現在利用不可能な場合には`ErrorKind::DeviceBusy`
    pub fn head_lump(
        &self,
        device_id: DeviceId,
        lump_id: LumpId,
    ) -> impl Future<Item = Option<LumpHeader>, Error = Error> {
        let mut client = rpc::HeadLumpRpc::client(&self.client.rpc_service);
        *client.options_mut() = self.rpc_options.clone();

        let request = self.lump_request(device_id, lump_id);
        Response::new(self.client.server, client.call(self.client.server, request))
    }

    /// Lumpの保存を行う.
    ///
    /// 返り値が`Ok(true)`の場合には新規作成が、`Ok(false)`の場合には上書きが、行われたことを表している.
    ///
    /// # Errors
    ///
    /// 例えば、以下のようなエラーが返されることがある:
    /// - 指定されたデバイスが存在しない場合には`ErrorKind::InvalidInput`
    /// - 指定されたデバイスが現在利用不可能な場合には`ErrorKind::DeviceBusy`
    /// - 指定されたデバイスの容量が満杯になっている場合には`ErrorKind::Full`
    ///
    /// # 注意
    ///
    /// 現在は指定されたデータをジャーナル領域に埋め込むかどうかはRPCサーバ側が決定するため
    /// `LumpData::new_embedded`関数を用いて`LumpData`を生成しても意味はない.
    pub fn put_lump(
        &self,
        device_id: DeviceId,
        lump_id: LumpId,
        lump_data: LumpData,
    ) -> impl Future<Item = bool, Error = Error> {
        let mut client = rpc::PutLumpRpc::client(&self.client.rpc_service);
        *client.options_mut() = self.rpc_options.clone();

        let request = rpc::PutLumpRequest {
            device_id,
            lump_id,
            lump_data,
            options: self.request_options(),
        };
        Response::new(self.client.server, client.call(self.client.server, request))
    }

    /// Lumpの削除を行う.
    ///
    /// 返り値が`Ok(true)`の場合には削除が行われたことを、
    /// `Ok(false)`の場合には対象lumpが存在しなかったことを、表している.
    ///
    /// # Errors
    ///
    /// 例えば、以下のようなエラーが返されることがある:
    /// - 指定されたデバイスが存在しない場合には`ErrorKind::InvalidInput`
    /// - 指定されたデバイスが現在利用不可能な場合には`ErrorKind::DeviceBusy`
    pub fn delete_lump(
        &self,
        device_id: DeviceId,
        lump_id: LumpId,
    ) -> impl Future<Item = bool, Error = Error> {
        let mut client = rpc::DeleteLumpRpc::client(&self.client.rpc_service);
        *client.options_mut() = self.rpc_options.clone();

        let request = self.lump_request(device_id, lump_id);
        Response::new(self.client.server, client.call(self.client.server, request))
    }

    /// デバイスに保存されているlumpのID一覧を取得する.
    ///
    /// # Errors
    ///
    /// 例えば、以下のようなエラーが返されることがある:
    /// - 指定されたデバイスが存在しない場合には`ErrorKind::InvalidInput`
    /// - 指定されたデバイスが現在利用不可能な場合には`ErrorKind::DeviceBusy`
    pub fn list_lumps(
        &self,
        device_id: DeviceId,
    ) -> impl Future<Item = Vec<LumpId>, Error = Error> {
        let mut client = rpc::ListLumpRpc::client(&self.client.rpc_service);
        *client.options_mut() = self.rpc_options.clone();

        let request = rpc::DeviceRequest {
            device_id,
            options: self.request_options(),
        };
        Response::new(self.client.server, client.call(self.client.server, request))
    }

    /// lumpの範囲を指定してデバイスのストレージ使用量を取得する.
    ///
    /// # Errors
    ///
    /// 例えば、以下のようなエラーが返されることがある:
    /// - 指定されたデバイスが存在しない場合には`ErrorKind::InvalidInput`
    /// - 指定されたデバイスが現在利用不可能な場合には`ErrorKind::DeviceBusy`
    pub fn usage_range(
        &self,
        device_id: DeviceId,
        range: Range<LumpId>,
    ) -> impl Future<Item = StorageUsage, Error = Error> {
        let mut client = rpc::UsageRangeRpc::client(&self.client.rpc_service);
        *client.options_mut() = self.rpc_options.clone();

        let request = rpc::UsageRangeRequest {
            device_id,
            range,
            options: self.request_options(),
        };
        Response::new(self.client.server, client.call(self.client.server, request))
    }

    /// lump の範囲を指定して削除し対象となった lump の一覧を返す.
    ///
    /// # Errors
    ///
    /// 例えば、以下のようなエラーが返されることがある:
    /// - 指定されたデバイスが存在しない場合には`ErrorKind::InvalidInput`
    /// - 指定されたデバイスが現在利用不可能な場合には`ErrorKind::DeviceBusy`
    pub fn delete_range(
        &self,
        device_id: DeviceId,
        range: Range<LumpId>,
    ) -> impl Future<Item = Vec<LumpId>, Error = Error> {
        let mut client = rpc::DeleteRangeRpc::client(&self.client.rpc_service);
        *client.options_mut() = self.rpc_options.clone();

        let request = rpc::RangeLumpRequest {
            device_id,
            range,
            options: self.request_options(),
        };
        Response::new(self.client.server, client.call(self.client.server, request))
    }

    fn new(client: &'a Client) -> Self {
        RequestBuilder {
            client,
            deadline: None,
            max_queue_len: None,
            rpc_options: fibers_rpc::client::Options::default(),
        }
    }

    fn lump_request(&self, device_id: DeviceId, lump_id: LumpId) -> rpc::LumpRequest {
        rpc::LumpRequest {
            device_id,
            lump_id,
            options: self.request_options(),
        }
    }

    fn request_options(&self) -> rpc::RequestOptions {
        rpc::RequestOptions {
            deadline: self.deadline.unwrap_or_default(),
            max_queue_len: self.max_queue_len,
        }
    }
}

#[derive(Debug)]
struct Response<T> {
    server: SocketAddr,
    inner: fibers_rpc::client::Response<Result<T>>,
}
impl<T> Response<T> {
    fn new(server: SocketAddr, inner: fibers_rpc::client::Response<Result<T>>) -> Self {
        Response { server, inner }
    }
}
impl<T> Future for Response<T> {
    type Item = T;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.inner.poll() {
            Err(e) => {
                let original_kind = *e.kind();
                let kind = match original_kind {
                    fibers_rpc::ErrorKind::InvalidInput => ErrorKind::InvalidInput,
                    fibers_rpc::ErrorKind::Timeout
                    | fibers_rpc::ErrorKind::Unavailable
                    | fibers_rpc::ErrorKind::Other => ErrorKind::Other,
                };
                Err(track!(kind.takes_over(e); original_kind, self.server).into())
            }
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Ok(Async::Ready(result)) => track!(result.map(Async::Ready); self.server),
        }
    }
}
