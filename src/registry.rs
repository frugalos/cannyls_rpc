use atomic_immut::AtomicImmut;
use cannyls::deadline::Deadline;
use cannyls::device::{Device, DeviceHandle};
use cannyls::{Error, ErrorKind, Result};
use fibers::sync::mpsc;
use futures::{Async, Future, Poll, Stream};
use slog::Logger;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::{Arc, Mutex};
use trackable::error::ErrorKindExt;

use device::DeviceId;

type DeviceHandles = Arc<AtomicImmut<HashMap<DeviceId, Mutex<DeviceHandle>>>>;

/// デバイスレジストリ.
///
/// RPC経由でデバイスにアクセスするためには、
/// 事前にここに登録されている必要がある.
/// (かつ、そのレジストリ用のRPCサーバが起動している必要がある)
#[derive(Debug)]
pub struct DeviceRegistry {
    logger: Logger,

    // 登録デバイス群.
    devices: HashMap<DeviceId, DeviceState>,

    // デバイスの検索やそれに対する操作は、
    // (性能上の理由から)レジストリに対するコマンド送受信を経由せずに直接行いたいので、
    // デバイスハンドルは別で管理する.
    // (`AtomicImmut`を使うことで、利用側が直接デバイスの検索等を行える)
    device_handles: DeviceHandles,

    // レジストリに対するコマンド送受信チャンネル.
    command_tx: mpsc::Sender<Command>,
    command_rx: mpsc::Receiver<Command>,

    // レジストリが停止中かどうかを示すためのフラグ.
    being_stopped: bool,
}
impl DeviceRegistry {
    /// 新しいレジストリインスタンスを生成する.
    pub fn new(logger: Logger) -> Self {
        let (command_tx, command_rx) = mpsc::channel();
        DeviceRegistry {
            logger,
            devices: HashMap::new(),
            device_handles: DeviceHandles::default(),
            command_tx,
            command_rx,
            being_stopped: false,
        }
    }

    /// レジストリを操作するためのハンドルを返す.
    pub fn handle(&self) -> DeviceRegistryHandle {
        DeviceRegistryHandle {
            command_tx: self.command_tx.clone(),
            device_handles: Arc::clone(&self.device_handles),
        }
    }

    /// レジストリの停止処理を開始する.
    ///
    /// 単に止めたいだけであれば、レジストリインスタンスを破棄するだけでも良いが、
    /// このメソッドを使用することで、登録デバイスのグレースフルな停止が可能となる.
    ///
    /// 具体的には、以下の処理が実行される:
    /// 1. 以後は、レジストリに対するデバイス登録要求、は全て無視される
    /// 2. 全登録デバイスに停止命令が発行される
    /// 3. 全デバイスが停止したら、レジストリ自体を停止する
    ///    - i.e., `Future:poll`の結果が`Ok(Async::Ready(()))`となる
    pub fn stop(&mut self) {
        info!(self.logger, "Starts being_stopped all devices");
        for (id, state) in &mut self.devices {
            if state.terminated {
                info!(self.logger, "Device {:?} already has been terminated", id);
            } else {
                state.device.stop(Deadline::Immediate)
            }
        }
        self.being_stopped = true;
    }

    /// デバイスを止める。
    /// 対象とするデバイスが見つかった場合は true を、見つからなかった場合は false を返す。
    pub fn stop_device(&mut self, device_id: &DeviceId) -> bool {
        info!(self.logger, "Stopping device: {:?}", device_id);
        if let Some(state) = self.devices.get(device_id) {
            if state.terminated {
                info!(
                    self.logger,
                    "stop_device: Device {:?} has already been terminated", device_id
                );
            } else {
                state.device.stop(Deadline::Immediate);
            }
            true
        } else {
            info!(self.logger, "Invalid device ID: {:?}", device_id);
            false
        }
    }

    fn handle_command(&mut self, command: Command) {
        match command {
            Command::PutDevice(id, device) => self.handle_put_device(&id, device),
            Command::DeleteDevice(id) => self.handle_delete_device(&id),
        }
    }

    fn handle_put_device(&mut self, id: &DeviceId, device: Device) {
        if self.being_stopped {
            warn!(
                self.logger,
                "`PutDevice({:?}, _)` was ignored (the registry is being stopped)", id
            );
            return;
        }

        info!(self.logger, "PUT device: {:?}", id);
        let old = self.devices.insert(id.clone(), DeviceState::new(device));
        if old.is_some() {
            warn!(self.logger, "Old device was removed: {:?}", id);
        }
        self.refresh_device_handles();
    }

    fn handle_delete_device(&mut self, id: &DeviceId) {
        info!(self.logger, "DELETE device: {:?}", id);
        if self.devices.remove(id).is_some() {
            self.refresh_device_handles();
        } else {
            warn!(self.logger, "No such device: {:?}", id);
        }
    }

    fn refresh_device_handles(&mut self) {
        let device_handles = self
            .devices
            .iter()
            .map(|(id, s)| (id.clone(), Mutex::new(s.device.handle())))
            .collect();
        self.device_handles.store(device_handles);
    }
}
impl Future for DeviceRegistry {
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        while let Async::Ready(command) = self.command_rx.poll().expect("Never fails") {
            let command = command.expect("DeviceRegistryが`command_tx`を保持しているので、このストリームが終端することはない");
            self.handle_command(command);
        }

        for (id, state) in &mut self.devices {
            if state.terminated {
                continue;
            }
            match track!(state.device.poll()) {
                Err(e) => {
                    error!(self.logger, "Device {:?} terminated abnormally: {}", id, e);
                    state.terminated = true;
                }
                Ok(Async::Ready(())) => {
                    info!(self.logger, "Device {:?} terminated normally", id);
                    state.terminated = true;
                }
                Ok(Async::NotReady) => {}
            }
        }
        if self.being_stopped && self.devices.values().all(|d| d.terminated) {
            info!(self.logger, "All devices have stopped");
            Ok(Async::Ready(()))
        } else {
            Ok(Async::NotReady)
        }
    }
}
impl Drop for DeviceRegistry {
    fn drop(&mut self) {
        self.device_handles.store(HashMap::new());
    }
}

/// デバイスレジストリを操作するためのハンドル.
#[derive(Debug, Clone)]
pub struct DeviceRegistryHandle {
    command_tx: mpsc::Sender<Command>,
    device_handles: DeviceHandles,
}
impl DeviceRegistryHandle {
    /// レジストリにデバイスを登録する.
    ///
    /// 同じIDのデバイスが既に存在する場合には、新しいもので上書きされる.
    ///
    /// デバイスが途中で終了した場合でも、明示的に削除するまでは、レジストリには残り続ける.
    ///
    /// # Errors
    ///
    /// 対象レジストリインスタンスがドロップしている場合には、`ErrorKind::Other`エラーが返る.
    pub fn put_device(&self, device_id: DeviceId, device: Device) -> Result<()> {
        let command = Command::PutDevice(device_id, device);
        track_assert!(self.command_tx.send(command).is_ok(), ErrorKind::Other);
        Ok(())
    }

    /// レジストリからデバイスを削除する.
    ///
    /// 指定されたデバイスが存在しない場合には、単に無視される.
    ///
    /// # Errors
    ///
    /// 対象レジストリインスタンスがドロップしている場合には、`ErrorKind::Other`エラーが返る.
    pub fn delete_device(&self, device_id: DeviceId) -> Result<()> {
        let command = Command::DeleteDevice(device_id);
        track_assert!(self.command_tx.send(command).is_ok(), ErrorKind::Other);
        Ok(())
    }

    /// レジストリに登録されているデバイスを取得する.
    ///
    /// 未登録のデバイスが指定された場合には`None`が返される.
    ///
    /// # 注意
    ///
    /// この操作は`DeviceRegistry`とハンドルを繋ぐチャンネルを経由せずに行われる.
    /// そのため、並列度が高い場合のスケーラビリティには優れるが、
    /// `put_device`や`delete_device`とは異なり、処理順序が直列化されることは、
    /// 念頭においておく必要がある.
    /// 例えば`put_device`の呼び出し直後に、`get_device`を呼び出した場合には、
    /// `None`が返ってくる可能性がある.
    ///
    /// また、レジストリインスタンスがドロップされている場合には、常に`None`が返される.
    ///
    /// # Errors
    ///
    /// デバイス用のロック獲得に失敗した場合には、`ErrorKind::Other`エラーが返される.
    ///
    /// 存在しないデバイスが指定された場合には、`ErrorKind::InvalidInput`エラーが返される.
    pub fn get_device<T>(&self, device_id: &T) -> Result<DeviceHandle>
    where
        T: Hash + Eq + Debug + ?Sized,
        DeviceId: Borrow<T>,
    {
        if let Some(d) = self.device_handles.load().get(device_id) {
            let d = track!(d.lock().map_err(|e| ErrorKind::Other.cause(e.to_string())))?;
            Ok(d.clone())
        } else {
            track_panic!(ErrorKind::InvalidInput, "No such device: {:?}", device_id);
        }
    }

    /// レジストリにデバイスが登録されているかどうかを判定する.
    pub fn contains_device(&self, device_id: &DeviceId) -> bool {
        self.device_handles.load().contains_key(device_id)
    }

    /// レジストリに登録されているデバイス一覧を取得する.
    ///
    /// # Errors
    ///
    /// デバイス用のロック獲得に失敗した場合には、`ErrorKind::Other`エラーが返される.
    pub fn list_devices(&self) -> Result<Vec<(DeviceId, DeviceHandle)>> {
        let mut devices = Vec::new();
        for (id, d) in self.device_handles.load().iter() {
            let d = track!(d.lock().map_err(|e| ErrorKind::Other.cause(e.to_string())))?;
            devices.push((id.clone(), d.clone()));
        }
        Ok(devices)
    }
}

#[derive(Debug)]
enum Command {
    PutDevice(DeviceId, Device),
    DeleteDevice(DeviceId),
}

#[derive(Debug)]
struct DeviceState {
    device: Device,
    terminated: bool,
}
impl DeviceState {
    fn new(device: Device) -> Self {
        DeviceState {
            device,
            terminated: false,
        }
    }
}
