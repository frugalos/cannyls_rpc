use std::borrow::Borrow;

/// RPCの対象となるデバイスのID.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DeviceId(String);
impl DeviceId {
    /// 新しい`DeviceId`インスタンスを生成する.
    pub fn new<T: Into<String>>(id: T) -> Self {
        DeviceId(id.into())
    }

    /// デバイスIDを文字列に変換する.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// 所有権を放棄して、内部の文字列を返す.
    pub fn into_string(self) -> String {
        self.0
    }
}
impl AsRef<str> for DeviceId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}
impl Borrow<str> for DeviceId {
    fn borrow(&self) -> &str {
        &self.0
    }
}
