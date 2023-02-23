use super::error::Context;
use super::error::DatenLordResult;
use serde::de::DeserializeOwned;
use std::fmt::Write;

/// Decode from bytes
#[inline]
pub fn decode_from_bytes<T: DeserializeOwned>(bytes: &[u8]) -> DatenLordResult<T> {
    let decoded_value = bincode::deserialize(bytes)
        .with_context(|| format!("failed to decode bytes to {}", std::any::type_name::<T>(),))?;
    Ok(decoded_value)
}

/// Format `anyhow::Error`
#[must_use]
#[inline]
pub fn format_anyhow_error(error: &anyhow::Error) -> String {
    let err_msg_vec = anyhow::Error::chain(error)
        .map(std::string::ToString::to_string)
        .collect::<Vec<_>>();
    let mut err_msg = err_msg_vec.as_slice().join(", caused by: ");
    write!(err_msg, ", root cause: {}", error.root_cause()).unwrap_or_else(|e| {
        panic!("write str err, {}", e);
    });
    err_msg
}
