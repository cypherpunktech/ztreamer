//! The JSON forms of the fields that are not plain serde: hex bytes and `Display` strings.
//!
//! A field names a module of this file with `#[serde(with = "crate::json::hex_bytes")]` or
//! `#[serde(with = "crate::json::display")]`.

/// Bytes as a lowercase hex string.
pub(crate) mod hex_bytes {
    use std::borrow::Cow;

    use serde::{Deserialize, Deserializer, Serializer, de::Error as _};

    /// Writes `bytes` as lowercase hex.
    pub(crate) fn serialize<S: Serializer>(
        bytes: &impl AsRef<[u8]>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&hex::encode(bytes))
    }

    /// Reads a hex string. Refuses a digit that is not hex and a length that `T` refuses.
    pub(crate) fn deserialize<'de, D: Deserializer<'de>, T: TryFrom<Vec<u8>>>(
        deserializer: D,
    ) -> Result<T, D::Error> {
        let hex = Cow::<'de, str>::deserialize(deserializer)?;
        let bytes = hex::decode(&*hex).map_err(D::Error::custom)?;
        let length = bytes.len();
        T::try_from(bytes).map_err(|_| {
            D::Error::custom(format_args!(
                "Can't convert a Byte Vector of length {length} to the output type."
            ))
        })
    }
}

/// A value as its `Display` string, read back with `FromStr`.
pub(crate) mod display {
    use std::{borrow::Cow, fmt::Display, str::FromStr};

    use serde::{Deserialize, Deserializer, Serializer, de::Error as _};

    /// Writes the `Display` form of `value`.
    pub(crate) fn serialize<S: Serializer>(
        value: &impl Display,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.collect_str(value)
    }

    /// Reads a string and parses it. Refuses a string that `T` refuses.
    pub(crate) fn deserialize<'de, D: Deserializer<'de>, T: FromStr<Err: Display>>(
        deserializer: D,
    ) -> Result<T, D::Error> {
        Cow::<'de, str>::deserialize(deserializer)?.parse().map_err(D::Error::custom)
    }
}
