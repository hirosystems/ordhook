//! Defines error handling types used by the create
//! uses the `error-chain` create for generation

use neon;
use serde::{de, ser};
use std::{convert::From, error, fmt, fmt::Display, result};

pub type Result<T> = result::Result<T, Error>;

#[allow(dead_code)]
#[derive(Debug)]
pub enum Error {
    /// nodejs has a hard coded limit on string length
    /// trying to serialize a string that is too long will result in an error
    StringTooLong { len: usize },

    /// when deserializing to a boolean `false` `undefined` `null` `number`
    /// are valid inputs
    /// any other types will result in error
    UnableToCoerce { to_type: &'static str },

    /// occurs when deserializing a char from an empty string
    EmptyString,

    /// occurs when deserializing a char from a sting with
    /// more than one character
    StringTooLongForChar { len: usize },

    /// occurs when a deserializer expects a `null` or `undefined`
    /// property and found another type
    ExpectingNull,

    /// occurs when deserializing to an enum and the source object has
    /// a none-1 number of properties
    InvalidKeyType { key: String },

    /// an internal deserialization error from an invalid array
    ArrayIndexOutOfBounds { index: u32, length: u32 },

    #[doc(hidden)]
    /// This type of object is not supported
    NotImplemented { name: &'static str },

    /// A JS exception was thrown
    Js { throw: neon::result::Throw },

    /// failed to convert something to f64
    CastError,

    /// Generic serialize error
    Serialize { msg: String },

    /// Generic deserialize error
    Deserialize { msg: String },
}

impl error::Error for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::StringTooLong { len } => {
                "String too long for nodejs len: ".fmt(f)?;
                len.fmt(f)
            }
            Error::UnableToCoerce { to_type } => {
                "Unable to coerce value to type: ".fmt(f)?;
                to_type.fmt(f)
            }
            Error::EmptyString => "EmptyString".fmt(f),
            Error::StringTooLongForChar { len } => {
                "String too long to be a char expected len: 1 got len: ".fmt(f)?;
                len.fmt(f)
            }
            Error::ExpectingNull => "ExpectingNull".fmt(f),
            Error::InvalidKeyType { key } => {
                "Invalid type of key: '".fmt(f)?;
                key.fmt(f)?;
                '\''.fmt(f)
            }
            Error::ArrayIndexOutOfBounds { index, length } => {
                "Array index out of bounds (".fmt(f)?;
                index.fmt(f)?;
                " of ".fmt(f)?;
                length.fmt(f)?;
                ")".fmt(f)
            }
            Error::NotImplemented { name } => {
                "Not implemented: '".fmt(f)?;
                name.fmt(f)?;
                '\''.fmt(f)
            }
            Error::Js { throw: _ } => "JS exception".fmt(f),
            Error::CastError => "Casting error".fmt(f),
            Error::Serialize { msg } => {
                "Serialize error: ".fmt(f)?;
                msg.fmt(f)
            }
            Error::Deserialize { msg } => {
                "Deserialize error: ".fmt(f)?;
                msg.fmt(f)
            }
        }
    }
}

impl ser::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        Self::Serialize {
            msg: msg.to_string(),
        }
    }
}

impl de::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        Self::Deserialize {
            msg: msg.to_string(),
        }
    }
}

impl From<neon::result::Throw> for Error {
    fn from(throw: neon::result::Throw) -> Self {
        Error::Js { throw }
    }
}
