use serde::Serializer;
use std::fmt;

use serde::{
    de::{self, Unexpected, Visitor},
    Deserialize, Deserializer, Serialize,
};

#[derive(Clone, Copy, Debug, Derivative, Eq, PartialEq)]
pub enum Concurrency {
    None,
    Adaptive,
    Fixed(usize),
}

impl Serialize for Concurrency {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match &self {
            Concurrency::None => serializer.serialize_str("none"),
            Concurrency::Adaptive => serializer.serialize_str("adaptive"),
            Concurrency::Fixed(i) => serializer.serialize_u64(*i as u64),
        }
    }
}

impl Default for Concurrency {
    fn default() -> Self {
        Self::None
    }
}

impl Concurrency {
    pub const fn if_none(self, other: Self) -> Self {
        match self {
            Self::None => other,
            _ => self,
        }
    }

    pub const fn parse_concurrency(&self, default: Self) -> Option<usize> {
        match self.if_none(default) {
            Concurrency::None | Concurrency::Adaptive => None,
            Concurrency::Fixed(limit) => Some(limit),
        }
    }
}

pub const fn concurrency_is_none(concurrency: &Concurrency) -> bool {
    matches!(concurrency, Concurrency::None)
}

impl<'de> Deserialize<'de> for Concurrency {
    // Deserialize either a positive integer or the string "adaptive"
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct UsizeOrAdaptive;

        impl<'de> Visitor<'de> for UsizeOrAdaptive {
            type Value = Concurrency;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str(r#"positive integer, "adaptive", or "none" "#)
            }

            fn visit_str<E: de::Error>(self, value: &str) -> Result<Concurrency, E> {
                if value == "adaptive" {
                    Ok(Concurrency::Adaptive)
                } else if value == "none" {
                    Ok(Concurrency::None)
                } else {
                    Err(de::Error::unknown_variant(value, &["adaptive"]))
                }
            }

            fn visit_i64<E: de::Error>(self, value: i64) -> Result<Concurrency, E> {
                if value > 0 {
                    Ok(Concurrency::Fixed(value as usize))
                } else {
                    Err(de::Error::invalid_value(
                        Unexpected::Signed(value),
                        &"positive integer",
                    ))
                }
            }

            fn visit_u64<E: de::Error>(self, value: u64) -> Result<Concurrency, E> {
                if value > 0 {
                    Ok(Concurrency::Fixed(value as usize))
                } else {
                    Err(de::Error::invalid_value(
                        Unexpected::Unsigned(value),
                        &"positive integer",
                    ))
                }
            }
        }

        deserializer.deserialize_any(UsizeOrAdaptive)
    }
}

#[test]
fn is_serialization_reversible() {
    let variants = [
        Concurrency::None,
        Concurrency::Adaptive,
        Concurrency::Fixed(8),
    ];

    for v in variants {
        let value = serde_json::to_value(v).unwrap();
        let deserialized = serde_json::from_value::<Concurrency>(value)
            .expect("Failed to deserialize a previously serialized Concurrency value");

        assert_eq!(v, deserialized)
    }
}
