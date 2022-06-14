use std::num::{
    NonZeroI16, NonZeroI32, NonZeroI64, NonZeroI8, NonZeroU16, NonZeroU32, NonZeroU64, NonZeroU8,
};

use num_traits::{Bounded, One, ToPrimitive, Zero};
use serde_json::Number;
use vector_config_common::num::{NUMERIC_ENFORCED_LOWER_BOUND, NUMERIC_ENFORCED_UPPER_BOUND};

/// A numeric type that can be represented correctly in a JSON Schema document.
pub trait ConfigurableNumber {
    /// The integral numeric type.
    ///
    /// We parameterize the "integral" numeric type in this way to allow generating the schema for wrapper types such as
    /// `NonZeroU64`, where the overall type must be represented as `NonZeroU64` but the integeral numeric type that
    /// we're constraining against is `u64`.
    type Numeric: Bounded + ToPrimitive + Zero + One;

    /// Whether or not this numeric type disallows nonzero values.
    fn is_nonzero() -> bool {
        false
    }

    /// Whether or not a generated schema for this numeric type must explicitly disallow zero values.
    ///
    /// In some cases, such as `NonZero*` types from `std::num`, a numeric type may not support zero values for reasons
    /// of correctness and/or optimization. In some cases, we can simply adjust the normal minimum/maximum bounds in the
    /// schema to encode this. In other cases, such as signed versions like `NonZeroI64`, zero is a discrete value
    /// within the minimum and maximum bounds and must be excluded explicitly.
    fn requires_nonzero_exclusion() -> bool {
        false
    }

    /// Gets the JSON encoded version of the zero value for the integral numeric type.
    fn get_encoded_zero_value() -> Number {
        let zero_num_unsigned = Self::Numeric::zero().to_u64().map(Into::into);
        let zero_num_floating = Self::Numeric::zero().to_f64().and_then(Number::from_f64);
        zero_num_unsigned
            .or(zero_num_floating)
            .expect("No usable integer type should be unrepresentable by both `u64` and `f64`.")
    }

    /// Gets the minimum bound for this numeric type, limited by the representable range in JSON Schema.
    fn get_enforced_min_bound() -> f64 {
        let mechanical_minimum = match (Self::is_nonzero(), Self::requires_nonzero_exclusion()) {
            // If the number is not a nonzero type, or it is a nonzero type, but needs an exclusion, we simply return
            // its true mechanical minimum bound. For nonzero types, this is because we can only enforce the nonzero
            // constraint through a negative schema bound, not through its normal minimum/maximum bounds validation.
            (false, _) | (true, true) => Self::Numeric::min_value(),
            // If the number is a nonzero type, but does not need an exclusion, its minimum bound is always 1.
            (true, false) => Self::Numeric::one(),
        };

        let enforced_minimum = NUMERIC_ENFORCED_LOWER_BOUND;
        let mechanical_minimum = mechanical_minimum
            .to_f64()
            .expect("`Configurable` does not support numbers larger than an `f64` representation");

        if mechanical_minimum < enforced_minimum {
            enforced_minimum
        } else {
            mechanical_minimum
        }
    }

    /// Gets the maximum bound for this numeric type, limited by the representable range in JSON Schema.
    fn get_enforced_max_bound() -> f64 {
        let enforced_maximum = NUMERIC_ENFORCED_UPPER_BOUND;
        let mechanical_maximum = Self::Numeric::max_value()
            .to_f64()
            .expect("`Configurable` does not support numbers larger than an `f64` representation");

        if mechanical_maximum > enforced_maximum {
            enforced_maximum
        } else {
            mechanical_maximum
        }
    }
}

macro_rules! impl_configuable_number {
	($($ty:ty),+) => {
		$(
			impl ConfigurableNumber for $ty {
				type Numeric = $ty;
			}
		)+
	};
}

macro_rules! impl_configuable_number_nonzero {
	($($aty:ty => $ity:ty),+) => {
		$(
			impl ConfigurableNumber for $aty {
				type Numeric = $ity;

				fn is_nonzero() -> bool {
					true
				}
			}
		)+
	};

	(with_exclusion, $($aty:ty => $ity:ty),+) => {
		$(
			impl ConfigurableNumber for $aty {
				type Numeric = $ity;

				fn is_nonzero() -> bool {
					true
				}

				fn requires_nonzero_exclusion() -> bool {
					true
				}
			}
		)+
	};
}

impl_configuable_number!(u8, u16, u32, u64, usize, i8, i16, i32, i64, isize, f32, f64);
impl_configuable_number_nonzero!(NonZeroU8 => u8, NonZeroU16 => u16, NonZeroU32 => u32, NonZeroU64 => u64);
impl_configuable_number_nonzero!(with_exclusion, NonZeroI8 => i8, NonZeroI16 => i16, NonZeroI32 => i32, NonZeroI64 => i64);
