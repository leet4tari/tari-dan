//   Copyright 2023 The Tari Project
//   SPDX-License-Identifier: BSD-3-Clause

use std::{fmt::Display, ops::RangeInclusive};

use borsh::BorshSerialize;
use serde::{Deserialize, Serialize};

use crate::{uint::U256, NumPreshards, SubstateAddress};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, BorshSerialize)]
#[cfg_attr(
    feature = "ts",
    derive(ts_rs::TS),
    ts(export, export_to = "../../bindings/src/types/")
)]
#[serde(transparent)]
pub struct Shard(#[cfg_attr(feature = "ts", ts(type = "number"))] u32);

impl Shard {
    /// Returns the first available shard in the whole range.
    /// Note: it starts from `1` as `0` is reserved for global substates.
    pub const fn first() -> Shard {
        Shard(1)
    }

    /// Returns global shard number.
    /// It is a reserved shard for global substates.
    pub const fn global() -> Shard {
        Shard(0)
    }

    pub const fn is_global(&self) -> bool {
        self.0 == 0
    }

    pub const fn is_first(&self) -> bool {
        self.0 == 1
    }

    pub const fn as_u32(self) -> u32 {
        self.0
    }

    pub fn to_substate_address_range(self, num_shards: NumPreshards) -> RangeInclusive<SubstateAddress> {
        if num_shards.is_one() || self.is_global() {
            return RangeInclusive::new(SubstateAddress::zero(), SubstateAddress::max());
        }

        let num_shards = num_shards.as_u32();

        let shard_u256 = U256::from(self.0) - 1;
        let shard_index = self.0 - 1;

        // Power of two integer division using bit shifts
        let shard_size = U256::MAX >> num_shards.trailing_zeros();
        if shard_index == 0 {
            return RangeInclusive::new(
                SubstateAddress::zero(),
                SubstateAddress::from_u256_zero_version(shard_size - 1),
            );
        }

        // Add one to each start to account for remainder
        let start = shard_u256 * shard_size;

        if shard_index == num_shards - 1 {
            return RangeInclusive::new(
                SubstateAddress::from_u256_zero_version(start + shard_u256 - 1),
                SubstateAddress::max(),
            );
        }

        let end = start + shard_size;
        RangeInclusive::new(
            SubstateAddress::from_u256_zero_version(start + shard_u256 - 1),
            SubstateAddress::from_u256_zero_version(end + shard_u256 - 1),
        )
    }
}

impl From<u32> for Shard {
    fn from(v: u32) -> Self {
        Self(v)
    }
}

impl PartialEq<u32> for Shard {
    fn eq(&self, other: &u32) -> bool {
        self.0 == *other
    }
}
impl PartialEq<Shard> for u32 {
    fn eq(&self, other: &Shard) -> bool {
        *self == other.as_u32()
    }
}

impl Display for Shard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Shard({})", self.as_u32())
    }
}

#[cfg(test)]
mod test {
    use std::iter;

    use indexmap::IndexMap;

    use super::*;

    #[test]
    fn committee_is_properly_computed() {
        // TODO: clean this up a bit, I wrote this very hastily
        let power_of_twos = iter::successors(Some(1u32), |x| Some(x * 2))
            .take(8)
            .map(|v| NumPreshards::try_from(v).unwrap());
        let mut split_map = IndexMap::<_, Vec<_>>::new();
        for num_of_shards in power_of_twos {
            let mut last_end = U256::ZERO;
            for shard_index in 0..num_of_shards.as_u32() {
                let shard = Shard::from(shard_index + 1);
                let range = shard.to_substate_address_range(num_of_shards);
                if shard_index == 0 {
                    assert_eq!(range.start().to_u256(), U256::ZERO, "First shard should start at 0");
                } else {
                    assert_eq!(
                        range.start().to_u256(),
                        last_end + 1,
                        "Shard should start where the previous one ended+1"
                    );
                }
                last_end = range.end().to_u256();
                split_map.entry(num_of_shards.as_u32()).or_default().push(range);
            }
            assert_eq!(last_end, U256::MAX, "Last shard should end at U256::MAX");
        }

        let mut i = 0usize;
        for (num_of_shards, splits) in &split_map {
            // Each split in the next num_of_shards should match the previous shard splits
            let Some(next_splits) = split_map.get(&(num_of_shards << 1)) else {
                break;
            };

            i += 1;

            for (split, next_split) in splits.iter().zip(
                next_splits
                    .iter()
                    .enumerate()
                    // Every 2nd boundary matches
                    .filter(|(i, _)| i % 2 == 1)
                    .map(|(_, s)| s),
            ) {
                assert_eq!(
                    split.end().to_u256(),
                    next_split.end().to_u256(),
                    "Bucket should end where the next one starts-1"
                );
            }

            if splits.len() >= 2 {
                let mut size = None;
                for split in splits.iter().skip(1).take(splits.len() - 2) {
                    if let Some(size) = size {
                        assert_eq!(
                            split.end().to_u256() - split.start().to_u256(),
                            size,
                            "Shard size should be consistent"
                        );
                    }
                    size = Some(split.end().to_u256() - split.start().to_u256());
                }
            }
        }

        // Check that we didnt break early
        assert_eq!(i, 7);
    }
}
