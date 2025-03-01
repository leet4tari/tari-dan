//   Copyright 2022 The Tari Project
//   SPDX-License-Identifier: BSD-3-Clause

use std::{fmt::Display, ops::Deref};

use borsh::BorshSerialize;
use log::*;
use serde::{Deserialize, Serialize};
use tari_common_types::types::{FixedHash, FixedHashSizeError};
use tari_dan_common_types::{
    hashing::quorum_certificate_hasher,
    optional::Optional,
    serde_with,
    Epoch,
    NodeHeight,
    ShardGroup,
};

use crate::{
    consensus_models::{
        Block,
        BlockHeader,
        BlockId,
        HighQc,
        LastVoted,
        LeafBlock,
        QuorumDecision,
        ValidatorSignature,
        ValidatorStatsUpdate,
    },
    StateStoreReadTransaction,
    StateStoreWriteTransaction,
    StorageError,
};

const LOG_TARGET: &str = "tari::dan::storage::quorum_certificate";

#[derive(Debug, Clone, Deserialize, Serialize)]
#[cfg_attr(
    feature = "ts",
    derive(ts_rs::TS),
    ts(export, export_to = "../../bindings/src/types/")
)]
pub struct QuorumCertificate {
    #[cfg_attr(feature = "ts", ts(type = "string"))]
    qc_id: QcId,
    #[cfg_attr(feature = "ts", ts(type = "string"))]
    block_id: BlockId,
    #[cfg_attr(feature = "ts", ts(type = "string"))]
    header_hash: FixedHash,
    #[cfg_attr(feature = "ts", ts(type = "string"))]
    parent_id: BlockId,
    block_height: NodeHeight,
    epoch: Epoch,
    shard_group: ShardGroup,
    signatures: Vec<ValidatorSignature>,
    #[serde(with = "serde_with::hex::vec")]
    #[cfg_attr(feature = "ts", ts(type = "Array<string>"))]
    leaf_hashes: Vec<FixedHash>,
    decision: QuorumDecision,
    is_shares_processed: bool,
}

impl QuorumCertificate {
    pub fn new(
        header_hash: FixedHash,
        parent_id: BlockId,
        block_height: NodeHeight,
        epoch: Epoch,
        shard_group: ShardGroup,
        signatures: Vec<ValidatorSignature>,
        mut leaf_hashes: Vec<FixedHash>,
        decision: QuorumDecision,
    ) -> Self {
        leaf_hashes.sort();
        let mut qc = Self {
            qc_id: QcId::zero(),
            block_id: BlockHeader::calculate_block_id(&header_hash, &parent_id),
            header_hash,
            parent_id,
            block_height,
            epoch,
            shard_group,
            signatures,
            leaf_hashes,
            decision,
            is_shares_processed: false,
        };
        qc.qc_id = qc.calculate_id();
        qc
    }

    pub fn genesis(epoch: Epoch, shard_group: ShardGroup) -> Self {
        let mut qc = Self {
            qc_id: QcId::zero(),
            block_id: BlockHeader::calculate_block_id(&FixedHash::zero(), &BlockId::zero()),
            header_hash: FixedHash::zero(),
            parent_id: BlockId::zero(),
            block_height: NodeHeight::zero(),
            epoch,
            shard_group,
            signatures: vec![],
            leaf_hashes: vec![],
            decision: QuorumDecision::Accept,
            is_shares_processed: false,
        };
        qc.qc_id = qc.calculate_id();
        qc
    }

    pub fn calculate_id(&self) -> QcId {
        quorum_certificate_hasher()
            .chain(&self.epoch)
            .chain(&self.shard_group)
            .chain(&self.header_hash)
            .chain(&self.parent_id)
            .chain(&self.block_height)
            .chain(&self.signatures)
            .chain(&self.leaf_hashes)
            .chain(&self.decision)
            .finalize_into_array()
            .into()
    }
}

impl QuorumCertificate {
    pub fn justifies_zero_block(&self) -> bool {
        self.block_id.is_zero()
    }

    pub fn id(&self) -> &QcId {
        &self.qc_id
    }

    pub fn epoch(&self) -> Epoch {
        self.epoch
    }

    pub fn shard_group(&self) -> ShardGroup {
        self.shard_group
    }

    pub fn leaf_hashes(&self) -> &[FixedHash] {
        &self.leaf_hashes
    }

    pub fn signatures(&self) -> &[ValidatorSignature] {
        &self.signatures
    }

    pub fn block_height(&self) -> NodeHeight {
        self.block_height
    }

    pub fn decision(&self) -> QuorumDecision {
        self.decision
    }

    pub fn block_id(&self) -> &BlockId {
        &self.block_id
    }

    pub fn header_hash(&self) -> &FixedHash {
        &self.header_hash
    }

    pub fn parent_id(&self) -> &BlockId {
        &self.parent_id
    }

    pub fn as_high_qc(&self) -> HighQc {
        HighQc {
            block_id: self.block_id,
            block_height: self.block_height,
            epoch: self.epoch,
            qc_id: self.qc_id,
        }
    }

    pub fn as_leaf_block(&self) -> LeafBlock {
        LeafBlock {
            block_id: self.block_id,
            height: self.block_height,
            epoch: self.epoch,
        }
    }

    pub fn as_last_voted(&self) -> LastVoted {
        LastVoted {
            block_id: self.block_id,
            height: self.block_height,
            epoch: self.epoch,
        }
    }
}

impl QuorumCertificate {
    pub fn get<TTx: StateStoreReadTransaction>(tx: &TTx, qc_id: &QcId) -> Result<Self, StorageError> {
        tx.quorum_certificates_get(qc_id)
    }

    pub fn get_all<'a, TTx, I>(tx: &TTx, qc_ids: I) -> Result<Vec<Self>, StorageError>
    where
        TTx: StateStoreReadTransaction,
        I: IntoIterator<Item = &'a QcId>,
        I::IntoIter: ExactSizeIterator,
    {
        tx.quorum_certificates_get_all(qc_ids)
    }

    pub fn get_block<TTx: StateStoreReadTransaction>(&self, tx: &TTx) -> Result<Block, StorageError> {
        Block::get(tx, &self.block_id)
    }

    pub fn get_by_block_id<TTx: StateStoreReadTransaction>(tx: &TTx, block_id: &BlockId) -> Result<Self, StorageError> {
        tx.quorum_certificates_get_by_block_id(block_id)
    }

    pub fn insert<TTx: StateStoreWriteTransaction>(&self, tx: &mut TTx) -> Result<(), StorageError> {
        tx.quorum_certificates_insert(self)
    }

    pub fn exists<TTx: StateStoreReadTransaction>(&self, tx: &TTx) -> Result<bool, StorageError> {
        Ok(tx.quorum_certificates_get(&self.qc_id).optional()?.is_some())
    }

    pub fn check_high_qc<TTx>(&self, tx: &mut TTx) -> Result<(bool, HighQc), StorageError>
    where
        TTx: StateStoreWriteTransaction + Deref,
        TTx::Target: StateStoreReadTransaction,
    {
        let Some(high_qc) = HighQc::get(&**tx, self.epoch).optional()? else {
            // We haven't started the epoch
            return Ok((true, self.as_high_qc()));
        };
        if self.block_height() > high_qc.block_height() {
            return Ok((true, self.as_high_qc()));
        }

        Ok((false, high_qc))
    }

    pub fn update_high_qc<TTx>(&self, tx: &mut TTx) -> Result<HighQc, StorageError>
    where
        TTx: StateStoreWriteTransaction + Deref,
        TTx::Target: StateStoreReadTransaction,
    {
        let (is_new, high_qc) = self.check_high_qc(tx)?;

        info!(
            target: LOG_TARGET,
            "🔥 {}HIGH_QC ({}, previous high QC: {} {})",
            if is_new { "NEW " } else { "" },
            self,
            high_qc.block_id(),
            high_qc.block_height(),
        );

        if !is_new {
            return Ok(high_qc);
        }

        self.save(tx)?;
        // This will fail if the block doesnt exist
        self.as_leaf_block().set(tx)?;
        high_qc.set(tx)?;

        Ok(high_qc)
    }

    pub fn update_participation_shares<TTx: StateStoreWriteTransaction>(
        &self,
        tx: &mut TTx,
    ) -> Result<(), StorageError> {
        if self.is_shares_processed {
            return Ok(());
        }

        tx.validator_epoch_stats_updates(
            self.epoch,
            self.signatures.iter().map(|s| s.public_key()).map(|pk| {
                ValidatorStatsUpdate::new(pk)
                    .increment_participation_share()
                    .decrement_missed_proposal()
            }),
        )?;
        tx.quorum_certificates_set_shares_processed(&self.qc_id)?;

        Ok(())
    }

    pub fn save<TTx>(&self, tx: &mut TTx) -> Result<bool, StorageError>
    where
        TTx: StateStoreWriteTransaction + Deref,
        TTx::Target: StateStoreReadTransaction,
    {
        if self.exists(&**tx)? {
            return Ok(true);
        }
        self.insert(tx)?;
        Ok(false)
    }
}

impl Display for QuorumCertificate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Qc(block: {} {}, qc_id: {}, epoch: {}, {} signatures)",
            self.block_height,
            self.block_id,
            self.qc_id,
            self.epoch,
            self.signatures.len()
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, BorshSerialize)]
#[serde(transparent)]
pub struct QcId(#[serde(with = "serde_with::hex")] FixedHash);

impl QcId {
    /// Represents a zero/null QC. This QC is used to represent the unsigned initial QC.
    pub const fn zero() -> Self {
        Self(FixedHash::zero())
    }

    pub fn new<T: Into<FixedHash>>(hash: T) -> Self {
        Self(hash.into())
    }

    pub const fn hash(&self) -> &FixedHash {
        &self.0
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_slice()
    }

    pub fn is_zero(&self) -> bool {
        self.0.iter().all(|b| *b == 0)
    }
}

impl AsRef<[u8]> for QcId {
    fn as_ref(&self) -> &[u8] {
        self.0.as_slice()
    }
}

impl From<FixedHash> for QcId {
    fn from(value: FixedHash) -> Self {
        Self(value)
    }
}

impl From<[u8; 32]> for QcId {
    fn from(value: [u8; 32]) -> Self {
        Self(value.into())
    }
}

impl TryFrom<Vec<u8>> for QcId {
    type Error = FixedHashSizeError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        FixedHash::try_from(value).map(Self)
    }
}

impl Display for QcId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}
