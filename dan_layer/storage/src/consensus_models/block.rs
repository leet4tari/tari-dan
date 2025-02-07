//   Copyright 2023 The Tari Project
//   SPDX-License-Identifier: BSD-3-Clause

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    fmt::{Debug, Display, Formatter},
    iter,
    ops::{Deref, RangeInclusive},
};

use borsh::BorshSerialize;
use indexmap::IndexMap;
use log::*;
use serde::{Deserialize, Serialize};
use tari_common::configuration::Network;
use tari_common_types::types::{FixedHash, FixedHashSizeError, PublicKey};
use tari_crypto::{ristretto::RistrettoPublicKey, tari_utilities::ByteArray};
use tari_dan_common_types::{
    committee::CommitteeInfo,
    optional::Optional,
    serde_with,
    shard::Shard,
    Epoch,
    ExtraData,
    ExtraFieldKey,
    NodeHeight,
    NumPreshards,
    ShardGroup,
    SubstateAddress,
    VersionedSubstateId,
    VersionedSubstateIdRef,
};
use tari_engine_types::transaction_receipt::TransactionReceiptAddress;
use tari_state_tree::{compute_proof_for_hashes, SparseMerkleProofExt, StateTreeError, TreeHash};
use tari_transaction::TransactionId;
use time::PrimitiveDateTime;
#[cfg(feature = "ts")]
use ts_rs::TS;

use super::{
    BlockDiff,
    BlockPledge,
    EvictNodeAtom,
    ForeignProposal,
    ForeignProposalAtom,
    ForeignSendCounters,
    HighQc,
    MintConfidentialOutputAtom,
    PendingShardStateTreeDiff,
    QuorumCertificate,
    SubstateChange,
    SubstateDestroyedProof,
    SubstateRecord,
    TransactionAtom,
    ValidatorSchnorrSignature,
    ValidatorStatsUpdate,
};
use crate::{
    consensus_models::{
        block_header::BlockHeader,
        Command,
        LastExecuted,
        LastProposed,
        LastVoted,
        LeafBlock,
        LockedBlock,
        SubstateCreatedProof,
        SubstateUpdate,
        TransactionRecord,
        Vote,
    },
    StateStoreReadTransaction,
    StateStoreWriteTransaction,
    StorageError,
};

const LOG_TARGET: &str = "tari::dan::storage::consensus_models::block";

#[derive(Debug, thiserror::Error)]
pub enum BlockError {
    #[error("Error computing command merkle hash: {0}")]
    StateTreeError(#[from] StateTreeError),
    #[error("Merke proof generation command index out of bounds: {index}/{len}")]
    MerkleProofGenerationCommandIndexOutOfBounds { index: usize, len: usize },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(TS), ts(export, export_to = "../../bindings/src/types/"))]
pub struct Block {
    header: BlockHeader,
    justify: QuorumCertificate,
    /// Commands in the block. These are in canonical order to ensure a deterministic block hash.
    commands: BTreeSet<Command>,
    // Metadata - not included in the block hash
    /// Flag that indicates that the block has been justified by a new high QC.
    is_justified: bool,
    /// Flag that indicates that the block has been committed.
    is_committed: bool,
    #[cfg_attr(feature = "ts", ts(type = "number | null"))]
    block_time: Option<u64>,
    /// Timestamp when was this stored.
    #[cfg_attr(feature = "ts", ts(type = "Array<number>| null"))]
    stored_at: Option<PrimitiveDateTime>,
}

impl Block {
    /// Creates a new block from the provided params. Returns an error if the command merkle root fails to construct.
    /// This is infallible for empty commands.
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        network: Network,
        parent: BlockId,
        justify: QuorumCertificate,
        height: NodeHeight,
        epoch: Epoch,
        shard_group: ShardGroup,
        proposed_by: PublicKey,
        commands: BTreeSet<Command>,
        state_merkle_root: FixedHash,
        total_leader_fee: u64,
        sorted_foreign_indexes: BTreeMap<Shard, u64>,
        signature: Option<ValidatorSchnorrSignature>,
        timestamp: u64,
        base_layer_block_height: u64,
        base_layer_block_hash: FixedHash,
        extra_data: ExtraData,
    ) -> Result<Self, BlockError> {
        let header = BlockHeader::create(
            network,
            parent,
            *justify.id(),
            height,
            epoch,
            shard_group,
            proposed_by,
            state_merkle_root,
            &commands,
            total_leader_fee,
            sorted_foreign_indexes,
            signature,
            timestamp,
            base_layer_block_height,
            base_layer_block_hash,
            extra_data,
        )?;
        Ok(Self::new(header, justify, commands))
    }

    pub fn new(header: BlockHeader, justify: QuorumCertificate, commands: BTreeSet<Command>) -> Self {
        Self {
            header,
            justify,
            commands,
            is_justified: false,
            is_committed: false,
            block_time: None,
            stored_at: None,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn load(
        id: BlockId,
        network: Network,
        parent: BlockId,
        justify: QuorumCertificate,
        height: NodeHeight,
        epoch: Epoch,
        shard_group: ShardGroup,
        proposed_by: PublicKey,
        state_merkle_root: FixedHash,
        commands: BTreeSet<Command>,
        command_merkle_root: FixedHash,
        total_leader_fee: u64,
        is_dummy: bool,
        is_justified: bool,
        is_committed: bool,
        sorted_foreign_indexes: BTreeMap<Shard, u64>,
        signature: Option<ValidatorSchnorrSignature>,
        created_at: PrimitiveDateTime,
        block_time: Option<u64>,
        timestamp: u64,
        base_layer_block_height: u64,
        base_layer_block_hash: FixedHash,
        extra_data: ExtraData,
    ) -> Self {
        let header = BlockHeader::load(
            id,
            network,
            parent,
            *justify.id(),
            height,
            epoch,
            shard_group,
            proposed_by,
            state_merkle_root,
            total_leader_fee,
            is_dummy,
            sorted_foreign_indexes,
            signature,
            timestamp,
            base_layer_block_height,
            base_layer_block_hash,
            extra_data,
            command_merkle_root,
        );
        Self {
            header,
            justify,
            commands,
            is_justified,
            is_committed,
            block_time,
            stored_at: Some(created_at),
        }
    }

    pub fn genesis(
        network: Network,
        epoch: Epoch,
        shard_group: ShardGroup,
        state_merkle_root: FixedHash,
        sidechain_id: Option<RistrettoPublicKey>,
    ) -> Self {
        let mut extra_data = ExtraData::new();
        if let Some(sidechain_id) = sidechain_id {
            extra_data.insert(
                ExtraFieldKey::SidechainId,
                sidechain_id
                    .as_bytes()
                    .to_vec()
                    .try_into()
                    .expect("RistrettoPublicKey is 32 bytes"),
            );
        }

        Self::create(
            network,
            BlockId::zero(),
            QuorumCertificate::genesis(epoch, shard_group),
            NodeHeight::zero(),
            epoch,
            shard_group,
            PublicKey::default(),
            Default::default(),
            state_merkle_root,
            0,
            BTreeMap::new(),
            None,
            0,
            0,
            FixedHash::zero(),
            extra_data,
        )
        .expect("Infallible with empty commands")
    }

    /// This is the parent block for all genesis blocks. Its block ID is always zero.
    pub fn zero_block(network: Network, num_preshards: NumPreshards) -> Self {
        Self {
            header: BlockHeader::zero_block(network, num_preshards),
            justify: QuorumCertificate::genesis(Epoch::zero(), ShardGroup::all_shards(num_preshards)),
            commands: Default::default(),
            is_justified: false,
            is_committed: true,
            stored_at: None,
            block_time: None,
        }
    }

    pub fn calculate_id(&self) -> BlockId {
        self.header.calculate_id()
    }

    pub fn header(&self) -> &BlockHeader {
        &self.header
    }

    pub fn is_genesis(&self) -> bool {
        self.header().is_genesis()
    }

    pub fn is_epoch_end(&self) -> bool {
        self.commands.iter().any(|c| c.is_epoch_end())
    }

    pub fn all_transaction_ids(&self) -> impl Iterator<Item = &TransactionId> + '_ {
        self.commands.iter().filter_map(|d| d.transaction().map(|t| t.id()))
    }

    pub fn all_transaction_ids_in_committee<'a>(
        &'a self,
        committee_info: &'a CommitteeInfo,
    ) -> impl Iterator<Item = &'a TransactionId> + Clone + 'a {
        self.commands
            .iter()
            .filter_map(|cmd| cmd.transaction())
            .filter(|t| t.evidence.has_and_not_empty(&committee_info.shard_group()))
            .map(|t| t.id())
    }

    pub fn all_committing_transactions_ids(&self) -> impl Iterator<Item = &TransactionId> + '_ {
        self.commands.iter().filter_map(|d| d.committing()).map(|t| t.id())
    }

    pub fn all_finalising_transactions_ids(&self) -> impl Iterator<Item = &TransactionId> + '_ {
        self.commands.iter().filter_map(|d| d.finalising()).map(|t| t.id())
    }

    pub fn all_aborting_transaction_ids(&self) -> impl Iterator<Item = &TransactionId> + '_ {
        self.commands.iter().filter_map(|d| d.aborting()).map(|t| t.id())
    }

    pub fn all_foreign_proposals(&self) -> impl Iterator<Item = &ForeignProposalAtom> + '_ {
        self.commands.iter().filter_map(|c| c.foreign_proposal())
    }

    pub fn all_node_evictions(&self) -> impl Iterator<Item = &EvictNodeAtom> + '_ {
        self.commands.iter().filter_map(|c| c.evict_node())
    }

    pub fn all_confidential_output_mints(&self) -> impl Iterator<Item = &MintConfidentialOutputAtom> + '_ {
        self.commands.iter().filter_map(|c| c.mint_confidential_output())
    }

    pub fn all_some_prepare(&self) -> impl Iterator<Item = &TransactionAtom> + '_ {
        self.commands.iter().filter_map(|c| c.some_prepare())
    }

    pub fn command_count(&self) -> usize {
        self.commands.len()
    }

    pub fn as_locked_block(&self) -> LockedBlock {
        self.header().as_locked_block()
    }

    pub fn as_last_executed(&self) -> LastExecuted {
        self.header().as_last_executed()
    }

    pub fn as_last_voted(&self) -> LastVoted {
        self.header().as_last_voted()
    }

    pub fn as_leaf_block(&self) -> LeafBlock {
        self.header().as_leaf_block()
    }

    pub fn as_last_proposed(&self) -> LastProposed {
        self.header().as_last_proposed()
    }

    pub fn id(&self) -> &BlockId {
        self.header.id()
    }

    pub fn network(&self) -> Network {
        self.header.network()
    }

    pub fn parent(&self) -> &BlockId {
        self.header.parent()
    }

    pub fn justify(&self) -> &QuorumCertificate {
        &self.justify
    }

    pub fn into_justify(self) -> QuorumCertificate {
        self.justify
    }

    pub fn justifies_parent(&self) -> bool {
        self.justify.block_id() == self.parent()
    }

    pub fn height(&self) -> NodeHeight {
        self.header.height()
    }

    pub fn epoch(&self) -> Epoch {
        self.header.epoch()
    }

    pub fn shard_group(&self) -> ShardGroup {
        self.header.shard_group()
    }

    pub fn total_leader_fee(&self) -> u64 {
        self.header.total_leader_fee()
    }

    pub fn calculate_total_transaction_fee(&self) -> u64 {
        self.commands
            .iter()
            .filter_map(|c| c.committing())
            .map(|atom| atom.transaction_fee)
            .sum()
    }

    pub fn proposed_by(&self) -> &PublicKey {
        self.header.proposed_by()
    }

    pub fn state_merkle_root(&self) -> &FixedHash {
        self.header.state_merkle_root()
    }

    pub fn command_merkle_root(&self) -> &FixedHash {
        self.header.command_merkle_root()
    }

    pub fn commands(&self) -> &BTreeSet<Command> {
        &self.commands
    }

    pub fn into_commands(self) -> BTreeSet<Command> {
        self.commands
    }

    pub fn is_dummy(&self) -> bool {
        self.header.is_dummy()
    }

    pub fn is_justified(&self) -> bool {
        self.is_justified
    }

    pub fn is_committed(&self) -> bool {
        self.is_committed
    }

    pub fn get_foreign_counter(&self, shard: &Shard) -> Option<u64> {
        self.header.get_foreign_counter(shard)
    }

    pub fn foreign_indexes(&self) -> &BTreeMap<Shard, u64> {
        self.header.foreign_indexes()
    }

    pub fn block_time(&self) -> Option<u64> {
        self.block_time
    }

    pub fn timestamp(&self) -> u64 {
        self.header.timestamp()
    }

    pub fn signature(&self) -> Option<&ValidatorSchnorrSignature> {
        self.header.signature()
    }

    pub fn base_layer_block_height(&self) -> u64 {
        self.header.base_layer_block_height()
    }

    pub fn base_layer_block_hash(&self) -> &FixedHash {
        self.header.base_layer_block_hash()
    }

    pub fn extra_data(&self) -> &ExtraData {
        self.header.extra_data()
    }

    pub fn compute_command_inclusion_proof(&self, command_index: usize) -> Result<SparseMerkleProofExt, BlockError> {
        let hashes = self.commands.iter().map(|cmd| TreeHash::from(cmd.hash().into_array()));
        let command = self.commands.iter().nth(command_index).ok_or(
            BlockError::MerkleProofGenerationCommandIndexOutOfBounds {
                index: command_index,
                len: self.commands.len(),
            },
        )?;
        let hash = TreeHash::new(command.hash().into_array());
        let (value, proof) = compute_proof_for_hashes(hashes, hash)?;
        value.expect(
            "Value not found in proof. This is a bug because the hash is taken from commands that generate the tree",
        );
        Ok(proof)
    }
}

impl Block {
    pub fn get<TTx: StateStoreReadTransaction>(tx: &TTx, id: &BlockId) -> Result<Self, StorageError> {
        tx.blocks_get(id)
    }

    pub fn get_ids_by_epoch_and_height<TTx: StateStoreReadTransaction>(
        tx: &TTx,
        epoch: Epoch,
        height: NodeHeight,
    ) -> Result<Vec<BlockId>, StorageError> {
        tx.blocks_get_all_ids_by_height(epoch, height)
    }

    pub fn get_genesis_for_epoch<TTx: StateStoreReadTransaction>(tx: &TTx, epoch: Epoch) -> Result<Self, StorageError> {
        let ids = Self::get_ids_by_epoch_and_height(tx, epoch, NodeHeight::zero())?;
        if ids.is_empty() {
            return Err(StorageError::DataInconsistency {
                details: format!("No genesis block found for epoch {}", epoch),
            });
        }
        if ids.len() > 1 {
            return Err(StorageError::DataInconsistency {
                details: format!("Multiple genesis blocks found for epoch {}", epoch),
            });
        }

        Self::get(tx, &ids[0])
    }

    /// Returns all blocks from and excluding the start block (lower height) to the end block (inclusive)
    pub fn get_all_blocks_between<TTx: StateStoreReadTransaction>(
        tx: &TTx,
        epoch: Epoch,
        shard_group: ShardGroup,
        start_block_height: NodeHeight,
        end_block_height: NodeHeight,
        include_dummy_blocks: bool,
        limit: u64,
    ) -> Result<Vec<Self>, StorageError> {
        tx.blocks_get_all_between(
            epoch,
            shard_group,
            start_block_height,
            end_block_height,
            include_dummy_blocks,
            limit,
        )
    }

    pub fn get_last_n_in_epoch<TTx: StateStoreReadTransaction>(
        tx: &TTx,
        n: usize,
        epoch: Epoch,
    ) -> Result<Vec<Self>, StorageError> {
        tx.blocks_get_last_n_in_epoch(n, epoch)
    }

    pub fn exists<TTx: StateStoreReadTransaction>(&self, tx: &TTx) -> Result<bool, StorageError> {
        Self::record_exists(tx, self.id())
    }

    pub fn parent_exists<TTx: StateStoreReadTransaction>(&self, tx: &TTx) -> Result<bool, StorageError> {
        Self::record_exists(tx, self.parent())
    }

    pub fn has_been_justified<TTx: StateStoreReadTransaction>(
        tx: &TTx,
        block_id: &BlockId,
    ) -> Result<bool, StorageError> {
        // TODO: consider optimising
        let b = Self::get(tx, block_id)?;
        Ok(b.is_justified)
    }

    pub fn record_exists<TTx: StateStoreReadTransaction>(tx: &TTx, block_id: &BlockId) -> Result<bool, StorageError> {
        tx.blocks_exists(block_id)
    }

    pub fn insert<TTx: StateStoreWriteTransaction>(&self, tx: &mut TTx) -> Result<(), StorageError> {
        tx.blocks_insert(self)
    }

    // pub fn get_paginated<TTx: StateStoreReadTransaction>(
    //     tx: &mut TTx,
    //     limit: u64,
    //     offset: u64,
    //     ordering: Option<Ordering>,
    // ) -> Result<Vec<Self>, StorageError> {
    //     tx.blocks_get_paginated(limit, offset, ordering)
    // }

    pub fn get_count<TTx: StateStoreReadTransaction>(tx: &TTx) -> Result<i64, StorageError> {
        tx.blocks_get_count()
    }

    /// Inserts the block if it doesnt exist. Returns true if the block was saved and did not exist previously,
    /// otherwise false.
    pub fn save<TTx>(&self, tx: &mut TTx) -> Result<bool, StorageError>
    where
        TTx: StateStoreWriteTransaction + Deref,
        TTx::Target: StateStoreReadTransaction,
    {
        let exists = self.exists(&**tx)?;
        if exists {
            return Ok(false);
        }
        self.insert(tx)?;
        Ok(true)
    }

    pub fn remove_parallel_chains<TTx>(&self, tx: &mut TTx) -> Result<(), StorageError>
    where
        TTx: StateStoreWriteTransaction + Deref,
        TTx::Target: StateStoreReadTransaction,
    {
        let other_blocks = Self::get_ids_by_epoch_and_height(&**tx, self.epoch(), self.height())?;
        for block_id in other_blocks {
            if block_id == *self.id() {
                continue;
            }
            info!(
                target: LOG_TARGET,
                "❗️🔗 Removing parallel chain block {} from epoch {} height {}",
                block_id,
                self.epoch(),
                self.height()
            );
            delete_block_and_children(tx, &block_id)?;
        }
        Ok(())
    }

    pub fn remove_diff<TTx: StateStoreWriteTransaction>(&self, tx: &mut TTx) -> Result<(), StorageError> {
        tx.block_diffs_remove(self.id())
    }

    pub fn remove_pending_tree_diff<TTx: StateStoreWriteTransaction>(&self, tx: &mut TTx) -> Result<(), StorageError> {
        tx.pending_state_tree_diffs_remove_by_block(self.id())
    }

    pub fn remove_pending_tree_diff_and_return<TTx: StateStoreWriteTransaction>(
        &self,
        tx: &mut TTx,
    ) -> Result<IndexMap<Shard, Vec<PendingShardStateTreeDiff>>, StorageError> {
        tx.pending_state_tree_diffs_remove_and_return_by_block(self.id())
    }

    pub fn delete<TTx: StateStoreWriteTransaction>(&self, tx: &mut TTx) -> Result<(), StorageError> {
        Self::delete_record(tx, self.id())
    }

    pub fn delete_record<TTx: StateStoreWriteTransaction>(
        tx: &mut TTx,
        block_id: &BlockId,
    ) -> Result<(), StorageError> {
        tx.blocks_delete(block_id)
    }

    pub fn commit_diff<TTx: StateStoreWriteTransaction>(
        &self,
        tx: &mut TTx,
        block_diff: BlockDiff,
    ) -> Result<(), StorageError> {
        if block_diff.block_id() != self.id() {
            return Err(StorageError::QueryError {
                reason: format!(
                    "[commit_diff] Block ID mismatch. Expected: {}, got: {}",
                    self.id(),
                    block_diff.block_id()
                ),
            });
        }

        if self.is_dummy() && !block_diff.is_empty() {
            return Err(StorageError::QueryError {
                reason: format!(
                    "[commit_diff] Dummy block cannot have any substate changes. Block ID: {}",
                    self.id()
                ),
            });
        }

        if !self.is_dummy() {
            block_diff.remove(tx)?;
        }

        for change in block_diff.into_changes() {
            match change {
                SubstateChange::Up {
                    id,
                    shard,
                    transaction_id,
                    substate,
                } => {
                    let version = id.version();
                    SubstateRecord::new(
                        id.into_substate_id(),
                        version,
                        substate.into_substate_value(),
                        shard,
                        self.epoch(),
                        self.height(),
                        *self.id(),
                        transaction_id,
                        *self.justify().id(),
                    )
                    .create(tx)?;
                },
                SubstateChange::Down {
                    id,
                    transaction_id,
                    shard,
                } => {
                    SubstateRecord::destroy(
                        tx,
                        id,
                        shard,
                        self.epoch(),
                        self.height(),
                        self.justify().id(),
                        &transaction_id,
                    )?;
                },
            }
        }

        tx.blocks_set_flags(self.id(), Some(true), None)
    }

    pub fn get_diff<TTx: StateStoreReadTransaction>(&self, tx: &TTx) -> Result<BlockDiff, StorageError> {
        tx.block_diffs_get(self.id())
    }

    pub fn set_as_justified<TTx: StateStoreWriteTransaction>(&mut self, tx: &mut TTx) -> Result<(), StorageError> {
        self.is_justified = true;
        tx.blocks_set_flags(self.id(), None, Some(true))
    }

    pub fn find_involved_shards<TTx: StateStoreReadTransaction>(
        &self,
        tx: &TTx,
    ) -> Result<HashSet<SubstateAddress>, StorageError> {
        tx.transactions_fetch_involved_shards(self.all_transaction_ids().copied().collect())
    }

    pub fn max_height<TTx: StateStoreReadTransaction>(tx: &TTx) -> Result<NodeHeight, StorageError> {
        tx.blocks_max_height()
    }

    pub fn extends<TTx: StateStoreReadTransaction>(&self, tx: &TTx, ancestor: &BlockId) -> Result<bool, StorageError> {
        if self.id() == ancestor {
            return Ok(false);
        }
        if self.parent() == ancestor {
            return Ok(true);
        }
        // First check the parent here, if it does not exist, then this block cannot extend anything.
        if !Block::record_exists(tx, self.parent())? {
            return Ok(false);
        }

        tx.blocks_is_ancestor(self.parent(), ancestor)
    }

    pub fn get_parent<TTx: StateStoreReadTransaction>(&self, tx: &TTx) -> Result<Block, StorageError> {
        if self.id().is_zero() && self.parent().is_zero() {
            return Err(StorageError::NotFound {
                item: "Block parent",
                key: self.parent().to_string(),
            });
        }

        Block::get(tx, self.parent())
    }

    pub fn get_parent_chain<TTx: StateStoreReadTransaction>(
        &self,
        tx: &TTx,
        limit: usize,
    ) -> Result<Vec<Block>, StorageError> {
        tx.blocks_get_parent_chain(self.id(), limit)
    }

    pub fn get_votes<TTx: StateStoreReadTransaction>(&self, tx: &TTx) -> Result<Vec<Vote>, StorageError> {
        Vote::get_for_block(tx, self.id())
    }

    pub fn get_child_block_ids<TTx: StateStoreReadTransaction>(&self, tx: &TTx) -> Result<Vec<BlockId>, StorageError> {
        tx.blocks_get_ids_by_parent(self.id())
    }

    pub fn get_any_with_epoch_range_for_validator<TTx: StateStoreReadTransaction>(
        tx: &TTx,
        range: RangeInclusive<Epoch>,
        validator_public_key: Option<&PublicKey>,
    ) -> Result<Vec<Self>, StorageError> {
        tx.blocks_get_any_with_epoch_range(range, validator_public_key)
    }

    pub fn get_transactions<TTx: StateStoreReadTransaction>(
        &self,
        tx: &TTx,
    ) -> Result<Vec<TransactionRecord>, StorageError> {
        let tx_ids = self.commands().iter().filter_map(|t| t.transaction().map(|t| t.id()));
        let (found, missing) = TransactionRecord::get_any(tx, tx_ids)?;
        if !missing.is_empty() {
            return Err(StorageError::NotFound {
                item: "Transaction",
                key: missing
                    .into_iter()
                    .map(|id| id.to_string())
                    .collect::<Vec<_>>()
                    .join(", "),
            });
        }

        Ok(found)
    }

    /// Returns the transactions that are/will be committed by this block when this block.
    pub fn get_committing_transactions<TTx: StateStoreReadTransaction>(
        &self,
        tx: &TTx,
    ) -> Result<Vec<TransactionRecord>, StorageError> {
        let tx_ids = self.commands().iter().filter_map(|t| t.committing()).map(|t| t.id());
        let (found, missing) = TransactionRecord::get_any(tx, tx_ids)?;
        if !missing.is_empty() {
            return Err(StorageError::NotFound {
                item: "Transaction (get_committed_transactions)",
                key: missing
                    .into_iter()
                    .map(|id| id.to_string())
                    .collect::<Vec<_>>()
                    .join(", "),
            });
        }

        Ok(found)
    }

    pub fn get_substate_updates<TTx: StateStoreReadTransaction>(
        &self,
        tx: &TTx,
    ) -> Result<Vec<SubstateUpdate>, StorageError> {
        let committed = self
            .commands()
            .iter()
            .filter_map(|c| c.committing())
            .filter(|t| t.decision.is_commit())
            .collect::<Vec<_>>();

        let mut updates = Vec::with_capacity(committed.len());
        for transaction in committed {
            let substates = tx.substates_get_all_for_transaction(&transaction.id)?;
            for substate in substates {
                if let Some(destroyed) = substate.destroyed() {
                    // This substate is destroyed. One of the following are possible:
                    // 1. The substate was destroyed by this transaction and created in an earlier transaction
                    // 2. The substate was created by this transaction and destroyed in a later transaction
                    // It isn't possible for a substate to be created and destroyed by the same transaction
                    // because the engine can never emit such a substate diff.
                    // TODO: This is currently not used - if we need this in future, we can include the state hash en
                    //       lieu of the actual state which does not exist
                    // if substate.created_by_transaction == transaction.id
                    // {     updates.push(SubstateUpdate::Create(SubstateCreatedProof {
                    //         // created_qc: substate.get_created_quorum_certificate(tx)?,
                    //         substate: substate.try_into()?,
                    //     }));
                    // } else {
                    updates.push(SubstateUpdate::Destroy(SubstateDestroyedProof {
                        substate_id: substate.substate_id.clone(),
                        version: substate.version,
                        // justify: QuorumCertificate::get(tx, &destroyed.justify)?,
                        destroyed_by_transaction: destroyed.by_transaction,
                    }));
                    // }
                } else {
                    updates.push(SubstateUpdate::Create(SubstateCreatedProof {
                        // created_qc: substate.get_created_quorum_certificate(tx)?,
                        substate: substate.into(),
                    }));
                };
            }
        }

        Ok(updates)
    }

    pub fn get_transaction_receipts<TTx: StateStoreReadTransaction>(
        &self,
        tx: &TTx,
    ) -> Result<Vec<SubstateCreatedProof>, StorageError> {
        let committed = self
            .commands()
            .iter()
            .filter_map(|c| c.committing())
            .filter(|t| t.decision.is_commit());

        let receipt_ids = committed
            .map(|atom| TransactionReceiptAddress::from_array(atom.id.into_array()))
            .map(VersionedSubstateId::for_tx_receipt)
            .collect::<Vec<_>>();

        let receipts = SubstateRecord::get_all(tx, receipt_ids.iter().map(Into::into))?;
        let receipts = receipts
            .into_iter()
            .map(|receipt| {
                Ok::<_, StorageError>(SubstateCreatedProof {
                    substate: receipt.into(),
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(receipts)
    }

    /// Returns the QC that justifies this block
    pub fn get_justify_qc<TTx: StateStoreReadTransaction>(&self, tx: &TTx) -> Result<QuorumCertificate, StorageError> {
        tx.quorum_certificates_get_by_block_id(self.id())
    }

    pub fn update_nodes<TTx, TFnOnLock, TFnOnCommit, E>(
        &self,
        tx: &mut TTx,
        mut on_lock_block: TFnOnLock,
        mut on_commit: TFnOnCommit,
    ) -> Result<HighQc, E>
    where
        TTx: StateStoreWriteTransaction + Deref,
        TTx::Target: StateStoreReadTransaction,
        TFnOnLock: FnMut(&mut TTx, &LockedBlock, &Block, &QuorumCertificate) -> Result<(), E>,
        TFnOnCommit: FnMut(&mut TTx, &LastExecuted, Block) -> Result<(), E>,
        E: From<StorageError>,
    {
        let high_qc = self.justify().update_high_qc(tx)?;

        // b'' <- b*.justify.node i.e. the (possibly new) justified block
        let justified_node = self.justify().get_block(&**tx)?;

        // b' <- b''.justify.node
        let new_locked = justified_node.justify().get_block(&**tx)?;

        if new_locked.is_genesis() {
            return Ok(high_qc);
        }

        let current_locked = LockedBlock::get(&**tx, self.epoch())?;
        if new_locked.height() > current_locked.height {
            on_locked_block_recurse(
                tx,
                &current_locked,
                &new_locked,
                justified_node.justify(),
                &mut on_lock_block,
            )?;
            new_locked.as_locked_block().set(tx)?;
        }

        // b <- b'.justify.node
        let commit_node = new_locked.justify().block_id();
        if justified_node.parent() == new_locked.id() && new_locked.parent() == commit_node {
            debug!(
                target: LOG_TARGET,
                "✅ Block {} {} forms a 3-chain b'' = {}, b' = {}, b = {}",
                self.height(),
                self.id(),
                justified_node.id(),
                new_locked.id(),
                commit_node,
            );

            // Commit prepare_node (b)
            if commit_node.is_zero() {
                return Ok(high_qc);
            }
            let prepare_node = Block::get(&**tx, commit_node)?;
            let last_executed = LastExecuted::get(&**tx)?;
            let last_exec = prepare_node.as_last_executed();
            on_commit_block_recurse(tx, &last_executed, prepare_node, &mut on_commit)?;
            last_exec.set(tx)?;
        } else {
            debug!(
                target: LOG_TARGET,
                "Block {} {} DOES NOT form a 3-chain b'' = {}, b' = {}, b = {}, b* = {}",
                self.height(),
                self.id(),
                justified_node.id(),
                new_locked.id(),
                commit_node,
                self.id()
            );
        }

        Ok(high_qc)
    }

    /// safeNode predicate (https://arxiv.org/pdf/1803.05069v6.pdf)
    ///
    /// The safeNode predicate is a core ingredient of the protocol. It examines a proposal message
    /// m carrying a QC justification m.justify, and determines whether m.node is safe to accept. The safety rule to
    /// accept a proposal is the branch of m.node extends from the currently locked node lockedQC.node. On the other
    /// hand, the liveness rule is the replica will accept m if m.justify has a higher view than the current
    /// lockedQC. The predicate is true as long as either one of two rules holds.
    pub fn is_safe<TTx: StateStoreReadTransaction>(&self, tx: &TTx) -> Result<bool, StorageError> {
        let locked = LockedBlock::get(tx, self.epoch())?;

        // Liveness rules
        if self.justify().block_height() > locked.height() {
            return Ok(true);
        }

        // Safety rule
        if self.extends(tx, locked.block_id())? {
            return Ok(true);
        }

        info!(
            target: LOG_TARGET,
            "❌ Block {} does satisfy the liveness or safety rules of the safeNode predicate. Locked block {}",
            self,
            locked,
        );
        Ok(false)
    }

    pub fn save_foreign_send_counters<TTx>(&self, tx: &mut TTx) -> Result<(), StorageError>
    where
        TTx: StateStoreWriteTransaction + Deref,
        TTx::Target: StateStoreReadTransaction,
    {
        let mut counters = ForeignSendCounters::get_or_default(&**tx, self.justify().block_id())?;
        // Add counters for this block and carry over the counters from the justify block, if any
        for shard in self.foreign_indexes().keys() {
            counters.increment_counter(*shard);
        }
        if !counters.is_empty() {
            counters.set(tx, self.id())?;
        }
        Ok(())
    }

    pub fn get_block_pledge<TTx: StateStoreReadTransaction>(
        &self,
        tx: &TTx,
        for_shard_group: ShardGroup,
    ) -> Result<BlockPledge, StorageError> {
        if self.is_committed() {
            // TODO: this is only a problem if we do not preserve DOWN substates "for some reasonable time".
            warn!(
                target: LOG_TARGET,
                "get_block_pledge: Block {} is already committed. Some substates may be DOWN and therefore these pledges will not be provided", self.as_leaf_block()
            );
        }

        let log_bool = |context: &str, atom: &TransactionAtom, val: bool| {
            if !val {
                debug!(
                    target: LOG_TARGET,
                    "get_block_pledge: Excluding {atom} because {context}"
                );
            }
            val
        };

        let applicable_transactions = self
            .commands()
            .iter()
            .filter_map(|c| {
                c.local_prepare()
                    // No need to broadcast LocalPrepare if the committee is output only (TODO: this no longer applies as output only skips LocalPrepare, so do we need this?)
                    .filter(|atom| log_bool("LocalPrepare, local output-only", atom, !atom.evidence.is_committee_output_only(self.shard_group())))
                    .or_else(|| {
                        // Avoid pledging twice - for input-involved SGs we have already sent pledges in LocalPrepare phase. For output-only, we need to pledge in the LocalAccept phase
                        c.local_accept()
                            .filter(|atom| log_bool("LocalAccept, foreign input-involved", atom, atom.evidence.is_committee_output_only(for_shard_group)))
                    })
            })
            .filter(|atom| log_bool("Is ABORT", atom, atom.decision.is_commit()))
            .filter(|atom| log_bool("Foreign SG not involved", atom, atom.evidence.has(&for_shard_group)));

        let mut num_applicable = 0;
        let mut pledges = BlockPledge::new();
        for atom in applicable_transactions {
            num_applicable += 1;
            let evidence = atom
                .evidence
                .get(&self.shard_group())
                .ok_or_else(|| StorageError::DataInconsistency {
                    details: format!("Local evidence for atom {} is missing in block {}", atom.id, self),
                })?;

            // TODO(perf): O(n) queries
            let substates = SubstateRecord::get_all(
                tx,
                evidence
                    .all_pledged_inputs_iter()
                    .map(|(substate_id, ev)| VersionedSubstateIdRef::new(substate_id, ev.version)),
            )?;

            debug!(
                target: LOG_TARGET,
                "get_block_pledge: {} locked for atom {} in block {}",
                substates.len(), atom.id, self
            );

            let self_as_leaf = self.as_leaf_block();
            for substate in substates {
                let version = substate.version();
                let id = substate.substate_id;
                let value = substate.substate_value.ok_or_else(|| StorageError::DataInconsistency {
                    details: format!(
                        "Pledge {}:{} has no substate value however a value is required",
                        id, version
                    ),
                })?;

                debug!(
                    target: LOG_TARGET,
                    "get_block_pledge: Adding pledge {}:{} for atom {} in block {}",
                    id, version, atom.id, self_as_leaf
                );
                pledges.add_substate_pledge(id, version, value);
            }
        }

        debug!(
            target: LOG_TARGET,
            "get_block_pledge: {num_applicable} pledge(s) for shard group {for_shard_group}"
        );

        Ok(pledges)
    }

    pub fn get_foreign_proposals<TTx: StateStoreReadTransaction>(
        &self,
        tx: &TTx,
    ) -> Result<Vec<ForeignProposal>, StorageError> {
        ForeignProposal::get_any(tx, self.all_foreign_proposals().map(|p| &p.block_id))
    }

    pub fn increment_leader_failure_count<TTx: StateStoreWriteTransaction>(
        &self,
        tx: &mut TTx,
        max_missed_proposal_cap: u64,
    ) -> Result<(), StorageError> {
        tx.validator_epoch_stats_updates(
            self.epoch(),
            iter::once(
                ValidatorStatsUpdate::new(self.proposed_by())
                    .add_missed_proposal()
                    .set_max_missed_proposals_cap(max_missed_proposal_cap),
            ),
        )
    }

    pub fn clear_leader_failure_count<TTx: StateStoreWriteTransaction>(
        &self,
        tx: &mut TTx,
    ) -> Result<(), StorageError> {
        tx.validator_epoch_stats_updates(
            self.epoch(),
            iter::once(ValidatorStatsUpdate::new(self.proposed_by()).reset_missed_proposals()),
        )
    }
}

impl Display for Block {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.is_dummy() {
            write!(f, "Dummy")?;
        }
        write!(
            f,
            "[{}, justify: {} ({}), {}, {}, {} cmd(s), {}->{}]",
            self.height(),
            self.justify().block_height(),
            if self.justifies_parent() { "🟢" } else { "🟡" },
            self.epoch(),
            self.shard_group(),
            self.commands().len(),
            self.id(),
            self.parent()
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, BorshSerialize)]
#[serde(transparent)]
pub struct BlockId(#[serde(with = "serde_with::hex")] FixedHash);

impl BlockId {
    pub const fn genesis() -> Self {
        Self(FixedHash::zero())
    }

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

    pub const fn byte_size() -> usize {
        FixedHash::byte_size()
    }
}

impl AsRef<[u8]> for BlockId {
    fn as_ref(&self) -> &[u8] {
        self.0.as_slice()
    }
}

impl From<FixedHash> for BlockId {
    fn from(value: FixedHash) -> Self {
        Self(value)
    }
}

impl From<[u8; 32]> for BlockId {
    fn from(value: [u8; 32]) -> Self {
        Self(value.into())
    }
}

impl TryFrom<Vec<u8>> for BlockId {
    type Error = FixedHashSizeError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Self::try_from(value.as_slice())
    }
}

impl TryFrom<&[u8]> for BlockId {
    type Error = FixedHashSizeError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        FixedHash::try_from(value).map(Self)
    }
}

impl Display for BlockId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

fn on_locked_block_recurse<TTx, F, E>(
    tx: &mut TTx,
    locked: &LockedBlock,
    block: &Block,
    justify_qc: &QuorumCertificate,
    callback: &mut F,
) -> Result<(), E>
where
    TTx: StateStoreWriteTransaction + Deref,
    TTx::Target: StateStoreReadTransaction,
    E: From<StorageError>,
    F: FnMut(&mut TTx, &LockedBlock, &Block, &QuorumCertificate) -> Result<(), E>,
{
    if locked.height < block.height() {
        let parent = block.get_parent(&**tx)?;
        on_locked_block_recurse(tx, locked, &parent, block.justify(), callback)?;
        callback(tx, locked, block, justify_qc)?;
    }
    Ok(())
}

fn on_commit_block_recurse<TTx, F, E>(
    tx: &mut TTx,
    last_executed: &LastExecuted,
    block: Block,
    callback: &mut F,
) -> Result<(), E>
where
    TTx: StateStoreWriteTransaction + Deref,
    TTx::Target: StateStoreReadTransaction,
    E: From<StorageError>,
    F: FnMut(&mut TTx, &LastExecuted, Block) -> Result<(), E>,
{
    if last_executed.height < block.height() {
        let parent = block.get_parent(&**tx)?;
        // Recurse to "catch up" any parent blocks we may not have executed
        on_commit_block_recurse(tx, last_executed, parent, callback)?;
        callback(tx, last_executed, block)?;
    }
    Ok(())
}

/// Deletes everything related to a block as well as any child blocks
fn delete_block_and_children<TTx>(tx: &mut TTx, block_id: &BlockId) -> Result<(), StorageError>
where
    TTx: StateStoreWriteTransaction + Deref,
    TTx::Target: StateStoreReadTransaction,
{
    let children = tx.blocks_get_ids_by_parent(block_id)?;
    for child in children {
        delete_block_and_children(tx, &child)?;
    }
    tx.block_diffs_remove(block_id).optional()?;
    tx.pending_state_tree_diffs_remove_by_block(block_id).optional()?;
    tx.substate_locks_remove_any_by_block_id(block_id)?;
    tx.transaction_pool_state_updates_remove_any_by_block_id(block_id)?;
    tx.transaction_executions_remove_any_by_block_id(block_id)?;
    tx.foreign_proposals_clear_proposed_in(block_id)?;
    tx.burnt_utxos_clear_proposed_block(block_id)?;
    tx.lock_conflicts_remove_by_block_id(block_id)?;

    Block::delete_record(tx, block_id)?;

    Ok(())
}
