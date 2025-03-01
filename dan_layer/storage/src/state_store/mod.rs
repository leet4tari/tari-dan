//   Copyright 2023 The Tari Project
//   SPDX-License-Identifier: BSD-3-Clause

use std::{
    borrow::Borrow,
    collections::{HashMap, HashSet},
    ops::{Deref, RangeInclusive},
};

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use tari_common_types::types::{FixedHash, PublicKey};
use tari_dan_common_types::{
    shard::Shard,
    Epoch,
    NodeAddressable,
    NodeHeight,
    ShardGroup,
    SubstateAddress,
    ToSubstateAddress,
    VersionedSubstateId,
    VersionedSubstateIdRef,
};
use tari_engine_types::substate::SubstateId;
use tari_state_tree::{Node, NodeKey, StaleTreeNode, Version};
use tari_template_lib::models::UnclaimedConfidentialOutputAddress;
use tari_transaction::TransactionId;
#[cfg(feature = "ts")]
use ts_rs::TS;

use crate::{
    consensus_models::{
        Block,
        BlockDiff,
        BlockId,
        BlockTransactionExecution,
        BurntUtxo,
        Decision,
        EpochCheckpoint,
        Evidence,
        ForeignParkedProposal,
        ForeignProposal,
        ForeignProposalAtom,
        ForeignProposalStatus,
        ForeignReceiveCounters,
        ForeignSendCounters,
        HighQc,
        LastExecuted,
        LastProposed,
        LastSentVote,
        LastVoted,
        LeafBlock,
        LockConflict,
        LockedBlock,
        LockedSubstateValue,
        NoVoteReason,
        PendingShardStateTreeDiff,
        QcId,
        QuorumCertificate,
        StateTransition,
        StateTransitionId,
        SubstateChange,
        SubstateLock,
        SubstatePledges,
        SubstateRecord,
        TransactionPoolConfirmedStage,
        TransactionPoolRecord,
        TransactionPoolStage,
        TransactionPoolStatusUpdate,
        TransactionRecord,
        ValidatorConsensusStats,
        ValidatorStatsUpdate,
        VersionedStateHashTreeDiff,
        Vote,
    },
    StorageError,
};

const LOG_TARGET: &str = "tari::dan::storage";

pub trait StateStore {
    type Addr: NodeAddressable;
    type ReadTransaction<'a>: StateStoreReadTransaction<Addr = Self::Addr>
    where Self: 'a;
    type WriteTransaction<'a>: StateStoreWriteTransaction<Addr = Self::Addr> + Deref<Target = Self::ReadTransaction<'a>>
    where Self: 'a;

    fn create_read_tx(&self) -> Result<Self::ReadTransaction<'_>, StorageError>;
    fn create_write_tx(&self) -> Result<Self::WriteTransaction<'_>, StorageError>;

    fn with_write_tx<F: FnOnce(&mut Self::WriteTransaction<'_>) -> Result<R, E>, R, E>(&self, f: F) -> Result<R, E>
    where E: From<StorageError> {
        let mut tx = self.create_write_tx()?;
        match f(&mut tx) {
            Ok(r) => {
                tx.commit()?;
                Ok(r)
            },
            Err(e) => {
                if let Err(err) = tx.rollback() {
                    log::error!(target: LOG_TARGET, "Failed to rollback transaction: {}", err);
                }
                Err(e)
            },
        }
    }

    fn with_read_tx<F: FnOnce(&Self::ReadTransaction<'_>) -> Result<R, E>, R, E>(&self, f: F) -> Result<R, E>
    where E: From<StorageError> {
        let tx = self.create_read_tx()?;
        let ret = f(&tx)?;
        Ok(ret)
    }
}

pub trait StateStoreReadTransaction: Sized {
    type Addr: NodeAddressable;
    fn last_sent_vote_get(&self) -> Result<LastSentVote, StorageError>;
    fn last_voted_get(&self) -> Result<LastVoted, StorageError>;
    fn last_executed_get(&self) -> Result<LastExecuted, StorageError>;
    fn last_proposed_get(&self) -> Result<LastProposed, StorageError>;
    fn locked_block_get(&self, epoch: Epoch) -> Result<LockedBlock, StorageError>;
    fn leaf_block_get(&self, epoch: Epoch) -> Result<LeafBlock, StorageError>;
    fn high_qc_get(&self, epoch: Epoch) -> Result<HighQc, StorageError>;
    fn foreign_proposals_get_any<'a, I: IntoIterator<Item = &'a BlockId>>(
        &self,
        block_ids: I,
    ) -> Result<Vec<ForeignProposal>, StorageError>;
    fn foreign_proposals_exists(&self, block_id: &BlockId) -> Result<bool, StorageError>;
    fn foreign_proposals_has_unconfirmed(&self, epoch: Epoch) -> Result<bool, StorageError>;
    fn foreign_proposals_get_all_new(
        &self,
        block_id: &BlockId,
        limit: usize,
    ) -> Result<Vec<ForeignProposal>, StorageError>;
    fn foreign_proposal_get_all_pending(
        &self,
        from_block_id: &BlockId,
        to_block_id: &BlockId,
    ) -> Result<Vec<ForeignProposalAtom>, StorageError>;

    fn foreign_send_counters_get(&self, block_id: &BlockId) -> Result<ForeignSendCounters, StorageError>;
    fn foreign_receive_counters_get(&self) -> Result<ForeignReceiveCounters, StorageError>;
    fn transactions_get(&self, tx_id: &TransactionId) -> Result<TransactionRecord, StorageError>;
    fn transactions_exists(&self, tx_id: &TransactionId) -> Result<bool, StorageError>;

    fn transactions_get_any<'a, I: IntoIterator<Item = &'a TransactionId>>(
        &self,
        tx_ids: I,
    ) -> Result<Vec<TransactionRecord>, StorageError>;
    fn transactions_get_paginated(
        &self,
        limit: u64,
        offset: u64,
        asc_desc_created_at: Option<Ordering>,
    ) -> Result<Vec<TransactionRecord>, StorageError>;

    fn transaction_executions_get(
        &self,
        tx_id: &TransactionId,
        block: &BlockId,
    ) -> Result<BlockTransactionExecution, StorageError>;

    fn transaction_executions_get_pending_for_block(
        &self,
        tx_id: &TransactionId,
        from_block_id: &BlockId,
    ) -> Result<BlockTransactionExecution, StorageError>;
    fn blocks_get(&self, block_id: &BlockId) -> Result<Block, StorageError>;
    fn blocks_get_all_ids_by_height(&self, epoch: Epoch, height: NodeHeight) -> Result<Vec<BlockId>, StorageError>;
    fn blocks_get_genesis_for_epoch(&self, epoch: Epoch) -> Result<Block, StorageError>;
    fn blocks_get_last_n_in_epoch(&self, n: usize, epoch: Epoch) -> Result<Vec<Block>, StorageError>;
    /// Returns all blocks from and excluding the start block (lower height) to the end block (inclusive)
    fn blocks_get_all_between(
        &self,
        epoch: Epoch,
        shard_group: ShardGroup,
        start_block_height: NodeHeight,
        end_block_height: NodeHeight,
        include_dummy_blocks: bool,
        limit: u64,
    ) -> Result<Vec<Block>, StorageError>;
    fn blocks_exists(&self, block_id: &BlockId) -> Result<bool, StorageError>;
    fn blocks_is_ancestor(&self, descendant: &BlockId, ancestor: &BlockId) -> Result<bool, StorageError>;
    fn blocks_get_all_by_parent(&self, parent: &BlockId) -> Result<Vec<Block>, StorageError>;
    fn blocks_get_ids_by_parent(&self, parent: &BlockId) -> Result<Vec<BlockId>, StorageError>;
    fn blocks_get_parent_chain(&self, block_id: &BlockId, limit: usize) -> Result<Vec<Block>, StorageError>;
    fn blocks_get_pending_transactions(&self, block_id: &BlockId) -> Result<Vec<TransactionId>, StorageError>;

    fn blocks_get_any_with_epoch_range(
        &self,
        epoch_range: RangeInclusive<Epoch>,
        validator_public_key: Option<&PublicKey>,
    ) -> Result<Vec<Block>, StorageError>;
    fn blocks_get_paginated(
        &self,
        limit: u64,
        offset: u64,
        filter_index: Option<usize>,
        filter: Option<String>,
        ordering_index: Option<usize>,
        ordering: Option<Ordering>,
    ) -> Result<Vec<Block>, StorageError>;
    fn blocks_get_count(&self) -> Result<i64, StorageError>;

    fn filtered_blocks_get_count(
        &self,
        filter_index: Option<usize>,
        filter: Option<String>,
    ) -> Result<i64, StorageError>;
    fn blocks_max_height(&self) -> Result<NodeHeight, StorageError>;

    fn block_diffs_get(&self, block_id: &BlockId) -> Result<BlockDiff, StorageError>;
    fn block_diffs_get_last_change_for_substate(
        &self,
        block_id: &BlockId,
        substate_id: &SubstateId,
    ) -> Result<SubstateChange, StorageError>;
    fn block_diffs_get_change_for_versioned_substate<'a, T: Into<VersionedSubstateIdRef<'a>>>(
        &self,
        block_id: &BlockId,
        substate_id: T,
    ) -> Result<SubstateChange, StorageError>;

    // -------------------------------- QuorumCertificate -------------------------------- //
    fn quorum_certificates_get(&self, qc_id: &QcId) -> Result<QuorumCertificate, StorageError>;
    fn quorum_certificates_get_all<'a, I>(&self, qc_ids: I) -> Result<Vec<QuorumCertificate>, StorageError>
    where
        I: IntoIterator<Item = &'a QcId>,
        I::IntoIter: ExactSizeIterator;
    fn quorum_certificates_get_by_block_id(&self, block_id: &BlockId) -> Result<QuorumCertificate, StorageError>;

    // -------------------------------- Transaction Pools -------------------------------- //
    fn transaction_pool_get_for_blocks(
        &self,
        from_block_id: &BlockId,
        to_block_id: &BlockId,
        transaction_id: &TransactionId,
    ) -> Result<TransactionPoolRecord, StorageError>;
    fn transaction_pool_exists(&self, transaction_id: &TransactionId) -> Result<bool, StorageError>;
    fn transaction_pool_get_all(&self) -> Result<Vec<TransactionPoolRecord>, StorageError>;
    fn transaction_pool_get_many_ready(
        &self,
        max_txs: usize,
        block_id: &BlockId,
    ) -> Result<Vec<TransactionPoolRecord>, StorageError>;
    fn transaction_pool_has_pending_state_updates(&self, block_id: &BlockId) -> Result<bool, StorageError>;

    fn transaction_pool_count(
        &self,
        stage: Option<TransactionPoolStage>,
        is_ready: Option<bool>,
        confirmed_stage: Option<Option<TransactionPoolConfirmedStage>>,
        skip_lock_conflicted: bool,
    ) -> Result<usize, StorageError>;

    fn transactions_fetch_involved_shards(
        &self,
        transaction_ids: HashSet<TransactionId>,
    ) -> Result<HashSet<SubstateAddress>, StorageError>;

    // -------------------------------- Votes -------------------------------- //
    fn votes_get_by_block_and_sender(
        &self,
        block_id: &BlockId,
        sender_leaf_hash: &FixedHash,
    ) -> Result<Vote, StorageError>;
    fn votes_count_for_block(&self, block_id: &BlockId) -> Result<u64, StorageError>;
    fn votes_get_for_block(&self, block_id: &BlockId) -> Result<Vec<Vote>, StorageError>;
    //---------------------------------- Substates --------------------------------------------//
    fn substates_get(&self, address: &SubstateAddress) -> Result<SubstateRecord, StorageError>;
    fn substates_get_any<'a, I: IntoIterator<Item = &'a VersionedSubstateIdRef<'a>>>(
        &self,
        substate_ids: I,
    ) -> Result<Vec<SubstateRecord>, StorageError>;
    fn substates_get_any_max_version<'a, I>(&self, substate_ids: I) -> Result<Vec<SubstateRecord>, StorageError>
    where
        I: IntoIterator<Item = &'a SubstateId>,
        I::IntoIter: ExactSizeIterator;
    fn substates_get_max_version_for_substate(&self, substate_id: &SubstateId) -> Result<(u32, bool), StorageError>;
    fn substates_any_exist<I, S>(&self, substates: I) -> Result<bool, StorageError>
    where
        I: IntoIterator<Item = S>,
        S: Borrow<VersionedSubstateId>;

    fn substates_exists_for_transaction(&self, transaction_id: &TransactionId) -> Result<bool, StorageError>;

    fn substates_get_n_after(&self, n: usize, after: &SubstateAddress) -> Result<Vec<SubstateRecord>, StorageError>;

    fn substates_get_many_within_range(
        &self,
        start: &SubstateAddress,
        end: &SubstateAddress,
        exclude_shards: &[SubstateAddress],
    ) -> Result<Vec<SubstateRecord>, StorageError>;
    fn substates_get_many_by_created_transaction(
        &self,
        tx_id: &TransactionId,
    ) -> Result<Vec<SubstateRecord>, StorageError>;

    fn substates_get_many_by_destroyed_transaction(
        &self,
        tx_id: &TransactionId,
    ) -> Result<Vec<SubstateRecord>, StorageError>;
    fn substates_get_all_for_transaction(
        &self,
        transaction_id: &TransactionId,
    ) -> Result<Vec<SubstateRecord>, StorageError>;

    fn substate_locks_get_locked_substates_for_transaction(
        &self,
        transaction_id: &TransactionId,
    ) -> Result<Vec<LockedSubstateValue>, StorageError>;

    fn substate_locks_has_any_write_locks_for_substates<'a, I: IntoIterator<Item = &'a SubstateId>>(
        &self,
        exclude_transaction_id: Option<&TransactionId>,
        substate_ids: I,
        exclude_local_only: bool,
    ) -> Result<Option<TransactionId>, StorageError>;

    fn substate_locks_get_latest_for_substate(&self, substate_id: &SubstateId) -> Result<SubstateLock, StorageError>;

    fn pending_state_tree_diffs_get_all_up_to_commit_block(
        &self,
        block_id: &BlockId,
    ) -> Result<HashMap<Shard, Vec<PendingShardStateTreeDiff>>, StorageError>;

    fn state_transitions_get_n_after(
        &self,
        n: usize,
        id: StateTransitionId,
        end_epoch: Epoch,
    ) -> Result<Vec<StateTransition>, StorageError>;

    fn state_transitions_get_last_id(&self, shard: Shard) -> Result<StateTransitionId, StorageError>;

    fn state_tree_nodes_get(&self, shard: Shard, key: &NodeKey) -> Result<Node<Version>, StorageError>;
    fn state_tree_versions_get_latest(&self, shard: Shard) -> Result<Option<Version>, StorageError>;

    // -------------------------------- Epoch checkpoint -------------------------------- //
    fn epoch_checkpoint_get(&self, epoch: Epoch) -> Result<EpochCheckpoint, StorageError>;

    // -------------------------------- Foreign Substate Pledges -------------------------------- //
    fn foreign_substate_pledges_exists_for_transaction_and_address<T: ToSubstateAddress>(
        &self,
        transaction_id: &TransactionId,
        address: T,
    ) -> Result<bool, StorageError>;
    fn foreign_substate_pledges_get_write_pledges_to_transaction<'a, I: IntoIterator<Item = &'a SubstateId>>(
        &self,
        transaction_id: &TransactionId,
        substate_ids: I,
    ) -> Result<SubstatePledges, StorageError>;
    fn foreign_substate_pledges_get_all_by_transaction_id(
        &self,
        transaction_id: &TransactionId,
    ) -> Result<SubstatePledges, StorageError>;

    // -------------------------------- BurntUtxos -------------------------------- //
    fn burnt_utxos_get(&self, commitment: &UnclaimedConfidentialOutputAddress) -> Result<BurntUtxo, StorageError>;
    fn burnt_utxos_get_all_unproposed(
        &self,
        leaf_block: &BlockId,
        limit: usize,
    ) -> Result<Vec<BurntUtxo>, StorageError>;

    fn burnt_utxos_count(&self) -> Result<u64, StorageError>;

    // -------------------------------- Foreign parked block -------------------------------- //
    fn foreign_parked_blocks_exists(&self, block_id: &BlockId) -> Result<bool, StorageError>;

    // -------------------------------- ValidatorNodeStats -------------------------------- //
    fn validator_epoch_stats_get(
        &self,
        epoch: Epoch,
        public_key: &PublicKey,
    ) -> Result<ValidatorConsensusStats, StorageError>;

    fn validator_epoch_stats_get_nodes_to_evict(
        &self,
        block_id: &BlockId,
        threshold: u64,
        limit: u64,
    ) -> Result<Vec<PublicKey>, StorageError>;
    // -------------------------------- SuspendedNodes -------------------------------- //
    fn suspended_nodes_is_evicted(&self, block_id: &BlockId, public_key: &PublicKey) -> Result<bool, StorageError>;
    fn evicted_nodes_count(&self, epoch: Epoch) -> Result<u64, StorageError>;
}

pub trait StateStoreWriteTransaction {
    type Addr: NodeAddressable;

    fn commit(&mut self) -> Result<(), StorageError>;
    fn rollback(&mut self) -> Result<(), StorageError>;

    // -------------------------------- Block -------------------------------- //
    fn blocks_insert(&mut self, block: &Block) -> Result<(), StorageError>;
    fn blocks_delete(&mut self, block_id: &BlockId) -> Result<(), StorageError>;
    fn blocks_set_flags(
        &mut self,
        block_id: &BlockId,
        is_committed: Option<bool>,
        is_justified: Option<bool>,
    ) -> Result<(), StorageError>;

    // -------------------------------- BlockDiff -------------------------------- //
    fn block_diffs_insert(&mut self, block_id: &BlockId, changes: &[SubstateChange]) -> Result<(), StorageError>;
    fn block_diffs_remove(&mut self, block_id: &BlockId) -> Result<(), StorageError>;

    // -------------------------------- QuorumCertificate -------------------------------- //
    fn quorum_certificates_insert(&mut self, qc: &QuorumCertificate) -> Result<(), StorageError>;
    fn quorum_certificates_set_shares_processed(&mut self, qc_id: &QcId) -> Result<(), StorageError>;

    // -------------------------------- Bookkeeping -------------------------------- //
    fn last_sent_vote_set(&mut self, last_sent_vote: &LastSentVote) -> Result<(), StorageError>;
    fn last_voted_set(&mut self, last_voted: &LastVoted) -> Result<(), StorageError>;
    fn last_votes_unset(&mut self, last_voted: &LastVoted) -> Result<(), StorageError>;
    fn last_executed_set(&mut self, last_exec: &LastExecuted) -> Result<(), StorageError>;
    fn last_proposed_set(&mut self, last_proposed: &LastProposed) -> Result<(), StorageError>;
    fn last_proposed_unset(&mut self, last_proposed: &LastProposed) -> Result<(), StorageError>;
    fn leaf_block_set(&mut self, leaf_node: &LeafBlock) -> Result<(), StorageError>;
    fn locked_block_set(&mut self, locked_block: &LockedBlock) -> Result<(), StorageError>;
    fn high_qc_set(&mut self, high_qc: &HighQc) -> Result<(), StorageError>;
    fn foreign_proposals_upsert(
        &mut self,
        foreign_proposal: &ForeignProposal,
        proposed_in_block: Option<BlockId>,
    ) -> Result<(), StorageError>;
    fn foreign_proposals_delete(&mut self, block_id: &BlockId) -> Result<(), StorageError>;

    fn foreign_proposals_delete_in_epoch(&mut self, epoch: Epoch) -> Result<(), StorageError>;
    fn foreign_proposals_set_status(
        &mut self,
        block_id: &BlockId,
        status: ForeignProposalStatus,
    ) -> Result<(), StorageError>;

    fn foreign_proposals_set_proposed_in(
        &mut self,
        block_id: &BlockId,
        proposed_in_block: &BlockId,
    ) -> Result<(), StorageError>;
    fn foreign_proposals_clear_proposed_in(&mut self, proposed_in_block: &BlockId) -> Result<(), StorageError>;
    fn foreign_send_counters_set(
        &mut self,
        foreign_send_counter: &ForeignSendCounters,
        block_id: &BlockId,
    ) -> Result<(), StorageError>;
    fn foreign_receive_counters_set(
        &mut self,
        foreign_send_counter: &ForeignReceiveCounters,
    ) -> Result<(), StorageError>;

    // -------------------------------- Transaction -------------------------------- //
    fn transactions_insert(&mut self, transaction: &TransactionRecord) -> Result<(), StorageError>;
    fn transactions_update(&mut self, transaction: &TransactionRecord) -> Result<(), StorageError>;
    fn transactions_save_all<'a, I: IntoIterator<Item = &'a TransactionRecord>>(
        &mut self,
        transaction: I,
    ) -> Result<(), StorageError>;

    fn transactions_finalize_all<'a, I: IntoIterator<Item = &'a TransactionPoolRecord>>(
        &mut self,
        block_id: BlockId,
        transaction: I,
    ) -> Result<(), StorageError>;
    // -------------------------------- Transaction Executions -------------------------------- //
    fn transaction_executions_insert_or_ignore(
        &mut self,
        transaction_execution: &BlockTransactionExecution,
    ) -> Result<bool, StorageError>;

    fn transaction_executions_remove_any_by_block_id(&mut self, block_id: &BlockId) -> Result<(), StorageError>;

    // -------------------------------- Transaction Pool -------------------------------- //
    fn transaction_pool_insert_new(
        &mut self,
        tx_id: TransactionId,
        decision: Decision,
        initial_evidence: &Evidence,
        is_ready: bool,
        is_global: bool,
    ) -> Result<(), StorageError>;
    fn transaction_pool_add_pending_update(
        &mut self,
        block_id: &BlockId,
        pool_update: &TransactionPoolStatusUpdate,
    ) -> Result<(), StorageError>;

    fn transaction_pool_remove(&mut self, transaction_id: &TransactionId) -> Result<(), StorageError>;
    fn transaction_pool_remove_all<'a, I: IntoIterator<Item = &'a TransactionId>>(
        &mut self,
        transaction_ids: I,
    ) -> Result<Vec<TransactionPoolRecord>, StorageError>;
    fn transaction_pool_confirm_all_transitions(&mut self, new_locked_block: &LockedBlock) -> Result<(), StorageError>;
    fn transaction_pool_state_updates_remove_any_by_block_id(&mut self, block_id: &BlockId)
        -> Result<(), StorageError>;

    // -------------------------------- Missing Transactions -------------------------------- //

    fn missing_transactions_insert<'a, IMissing: IntoIterator<Item = &'a TransactionId>>(
        &mut self,
        park_block: &Block,
        foreign_proposals: &[ForeignProposal],
        missing_transaction_ids: IMissing,
    ) -> Result<(), StorageError>;

    fn missing_transactions_remove(
        &mut self,
        height: NodeHeight,
        transaction_id: &TransactionId,
    ) -> Result<Option<(Block, Vec<ForeignProposal>)>, StorageError>;

    fn foreign_parked_blocks_insert(&mut self, park_block: &ForeignParkedProposal) -> Result<(), StorageError>;

    fn foreign_parked_blocks_insert_missing_transactions<'a, I: IntoIterator<Item = &'a TransactionId>>(
        &mut self,
        park_block_id: &BlockId,
        missing_transaction_ids: I,
    ) -> Result<(), StorageError>;

    fn foreign_parked_blocks_remove_all_by_transaction(
        &mut self,
        transaction_id: &TransactionId,
    ) -> Result<Vec<ForeignParkedProposal>, StorageError>;

    // -------------------------------- Votes -------------------------------- //
    fn votes_insert(&mut self, vote: &Vote) -> Result<(), StorageError>;

    fn votes_delete_all(&mut self) -> Result<(), StorageError>;

    //---------------------------------- Substates --------------------------------------------//
    fn substate_locks_insert_all<'a, I: IntoIterator<Item = (&'a SubstateId, &'a Vec<SubstateLock>)>>(
        &mut self,
        block_id: &BlockId,
        locks: I,
    ) -> Result<(), StorageError>;

    fn substate_locks_remove_many_for_transactions<'a, I: Iterator<Item = &'a TransactionId>>(
        &mut self,
        transaction_ids: I,
    ) -> Result<(), StorageError>;

    fn substate_locks_remove_any_by_block_id(&mut self, block_id: &BlockId) -> Result<(), StorageError>;

    fn substates_create(&mut self, substate: &SubstateRecord) -> Result<(), StorageError>;
    fn substates_down(
        &mut self,
        versioned_substate_id: VersionedSubstateId,
        shard: Shard,
        epoch: Epoch,
        destroyed_block_height: NodeHeight,
        destroyed_transaction_id: &TransactionId,
        destroyed_qc_id: &QcId,
    ) -> Result<(), StorageError>;

    // -------------------------------- Foreign pledges -------------------------------- //

    #[allow(clippy::mutable_key_type)]
    fn foreign_substate_pledges_save(
        &mut self,
        transaction_id: &TransactionId,
        shard_group: ShardGroup,
        pledges: &SubstatePledges,
    ) -> Result<(), StorageError>;

    fn foreign_substate_pledges_remove_many<'a, I: IntoIterator<Item = &'a TransactionId>>(
        &mut self,
        transaction_ids: I,
    ) -> Result<(), StorageError>;

    // -------------------------------- Pending State Tree Diffs -------------------------------- //
    fn pending_state_tree_diffs_insert(
        &mut self,
        block_id: BlockId,
        shard: Shard,
        diff: &VersionedStateHashTreeDiff,
    ) -> Result<(), StorageError>;
    fn pending_state_tree_diffs_remove_by_block(&mut self, block_id: &BlockId) -> Result<(), StorageError>;
    fn pending_state_tree_diffs_remove_and_return_by_block(
        &mut self,
        block_id: &BlockId,
    ) -> Result<IndexMap<Shard, Vec<PendingShardStateTreeDiff>>, StorageError>;

    //---------------------------------- State tree --------------------------------------------//
    fn state_tree_nodes_insert(&mut self, shard: Shard, key: NodeKey, node: Node<Version>) -> Result<(), StorageError>;

    fn state_tree_nodes_record_stale_tree_node(
        &mut self,
        shard: Shard,
        node: StaleTreeNode,
    ) -> Result<(), StorageError>;
    fn state_tree_shard_versions_set(&mut self, shard: Shard, version: Version) -> Result<(), StorageError>;

    // -------------------------------- Epoch checkpoint -------------------------------- //
    fn epoch_checkpoint_save(&mut self, checkpoint: &EpochCheckpoint) -> Result<(), StorageError>;

    // -------------------------------- BurntUtxo -------------------------------- //
    fn burnt_utxos_insert(&mut self, burnt_utxo: &BurntUtxo) -> Result<(), StorageError>;
    fn burnt_utxos_set_proposed_block(
        &mut self,
        commitment: &UnclaimedConfidentialOutputAddress,
        proposed_in_block: &BlockId,
    ) -> Result<(), StorageError>;
    fn burnt_utxos_clear_proposed_block(&mut self, proposed_in_block: &BlockId) -> Result<(), StorageError>;
    fn burnt_utxos_delete(&mut self, commitment: &UnclaimedConfidentialOutputAddress) -> Result<(), StorageError>;

    // -------------------------------- Lock conflicts -------------------------------- //
    fn lock_conflicts_insert_all<'a, I: IntoIterator<Item = (&'a TransactionId, &'a Vec<LockConflict>)>>(
        &mut self,
        block_id: &BlockId,
        conflicts: I,
    ) -> Result<(), StorageError>;

    fn lock_conflicts_remove_by_transaction_ids<'a, I: IntoIterator<Item = &'a TransactionId>>(
        &mut self,
        transaction_ids: I,
    ) -> Result<(), StorageError>;

    fn lock_conflicts_remove_by_block_id(&mut self, block_id: &BlockId) -> Result<(), StorageError>;

    // -------------------------------- ParticipationShares -------------------------------- //
    fn validator_epoch_stats_add_participation_share(&mut self, qc_id: &QcId) -> Result<(), StorageError>;
    fn validator_epoch_stats_updates<'a, I: IntoIterator<Item = ValidatorStatsUpdate<'a>>>(
        &mut self,
        epoch: Epoch,
        updates: I,
    ) -> Result<(), StorageError>;

    // -------------------------------- SuspendedNodes -------------------------------- //

    fn evicted_nodes_evict(&mut self, public_key: &PublicKey, evicted_in_block: BlockId) -> Result<(), StorageError>;
    fn evicted_nodes_mark_eviction_as_committed(
        &mut self,
        public_key: &PublicKey,
        epoch: Epoch,
    ) -> Result<(), StorageError>;

    // -------------------------------- Diagnotics -------------------------------- //
    fn diagnostics_add_no_vote(&mut self, block_id: BlockId, reason: NoVoteReason) -> Result<(), StorageError>;
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(TS), ts(export, export_to = "../../bindings/src/types/"))]
pub enum Ordering {
    Ascending,
    Descending,
}
