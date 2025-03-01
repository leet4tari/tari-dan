//   Copyright 2023 The Tari Project
//   SPDX-License-Identifier: BSD-3-Clause

use std::{
    clone::Clone,
    fmt::{Display, Formatter},
    marker::PhantomData,
    num::NonZeroU64,
    str::FromStr,
};

use log::*;
use serde::Serialize;
use tari_dan_common_types::{
    committee::CommitteeInfo,
    displayable::Displayable,
    optional::{IsNotFoundError, Optional},
    NumPreshards,
    SubstateAddress,
    SubstateLockType,
};
use tari_engine_types::{substate::SubstateId, transaction_receipt::TransactionReceiptAddress};
use tari_transaction::TransactionId;

use crate::{
    consensus_models::{
        BlockId,
        BlockTransactionExecution,
        Decision,
        Evidence,
        LeaderFee,
        LeafBlock,
        LockedBlock,
        TransactionAtom,
        TransactionExecution,
        TransactionRecord,
    },
    StateStore,
    StateStoreReadTransaction,
    StateStoreWriteTransaction,
    StorageError,
};

const LOG_TARGET: &str = "tari::dan::storage::transaction_pool";

#[derive(Debug, Clone, Default)]
pub struct TransactionPool<TStateStore> {
    _store: PhantomData<TStateStore>,
}

impl<TStateStore: StateStore> TransactionPool<TStateStore> {
    pub fn new() -> Self {
        Self { _store: PhantomData }
    }

    pub fn get(
        &self,
        tx: &TStateStore::ReadTransaction<'_>,
        leaf: &LeafBlock,
        id: &TransactionId,
    ) -> Result<TransactionPoolRecord, TransactionPoolError> {
        // We always want to fetch the state at the current leaf block until the leaf block
        let locked = LockedBlock::get(tx, leaf.epoch())?;
        debug!(
            target: LOG_TARGET,
            "TransactionPool::get: transaction_id {}, leaf block {} and locked block {}",
            id,
            leaf,
            locked,
        );
        let rec = tx.transaction_pool_get_for_blocks(locked.block_id(), leaf.block_id(), id)?;
        Ok(rec)
    }

    pub fn exists(
        &self,
        tx: &TStateStore::ReadTransaction<'_>,
        id: &TransactionId,
    ) -> Result<bool, TransactionPoolError> {
        let exists = tx.transaction_pool_exists(id)?;
        Ok(exists)
    }

    pub fn insert_new(
        &self,
        tx: &mut TStateStore::WriteTransaction<'_>,
        tx_id: TransactionId,
        decision: Decision,
        initial_evidence: &Evidence,
        is_ready: bool,
        is_global: bool,
    ) -> Result<(), TransactionPoolError> {
        tx.transaction_pool_insert_new(tx_id, decision, initial_evidence, is_ready, is_global)?;
        Ok(())
    }

    pub fn insert_new_batched<'a, I: IntoIterator<Item = (&'a TransactionRecord, bool)>>(
        &self,
        tx: &mut TStateStore::WriteTransaction<'_>,
        num_preshards: NumPreshards,
        num_committees: u32,
        transactions: I,
    ) -> Result<(), TransactionPoolError> {
        // TODO(perf)
        for (transaction, is_ready) in transactions {
            tx.transaction_pool_insert_new(
                *transaction.id(),
                transaction.current_decision(),
                &transaction.to_initial_evidence(num_preshards, num_committees),
                is_ready,
                transaction.transaction().is_global(),
            )?;
        }
        Ok(())
    }

    pub fn get_batch_for_next_block(
        &self,
        tx: &TStateStore::ReadTransaction<'_>,
        max: usize,
        block_id: &BlockId,
    ) -> Result<Vec<TransactionPoolRecord>, TransactionPoolError> {
        if max == 0 {
            return Ok(Vec::new());
        }
        let recs = tx.transaction_pool_get_many_ready(max, block_id)?;
        Ok(recs)
    }

    pub fn has_ready_or_pending_transaction_updates(
        &self,
        tx: &TStateStore::ReadTransaction<'_>,
        block_id: &BlockId,
    ) -> Result<bool, TransactionPoolError> {
        // Check if any pending transactions have state updates that need to be applied
        if tx.transaction_pool_has_pending_state_updates(block_id)? {
            debug!(
                target: LOG_TARGET,
                "has_ready_or_pending_transaction_updates: Pending state updates found",
            );
            return Ok(true);
        }
        debug!(
            target: LOG_TARGET,
            "has_ready_or_pending_transaction_updates: No pending state updates",
        );

        // Check if any transactions are marked as ready to propose
        let count = tx.transaction_pool_count(None, Some(true), None, true)?;
        if count > 0 {
            debug!(
                target: LOG_TARGET,
                "has_ready_or_pending_transaction_updates: {} transactions marked as ready",
                count,
            );
            return Ok(true);
        }
        debug!(
            target: LOG_TARGET,
            "has_ready_or_pending_transaction_updates: No transactions marked as ready",
        );

        // Check if we have transactions that have not yet been confirmed (locked). In this case we should propose
        // until this stage is locked.
        // let count = tx.transaction_pool_count(None, None, Some(None))?;
        // if count > 0 {
        //     return Ok(true);
        // }

        let count = tx.transaction_pool_count(Some(TransactionPoolStage::LocalOnly), None, None, true)?;
        if count > 0 {
            debug!(
                target: LOG_TARGET,
                "has_ready_or_pending_transaction_updates: {} transactions that need to be finalized (LocalOnly)",
                count,
            );
            return Ok(true);
        }

        // Check if we have multishard transactions that need to be finalized. These checks apply to transactions that
        // have been locked but not committed.
        let count = tx.transaction_pool_count(Some(TransactionPoolStage::AllAccepted), None, None, true)?;
        if count > 0 {
            debug!(
                target: LOG_TARGET,
                "has_ready_or_pending_transaction_updates: {} transactions that need to be finalized (AllAccepted)",
                count,
            );
            return Ok(true);
        }

        let count = tx.transaction_pool_count(Some(TransactionPoolStage::SomeAccepted), None, None, true)?;
        if count > 0 {
            debug!(
                target: LOG_TARGET,
                "has_ready_or_pending_transaction_updates: {} transactions that need to be finalized (SomeAccepted)",
                count,
            );
            return Ok(true);
        }

        debug!(
            target: LOG_TARGET,
            "has_ready_or_pending_transaction_updates: No transactions that need to be finalized",
        );

        Ok(false)
    }

    pub fn count(&self, tx: &TStateStore::ReadTransaction<'_>) -> Result<usize, TransactionPoolError> {
        let count = tx.transaction_pool_count(None, None, None, false)?;
        Ok(count)
    }

    pub fn confirm_all_transitions(
        &self,
        tx: &mut TStateStore::WriteTransaction<'_>,
        locked_block: &LockedBlock,
    ) -> Result<(), TransactionPoolError> {
        tx.transaction_pool_confirm_all_transitions(locked_block)?;
        Ok(())
    }

    pub fn remove_all<'a, I: IntoIterator<Item = &'a TransactionId>>(
        &self,
        tx: &mut TStateStore::WriteTransaction<'_>,
        tx_ids: I,
    ) -> Result<Vec<TransactionPoolRecord>, TransactionPoolError> {
        TransactionPoolRecord::remove_all(tx, tx_ids)
    }
}

// Ord: ensure that the enum variants are ordered in the order of their progression
#[derive(Debug, Clone, Copy, PartialEq, Eq, Ord, PartialOrd, Serialize)]
#[cfg_attr(
    feature = "ts",
    derive(ts_rs::TS),
    ts(export, export_to = "../../bindings/src/types/")
)]
pub enum TransactionPoolStage {
    /// Transaction has just come in and has never been proposed
    New,
    /// Transaction is prepared in response to a Prepare command, but we do not yet have confirmation that the rest of
    /// the local committee has prepared.
    Prepared,
    /// We have proof that all local committees have prepared the transaction
    LocalPrepared,
    /// All involved shard groups have prepared and all have pledged their local inputs
    AllPrepared,
    /// Some involved shard groups have prepared but one or more did not successfully pledge their local inputs
    SomePrepared,
    /// The local shard group has accepted the transaction
    LocalAccepted,
    /// All involved shard groups have accepted the transaction
    AllAccepted,
    /// Some involved shard groups have accepted the transaction, but one or more have decided to ABORT
    SomeAccepted,
    /// Only involves local shards. This transaction can be executed and accepted without cross-shard agreement.
    LocalOnly,
}

impl TransactionPoolStage {
    pub fn is_new(&self) -> bool {
        matches!(self, Self::New)
    }

    pub fn is_local_only(&self) -> bool {
        matches!(self, Self::LocalOnly)
    }

    pub fn is_prepared(&self) -> bool {
        matches!(self, Self::Prepared)
    }

    pub fn is_local_prepared(&self) -> bool {
        matches!(self, Self::LocalPrepared)
    }

    pub fn is_local_accepted(&self) -> bool {
        matches!(self, Self::LocalAccepted)
    }

    pub fn is_some_prepared(&self) -> bool {
        matches!(self, Self::SomePrepared)
    }

    pub fn is_all_prepared(&self) -> bool {
        matches!(self, Self::AllPrepared)
    }

    pub fn is_all_accepted(&self) -> bool {
        matches!(self, Self::AllAccepted)
    }

    pub fn is_some_accepted(&self) -> bool {
        matches!(self, Self::SomeAccepted)
    }

    pub fn is_finalising(&self) -> bool {
        self.is_local_only() || self.is_all_accepted() || self.is_some_accepted()
    }
}

impl Display for TransactionPoolStage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

impl FromStr for TransactionPoolStage {
    type Err = TransactionPoolStageFromStrErr;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "New" => Ok(TransactionPoolStage::New),
            "Prepared" => Ok(TransactionPoolStage::Prepared),
            "LocalPrepared" => Ok(TransactionPoolStage::LocalPrepared),
            "SomePrepared" => Ok(TransactionPoolStage::SomePrepared),
            "AllPrepared" => Ok(TransactionPoolStage::AllPrepared),
            "LocalAccepted" => Ok(TransactionPoolStage::LocalAccepted),
            "AllAccepted" => Ok(TransactionPoolStage::AllAccepted),
            "SomeAccepted" => Ok(TransactionPoolStage::SomeAccepted),
            "LocalOnly" => Ok(TransactionPoolStage::LocalOnly),
            s => Err(TransactionPoolStageFromStrErr(s.to_string())),
        }
    }
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("Invalid TransactionPoolStage string '{0}'")]
pub struct TransactionPoolStageFromStrErr(String);

#[derive(Debug, Clone)]
pub enum TransactionPoolConfirmedStage {
    ConfirmedPrepared,
    ConfirmedAccepted,
}

impl Display for TransactionPoolConfirmedStage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TransactionPoolConfirmedStage::ConfirmedPrepared => write!(f, "ConfirmedPrepared"),
            TransactionPoolConfirmedStage::ConfirmedAccepted => write!(f, "ConfirmedAccepted"),
        }
    }
}

impl FromStr for TransactionPoolConfirmedStage {
    type Err = TransactionPoolConfirmedStageFromStrErr;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ConfirmedPrepared" => Ok(TransactionPoolConfirmedStage::ConfirmedPrepared),
            "ConfirmedAccepted" => Ok(TransactionPoolConfirmedStage::ConfirmedAccepted),
            s => Err(TransactionPoolConfirmedStageFromStrErr(s.to_string())),
        }
    }
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("Invalid TransactionPoolConfirmedStage string '{0}'")]
pub struct TransactionPoolConfirmedStageFromStrErr(String);

#[derive(Debug, Clone, Serialize)]
#[cfg_attr(
    feature = "ts",
    derive(ts_rs::TS),
    ts(export, export_to = "../../bindings/src/types/")
)]
pub struct TransactionPoolRecord {
    #[cfg_attr(feature = "ts", ts(type = "string"))]
    transaction_id: TransactionId,
    evidence: Evidence,
    is_global: bool,
    #[cfg_attr(feature = "ts", ts(type = "number"))]
    transaction_fee: u64,
    leader_fee: Option<LeaderFee>,
    stage: TransactionPoolStage,
    pending_stage: Option<TransactionPoolStage>,
    original_decision: Decision,
    local_decision: Option<Decision>,
    remote_decision: Option<Decision>,
    is_ready: bool,
}

impl TransactionPoolRecord {
    pub fn load(
        id: TransactionId,
        evidence: Evidence,
        is_global: bool,
        transaction_fee: u64,
        leader_fee: Option<LeaderFee>,
        stage: TransactionPoolStage,
        pending_stage: Option<TransactionPoolStage>,
        original_decision: Decision,
        local_decision: Option<Decision>,
        remote_decision: Option<Decision>,
        is_ready: bool,
    ) -> Self {
        Self {
            transaction_id: id,
            evidence,
            is_global,
            transaction_fee,
            leader_fee,
            stage,
            pending_stage,
            original_decision,
            local_decision,
            remote_decision,
            is_ready,
        }
    }

    pub fn current_decision(&self) -> Decision {
        self.remote_decision()
            // Prioritize remote ABORT i.e. if accept we look at our local decision
            .filter(|d| d.is_abort())
            .unwrap_or_else(|| self.current_local_decision())
    }

    fn can_continue_to(&self, stage: TransactionPoolStage) -> bool {
        match stage {
            TransactionPoolStage::New => self.is_ready,
            TransactionPoolStage::Prepared => true,
            TransactionPoolStage::LocalPrepared => match self.current_decision() {
                Decision::Commit => self.evidence.all_input_shard_groups_prepared(),
                Decision::Abort(_) => self.evidence.some_shard_groups_prepared(),
            },
            TransactionPoolStage::AllPrepared => self.evidence.all_input_shard_groups_prepared(),
            TransactionPoolStage::SomePrepared => self.evidence.some_shard_groups_prepared(),
            TransactionPoolStage::LocalAccepted => match self.current_decision() {
                Decision::Commit => self.evidence.all_shard_groups_accepted(),
                // If we have decided to abort, we can continue if any foreign shard or locally has prepared
                Decision::Abort(_) => self.evidence.some_shard_groups_prepared(),
            },
            TransactionPoolStage::AllAccepted |
            TransactionPoolStage::SomeAccepted |
            TransactionPoolStage::LocalOnly => false,
        }
    }

    pub fn is_ready_for_pending_stage(&self) -> bool {
        self.can_continue_to(self.current_stage())
    }

    pub fn current_local_decision(&self) -> Decision {
        self.local_decision().unwrap_or(self.original_decision())
    }

    pub fn original_decision(&self) -> Decision {
        self.original_decision
    }

    pub fn local_decision(&self) -> Option<Decision> {
        self.local_decision
    }

    pub fn remote_decision(&self) -> Option<Decision> {
        self.remote_decision
    }

    pub fn transaction_id(&self) -> &TransactionId {
        &self.transaction_id
    }

    pub fn evidence(&self) -> &Evidence {
        &self.evidence
    }

    pub fn evidence_mut(&mut self) -> &mut Evidence {
        &mut self.evidence
    }

    pub fn transaction_fee(&self) -> u64 {
        self.transaction_fee
    }

    /// Returns the committed stage of the transaction. This is the stage that has been confirmed by the local shard.
    pub fn committed_stage(&self) -> TransactionPoolStage {
        self.stage
    }

    /// Returns the pending stage of the transaction. This is the stage that the transaction is current but has not been
    /// confirmed by the local shard.
    pub fn pending_stage(&self) -> Option<TransactionPoolStage> {
        self.pending_stage
    }

    pub fn current_stage(&self) -> TransactionPoolStage {
        self.pending_stage.unwrap_or(self.stage)
    }

    pub fn leader_fee(&self) -> Option<&LeaderFee> {
        if self.current_decision().is_abort() {
            return None;
        }
        self.leader_fee.as_ref()
    }

    pub fn is_ready(&self) -> bool {
        self.is_ready
    }

    pub fn to_receipt_id(&self) -> TransactionReceiptAddress {
        (*self.transaction_id()).into()
    }

    pub fn get_current_transaction_atom(&self) -> TransactionAtom {
        TransactionAtom {
            id: self.transaction_id,
            decision: self.current_decision(),
            evidence: self.evidence.clone(),
            transaction_fee: self.transaction_fee,
            leader_fee: self.leader_fee().cloned(),
        }
    }

    pub fn get_local_transaction_atom(&self) -> TransactionAtom {
        TransactionAtom {
            id: self.transaction_id,
            decision: self.current_local_decision(),
            evidence: self.evidence.clone(),
            transaction_fee: self.transaction_fee,
            leader_fee: self.leader_fee().cloned(),
        }
    }

    pub fn into_current_transaction_atom(self) -> TransactionAtom {
        TransactionAtom {
            id: self.transaction_id,
            decision: self.current_decision(),
            leader_fee: self.leader_fee().cloned(),
            evidence: self.evidence,
            transaction_fee: self.transaction_fee,
        }
    }

    pub fn is_global(&self) -> bool {
        self.is_global
    }

    pub fn calculate_leader_fee(&self, num_involved_shards: NonZeroU64, exhaust_divisor: u64) -> LeaderFee {
        let target_burn = self.transaction_fee.checked_div(exhaust_divisor).unwrap_or(0);
        let block_fee_after_burn = self.transaction_fee - target_burn;

        let mut leader_fee = block_fee_after_burn / num_involved_shards;
        // The extra amount that is burnt from dividing the number of shards involved
        let excess_remainder_burn = block_fee_after_burn % num_involved_shards;

        // Adjust the leader fee to account for the remainder
        // If the remainder accounts for an extra burn of greater than half the number of involved shards, we
        // give each validator an extra 1 in fees if enough fees are available, burning less than the exhaust target.
        // Otherwise, we burn a little more than/equal to the exhaust target.
        let actual_burn = if excess_remainder_burn > 0 &&
            // If the div floor burn accounts for 1 less fee for more than half of number of shards, and ...
            excess_remainder_burn >= num_involved_shards.get() / 2 &&
            // ... if there are enough fees to pay out an additional 1 to all shards
            (leader_fee + 1) * num_involved_shards.get() <= self.transaction_fee
        {
            // Pay each leader 1 more
            leader_fee += 1;

            // We burn a little less due to the remainder
            target_burn.saturating_sub(num_involved_shards.get() - excess_remainder_burn)
        } else {
            // We burn a little more due to the remainder
            target_burn + excess_remainder_burn
        };

        LeaderFee {
            fee: leader_fee,
            global_exhaust_burn: actual_burn,
        }
    }

    pub fn set_remote_decision(&mut self, decision: Decision) -> &mut Self {
        // Only set remote_decision to ABORT, or COMMIT if it is not already ABORT
        let decision = self.remote_decision().map(|d| d.and(decision)).unwrap_or(decision);
        self.remote_decision = Some(decision);
        if decision.is_abort() {
            self.evidence.abort();
        }
        self
    }

    pub fn set_local_decision(&mut self, decision: Decision) -> &mut Self {
        self.local_decision = Some(decision);
        // Represents that no substates are locked/pledged when ABORT
        if decision.is_abort() {
            self.evidence.abort();
        }
        self
    }

    pub fn set_transaction_fee(&mut self, transaction_fee: u64) -> &mut Self {
        self.transaction_fee = transaction_fee;
        self
    }

    pub fn set_leader_fee(&mut self, leader_fee: LeaderFee) -> &mut Self {
        self.leader_fee = Some(leader_fee);
        self
    }

    pub fn update_from_execution(
        &mut self,
        num_preshards: NumPreshards,
        num_committees: u32,
        execution: &TransactionExecution,
    ) -> &mut Self {
        // Only change the local decision if we haven't already decided to ABORT
        if self.local_decision().map_or(true, |d| d.is_commit()) {
            self.set_local_decision(execution.decision());
        }

        let involved_locks = execution.resolved_inputs().iter().chain(execution.resulting_outputs());
        for lock in involved_locks {
            self.evidence_mut()
                .insert_from_lock_intent(num_preshards, num_committees, lock);
        }
        if self.current_decision().is_abort() {
            self.evidence.abort();
        }

        self.set_transaction_fee(execution.transaction_fee());
        self
    }

    pub fn set_next_stage(&mut self, next_stage: TransactionPoolStage) -> Result<(), TransactionPoolError> {
        let is_ready = self.can_continue_to(next_stage);
        self.check_pending_status_update(next_stage, is_ready)?;
        info!(
            target: LOG_TARGET,
            "📝 Setting next update for transaction {} to {}->{},is_ready={}->{},{}->{}",
            self.transaction_id(),
            self.current_stage(),
            next_stage,
            self.is_ready,
            is_ready,
            self.current_local_decision(),
            self.current_decision(),
        );
        self.pending_stage = Some(next_stage);
        self.is_ready = is_ready;
        Ok(())
    }

    pub fn set_ready(&mut self, is_ready: bool) -> &mut Self {
        self.is_ready = is_ready;
        self
    }

    pub fn set_evidence(&mut self, evidence: Evidence) -> &mut Self {
        self.evidence = evidence;
        self
    }

    pub fn check_pending_status_update(
        &self,
        pending_stage: TransactionPoolStage,
        is_ready: bool,
    ) -> Result<(), TransactionPoolError> {
        // Check that only permitted stage transactions are performed
        match ((self.current_stage(), pending_stage), is_ready) {
            ((TransactionPoolStage::New, TransactionPoolStage::New), true) |
            ((TransactionPoolStage::New, TransactionPoolStage::Prepared), true) |
            ((TransactionPoolStage::New, TransactionPoolStage::LocalOnly), false) |
            // Prepared
            ((TransactionPoolStage::Prepared, TransactionPoolStage::Prepared), _) |
            ((TransactionPoolStage::Prepared, TransactionPoolStage::LocalPrepared), _) |
            // Output-only case - we can skip straight to LocalAccepted
            ((TransactionPoolStage::Prepared, TransactionPoolStage::LocalAccepted), _) |
            // LocalPrepared
            ((TransactionPoolStage::LocalPrepared, TransactionPoolStage::LocalPrepared), _) |
            ((TransactionPoolStage::LocalPrepared, TransactionPoolStage::AllPrepared), _) |
            ((TransactionPoolStage::LocalPrepared, TransactionPoolStage::SomePrepared), _) |
            // AllPrepared
            ((TransactionPoolStage::AllPrepared, TransactionPoolStage::AllPrepared), _) |
            ((TransactionPoolStage::AllPrepared, TransactionPoolStage::LocalAccepted), _) |
            // SomePrepared
            ((TransactionPoolStage::SomePrepared, TransactionPoolStage::SomePrepared), _) |
            ((TransactionPoolStage::SomePrepared, TransactionPoolStage::LocalAccepted), _) |
            // LocalAccepted
            ((TransactionPoolStage::LocalAccepted, TransactionPoolStage::LocalAccepted), _) |
            ((TransactionPoolStage::LocalAccepted, TransactionPoolStage::AllAccepted), false) |
            ((TransactionPoolStage::LocalAccepted, TransactionPoolStage::SomeAccepted), false) |
            // Accepted
            ((TransactionPoolStage::AllAccepted, TransactionPoolStage::AllAccepted), false) => {}
            _ => {
                return Err(TransactionPoolError::InvalidTransactionTransition {
                    from: self.current_stage(),
                    to: pending_stage,
                    is_ready,
                });
            }
        }

        Ok(())
    }
}

impl TransactionPoolRecord {
    pub fn remove<TTx: StateStoreWriteTransaction>(&self, tx: &mut TTx) -> Result<(), TransactionPoolError> {
        tx.transaction_pool_remove(&self.transaction_id)?;
        Ok(())
    }

    pub fn remove_any<'a, TTx, I>(tx: &mut TTx, transaction_ids: I) -> Result<(), TransactionPoolError>
    where
        TTx: StateStoreWriteTransaction,
        I: IntoIterator<Item = &'a TransactionId>,
    {
        // TODO(perf): n queries
        for id in transaction_ids {
            let _ = tx.transaction_pool_remove(id).optional()?;
        }
        Ok(())
    }

    pub fn remove_all<'a, TTx, I>(
        tx: &mut TTx,
        transaction_ids: I,
    ) -> Result<Vec<TransactionPoolRecord>, TransactionPoolError>
    where
        TTx: StateStoreWriteTransaction,
        I: IntoIterator<Item = &'a TransactionId>,
    {
        let recs = tx.transaction_pool_remove_all(transaction_ids)?;
        let iter = recs.iter().map(|rec| rec.transaction_id());
        // Clear any related foreign pledges
        tx.foreign_substate_pledges_remove_many(iter.clone())?;
        // Clear any related lock_conflicts
        tx.lock_conflicts_remove_by_transaction_ids(iter)?;
        Ok(recs)
    }

    pub fn get<TTx: StateStoreReadTransaction>(
        tx: &TTx,
        from_block_id: &BlockId,
        to_block_id: &BlockId,
        transaction_id: &TransactionId,
    ) -> Result<TransactionPoolRecord, TransactionPoolError> {
        let rec = tx.transaction_pool_get_for_blocks(from_block_id, to_block_id, transaction_id)?;
        Ok(rec)
    }

    pub fn get_transaction<TTx: StateStoreReadTransaction>(
        &self,
        tx: &TTx,
    ) -> Result<TransactionRecord, TransactionPoolError> {
        let transaction = TransactionRecord::get(tx, self.transaction_id())?;
        Ok(transaction)
    }

    pub fn get_execution_for_block<TTx: StateStoreReadTransaction>(
        &self,
        tx: &TTx,
        from_block_id: &BlockId,
    ) -> Result<BlockTransactionExecution, TransactionPoolError> {
        let exec = BlockTransactionExecution::get_pending_for_block(tx, self.transaction_id(), from_block_id)?;
        Ok(exec)
    }

    pub fn involves_committee(&self, committee_info: &CommitteeInfo) -> bool {
        self.evidence.contains(&committee_info.shard_group())
    }

    pub fn committee_involves_inputs(&self, committee_info: &CommitteeInfo) -> bool {
        self.evidence
            .get(&committee_info.shard_group())
            .is_some_and(|e| !e.inputs().is_empty())
    }

    pub fn has_all_required_foreign_pledges<TTx: StateStoreReadTransaction>(
        &self,
        tx: &TTx,
        local_committee_info: &CommitteeInfo,
    ) -> Result<bool, StorageError> {
        let involved_objects = self
            .evidence()
            .all_inputs_iter()
            .map(|(_, substate_id, evidence)| (substate_id, evidence.map(|e| (e.version, e.as_lock_type()))))
            .chain(
                self.evidence()
                    .all_outputs_iter()
                    .map(|(_, substate_id, version)| (substate_id, Some((*version, SubstateLockType::Output)))),
            )
            .filter(|(substate_id, _)| !local_committee_info.includes_substate_id(substate_id));

        self.has_foreign_pledges_for_objects(tx, local_committee_info, involved_objects)
    }

    pub fn has_all_required_foreign_input_pledges<TTx: StateStoreReadTransaction>(
        &self,
        tx: &TTx,
        local_committee_info: &CommitteeInfo,
    ) -> Result<bool, StorageError> {
        let involved_objects = self
            .evidence()
            .all_inputs_iter()
            .map(|(_, substate_id, evidence)| (substate_id, evidence.map(|e| (e.version, e.as_lock_type()))))
            .filter(|(substate_id, _)| !local_committee_info.includes_substate_id(substate_id));

        self.has_foreign_pledges_for_objects(tx, local_committee_info, involved_objects)
    }

    fn has_foreign_pledges_for_objects<'a, TTx, TObj>(
        &self,
        tx: &TTx,
        local_committee_info: &CommitteeInfo,
        involved_objects: TObj,
    ) -> Result<bool, StorageError>
    where
        TTx: StateStoreReadTransaction,
        TObj: IntoIterator<Item = (&'a SubstateId, Option<(u32, SubstateLockType)>)>,
    {
        for (substate_id, data) in involved_objects {
            let Some((version, lock_type)) = data else {
                debug!(
                    target: LOG_TARGET,
                    "Transaction {} is missing a version for substate_id {}",
                    self.transaction_id(),
                    substate_id,
                );
                return Ok(false);
            };
            let address = SubstateAddress::from_substate_id(substate_id, version);
            // TODO(perf): O(n) queries
            if tx.foreign_substate_pledges_exists_for_transaction_and_address(self.transaction_id(), address)? {
                continue;
            }

            if log_enabled!(Level::Debug) {
                // Load them for debugging purposes
                let pledges = tx.foreign_substate_pledges_get_all_by_transaction_id(self.transaction_id())?;
                let remote_shard_group = address.to_shard_group(
                    local_committee_info.num_preshards(),
                    local_committee_info.num_committees(),
                );
                debug!(
                    target: LOG_TARGET,
                    "pledges: {}",
                    pledges.display(),
                );
                debug!(
                    target: LOG_TARGET,
                    "{} Transaction {} is missing a foreign {} pledge for {}:{} from {} ({} pledge(s) found)",
                    local_committee_info.shard_group(),
                    self.transaction_id(),
                    lock_type,
                    substate_id,
                    version,
                    remote_shard_group,
                    pledges.len(),
                );
            }

            return Ok(false);
        }
        Ok(true)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TransactionPoolError {
    #[error("Storage error: {0}")]
    StorageError(#[from] StorageError),
    #[error("Invalid transaction transition from {from:?} to {to:?} with is_ready={is_ready}")]
    InvalidTransactionTransition {
        from: TransactionPoolStage,
        to: TransactionPoolStage,
        is_ready: bool,
    },
    #[error("Transaction already executed: {transaction_id} in block {block_id}")]
    TransactionAlreadyExecuted {
        transaction_id: TransactionId,
        block_id: BlockId,
    },
}

impl IsNotFoundError for TransactionPoolError {
    fn is_not_found_error(&self) -> bool {
        match self {
            TransactionPoolError::StorageError(e) => e.is_not_found_error(),
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use rand::{rngs::OsRng, Rng};

    use super::*;
    use crate::consensus_models::LeaderFee;

    mod ordering {
        use super::*;

        #[test]
        fn it_is_ordered_correctly() {
            assert!(TransactionPoolStage::New < TransactionPoolStage::Prepared);
            assert!(TransactionPoolStage::Prepared < TransactionPoolStage::LocalPrepared);
            assert!(TransactionPoolStage::LocalPrepared < TransactionPoolStage::AllPrepared);
            assert!(TransactionPoolStage::LocalPrepared < TransactionPoolStage::SomePrepared);
            assert!(TransactionPoolStage::AllPrepared < TransactionPoolStage::LocalAccepted);
            assert!(TransactionPoolStage::SomePrepared < TransactionPoolStage::LocalAccepted);
            assert!(TransactionPoolStage::LocalAccepted < TransactionPoolStage::AllAccepted);
            assert!(TransactionPoolStage::LocalAccepted < TransactionPoolStage::SomeAccepted);
        }
    }

    mod calculate_leader_fee {
        use super::*;

        fn create_record_with_fee(fee: u64) -> TransactionPoolRecord {
            TransactionPoolRecord {
                transaction_id: TransactionId::new([0; 32]),
                original_decision: Decision::Commit,
                evidence: Default::default(),
                transaction_fee: fee,
                leader_fee: None,
                stage: TransactionPoolStage::New,
                is_global: false,
                pending_stage: None,
                local_decision: None,
                remote_decision: None,
                is_ready: false,
            }
        }

        fn check_calculate_leader_fee(
            total_tx_fee: u64,
            total_num_involved_shards: u64,
            exhaust_divisor: u64,
        ) -> LeaderFee {
            let tx = create_record_with_fee(total_tx_fee);
            let leader_fee = tx.calculate_leader_fee(total_num_involved_shards.try_into().unwrap(), exhaust_divisor);
            // Total payable fee + burn is always equal to the total block fee
            assert_eq!(
                leader_fee.fee * total_num_involved_shards + leader_fee.global_exhaust_burn,
                total_tx_fee,
                "Fees were created or lost in the calculation. Expected: {}, Actual: {}",
                total_tx_fee,
                leader_fee.fee * total_num_involved_shards + leader_fee.global_exhaust_burn
            );

            let deviation_from_target_burn =
                leader_fee.global_exhaust_burn as f32 - (total_tx_fee.checked_div(exhaust_divisor).unwrap_or(0) as f32);
            assert!(
                deviation_from_target_burn.abs() <= total_num_involved_shards as f32,
                "Deviation from target burn is too high: {} (target: {}, actual: {}, num_shards: {}, divisor: {})",
                deviation_from_target_burn,
                total_tx_fee.checked_div(exhaust_divisor).unwrap_or(0),
                leader_fee.global_exhaust_burn,
                total_num_involved_shards,
                exhaust_divisor
            );

            leader_fee
        }

        #[test]
        fn it_calculates_the_correct_leader_fee() {
            let fee = check_calculate_leader_fee(100, 1, 20);
            assert_eq!(fee.fee, 95);
            assert_eq!(fee.global_exhaust_burn, 5);

            let fee = check_calculate_leader_fee(100, 1, 10);
            assert_eq!(fee.fee, 90);
            assert_eq!(fee.global_exhaust_burn, 10);

            let fee = check_calculate_leader_fee(100, 2, 0);
            assert_eq!(fee.fee, 50);
            assert_eq!(fee.global_exhaust_burn, 0);

            let fee = check_calculate_leader_fee(100, 2, 10);
            assert_eq!(fee.fee, 45);
            assert_eq!(fee.global_exhaust_burn, 10);

            let fee = check_calculate_leader_fee(100, 3, 0);
            assert_eq!(fee.fee, 33);
            // Even with no exhaust, we still burn 1 due to integer div floor
            assert_eq!(fee.global_exhaust_burn, 1);

            let fee = check_calculate_leader_fee(100, 3, 10);
            assert_eq!(fee.fee, 30);
            assert_eq!(fee.global_exhaust_burn, 10);

            let fee = check_calculate_leader_fee(98, 3, 10);
            assert_eq!(fee.fee, 30);
            assert_eq!(fee.global_exhaust_burn, 8);

            let fee = check_calculate_leader_fee(98, 3, 21);
            assert_eq!(fee.fee, 32);
            // target burn is 4, but the remainder burn is 5, so we give 1 more to the leaders and burn 2
            assert_eq!(fee.global_exhaust_burn, 2);

            // Target burn is 8, and the remainder burn is 8, so we burn 8
            let fee = check_calculate_leader_fee(98, 10, 10);
            assert_eq!(fee.fee, 9);
            assert_eq!(fee.global_exhaust_burn, 8);

            let fee = check_calculate_leader_fee(19802, 45, 20);
            assert_eq!(fee.fee, 418);
            assert_eq!(fee.global_exhaust_burn, 992);

            // High burn amount due to not enough fees to pay out all involved shards to compensate
            let fee = check_calculate_leader_fee(311, 45, 20);
            assert_eq!(fee.fee, 6);
            assert_eq!(fee.global_exhaust_burn, 41);
        }

        #[test]
        fn simple_fuzz() {
            let mut total_fees = 0;
            let mut total_burnt = 0;
            for _ in 0..1_000_000 {
                let fee = OsRng.gen_range(100..100000u64);
                let involved = OsRng.gen_range(1..100u64);
                let fee = check_calculate_leader_fee(fee, involved, 20);
                total_fees += fee.fee * involved;
                total_burnt += fee.global_exhaust_burn;
            }

            println!(
                "total fees: {}, total burnt: {}, {}%",
                total_fees,
                total_burnt,
                // Should approach 5%, tends to be ~5.25%
                (total_burnt as f64 / total_fees as f64) * 100.0
            );
        }
    }
}
