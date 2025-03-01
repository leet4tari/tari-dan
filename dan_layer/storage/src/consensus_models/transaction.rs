//   Copyright 2023 The Tari Project
//   SPDX-License-Identifier: BSD-3-Clause

use std::{
    collections::{HashMap, HashSet},
    ops::Deref,
    time::Duration,
};

use log::*;
use serde::Deserialize;
use tari_dan_common_types::{
    committee::CommitteeInfo,
    displayable::Displayable,
    NumPreshards,
    SubstateLockType,
    ToSubstateAddress,
    VersionedSubstateId,
};
use tari_engine_types::{
    commit_result::{ExecuteResult, FinalizeResult, RejectReason},
    transaction_receipt::TransactionReceiptAddress,
};
use tari_transaction::{Transaction, TransactionId};

use crate::{
    consensus_models::{
        AbortReason,
        BlockId,
        Decision,
        Evidence,
        ExecutedTransaction,
        LockedSubstateValue,
        SubstatePledge,
        SubstatePledges,
        TransactionExecution,
        TransactionPoolRecord,
        VersionedSubstateIdLockIntent,
    },
    Ordering,
    StateStoreReadTransaction,
    StateStoreWriteTransaction,
    StorageError,
};

const LOG_TARGET: &str = "tari::dan::storage::consensus_models::transaction";

#[derive(Debug, Clone, Deserialize)]
pub struct TransactionRecord {
    pub transaction: Transaction,
    pub execution_result: Option<ExecuteResult>,
    pub resulting_outputs: Option<Vec<VersionedSubstateIdLockIntent>>,
    pub resolved_inputs: Option<Vec<VersionedSubstateIdLockIntent>>,
    pub final_decision: Option<Decision>,
    pub finalized_time: Option<Duration>,
    pub abort_reason: Option<RejectReason>,
}

impl TransactionRecord {
    pub fn new(transaction: Transaction) -> Self {
        Self {
            transaction,
            execution_result: None,
            resolved_inputs: None,
            final_decision: None,
            finalized_time: None,
            resulting_outputs: None,
            abort_reason: None,
        }
    }

    pub fn load(
        transaction: Transaction,
        result: Option<ExecuteResult>,
        resolved_inputs: Option<Vec<VersionedSubstateIdLockIntent>>,
        final_decision: Option<Decision>,
        finalized_time: Option<Duration>,
        resulting_outputs: Option<Vec<VersionedSubstateIdLockIntent>>,
        abort_reason: Option<RejectReason>,
    ) -> Self {
        Self {
            transaction,
            resolved_inputs,
            execution_result: result,
            final_decision,
            finalized_time,
            resulting_outputs,
            abort_reason,
        }
    }

    pub fn id(&self) -> &TransactionId {
        self.transaction.id()
    }

    pub fn transaction(&self) -> &Transaction {
        &self.transaction
    }

    pub fn transaction_mut(&mut self) -> &mut Transaction {
        &mut self.transaction
    }

    pub fn into_transaction(self) -> Transaction {
        self.transaction
    }

    pub fn execution_result(&self) -> Option<&ExecuteResult> {
        self.execution_result.as_ref()
    }

    pub fn has_executed(&self) -> bool {
        self.execution_result.is_some()
    }

    pub fn resulting_outputs(&self) -> Option<&[VersionedSubstateIdLockIntent]> {
        self.resulting_outputs.as_deref()
    }

    pub fn resolved_inputs(&self) -> Option<&[VersionedSubstateIdLockIntent]> {
        self.resolved_inputs.as_deref()
    }

    pub fn execution_decision(&self) -> Option<Decision> {
        self.execution_result().map(|r| Decision::from(&r.finalize.result))
    }

    pub fn transaction_fee(&self) -> Option<u64> {
        self.execution_result
            .as_ref()
            .map(|r| r.finalize.fee_receipt.total_fees_paid().as_u64_checked().unwrap())
    }

    pub fn current_decision(&self) -> Decision {
        self.final_decision
            .or_else(|| self.abort_reason.as_ref().map(|reason| Decision::Abort(AbortReason::from(reason))))
            .or_else(|| self.execution_decision())
            // We will choose to commit a transaction unless (1) we aborted it, (2) the execution has failed
            .unwrap_or(Decision::Commit)
    }

    pub fn final_decision(&self) -> Option<Decision> {
        self.final_decision
    }

    pub fn execution_time(&self) -> Option<Duration> {
        self.execution_result.as_ref().map(|r| r.execution_time)
    }

    pub fn finalized_time(&self) -> Option<Duration> {
        self.finalized_time
    }

    pub fn is_finalized(&self) -> bool {
        self.final_decision.is_some()
    }

    pub fn is_executed(&self) -> bool {
        self.execution_result.is_some()
    }

    pub fn abort_reason(&self) -> Option<&RejectReason> {
        self.abort_reason.as_ref()
    }

    pub fn abort(&mut self, reason: RejectReason) -> &mut Self {
        self.abort_reason = Some(reason);
        let receipt = self.id().into_receipt_address();
        let id = VersionedSubstateId::for_tx_receipt(receipt);
        self.resulting_outputs = Some(vec![VersionedSubstateIdLockIntent::new(
            id,
            SubstateLockType::Output,
            true,
        )]);
        self
    }

    pub fn is_involved_in_inputs(&self, local_committee_info: &CommitteeInfo) -> bool {
        self.transaction
            .all_inputs_iter()
            .any(|i| local_committee_info.includes_substate_id(i.substate_id()))
    }

    pub fn to_receipt_id(&self) -> TransactionReceiptAddress {
        (*self.id()).into()
    }

    pub fn into_execution(mut self) -> Option<TransactionExecution> {
        self.take_execution()
    }

    fn take_execution(&mut self) -> Option<TransactionExecution> {
        // TODO: This is hacky. We're using this as a way to finalize the transaction which always expects some
        // execution result.
        let transaction_id = *self.transaction.id();
        let resolved_inputs = self.resolved_inputs.take().unwrap_or_else(|| {
            self.transaction
                .all_inputs_iter()
                .map(|i| VersionedSubstateIdLockIntent::from_requirement(i.to_owned(), SubstateLockType::Write))
                .collect()
        });
        let resulting_outputs = self.resulting_outputs.take().unwrap_or_default();
        let result = if let Some(ref reason) = self.abort_reason {
            // Only use rejected results for the transaction. If execution ACCEPTed but the final decision is ABORT,
            // then use abort_details (which should have been set in this case).
            let exec_result = self.execution_result.as_ref().filter(|r| r.finalize.result.is_reject());
            let execution_time = exec_result.as_ref().map(|r| r.execution_time).unwrap_or_default();
            ExecuteResult {
                finalize: exec_result.map(|r| r.finalize.clone()).unwrap_or_else(|| {
                    FinalizeResult::new_rejected(self.transaction.id().into_array().into(), reason.clone())
                }),
                execution_time,
            }
        } else {
            // If there's no abort reason or execution result, return None here
            self.execution_result.take()?
        };

        Some(TransactionExecution {
            transaction_id,
            result,
            abort_reason: self.abort_reason.take(),
            resolved_inputs,
            resulting_outputs,
        })
    }

    pub fn into_transaction_and_execution(mut self) -> (Transaction, Option<TransactionExecution>) {
        let maybe_execution = self.take_execution();
        (self.transaction, maybe_execution)
    }

    pub fn into_final_result(self) -> Option<ExecuteResult> {
        // TODO: This is hacky, result should be broken up into execution result, validation (mempool) result, finality
        //       result. These results are independent of each other.
        self.final_decision().and_then(|d| {
            if d.is_commit() {
                // Is is expected that the result is ACCEPT.
                // TODO: Handle (elsewhere) the edge-case where our execution failed but the committee decided to COMMIT
                // (fetch the state transitions from a peer?)
                self.execution_result
            } else {
                // Only use rejected results for the transaction. If execution ACCEPTed but the final decision is ABORT,
                // then use abort_details (which should have been set in this case).
                let exec_result = self.execution_result.filter(|r| r.finalize.result.is_reject());
                let execution_time = exec_result.as_ref().map(|r| r.execution_time).unwrap_or_default();
                Some(ExecuteResult {
                    finalize: exec_result.map(|r| r.finalize).unwrap_or_else(|| {
                        FinalizeResult::new_rejected(
                            self.transaction.id().into_array().into(),
                            // TODO: RejectReason::Unknown should never occur.
                            self.abort_reason.unwrap_or(RejectReason::Unknown),
                        )
                    }),
                    execution_time,
                })
            }
        })
    }

    pub fn to_initial_evidence(&self, num_preshards: NumPreshards, num_committees: u32) -> Evidence {
        let inputs = self.transaction.all_inputs_iter();
        let receipt = self.transaction.id().into_receipt_address();
        Evidence::from_initial_substates(num_preshards, num_committees, inputs, [VersionedSubstateId::new(
            receipt, 0,
        )])
    }
}

impl TransactionRecord {
    pub fn insert<TTx: StateStoreWriteTransaction>(&self, tx: &mut TTx) -> Result<(), StorageError> {
        tx.transactions_insert(self)
    }

    pub fn save<TTx>(&self, tx: &mut TTx) -> Result<(), StorageError>
    where
        TTx: StateStoreWriteTransaction + Deref,
        TTx::Target: StateStoreReadTransaction,
    {
        if !Self::exists(&**tx, self.transaction.id())? {
            self.insert(tx)?;
        }
        Ok(())
    }

    pub fn save_all<'a, TTx, I>(tx: &mut TTx, transactions: I) -> Result<(), StorageError>
    where
        TTx: StateStoreWriteTransaction + Deref,
        TTx::Target: StateStoreReadTransaction,
        I: IntoIterator<Item = &'a TransactionRecord>,
    {
        tx.transactions_save_all(transactions)
    }

    pub fn update<TTx: StateStoreWriteTransaction>(&self, tx: &mut TTx) -> Result<(), StorageError> {
        tx.transactions_update(self)
    }

    pub fn upsert<TTx>(&self, tx: &mut TTx) -> Result<(), StorageError>
    where
        TTx: StateStoreWriteTransaction + Deref,
        TTx::Target: StateStoreReadTransaction,
    {
        if TransactionRecord::exists(&**tx, self.id())? {
            self.update(tx)
        } else {
            self.insert(tx)
        }
    }

    pub fn get<TTx: StateStoreReadTransaction>(tx: &TTx, tx_id: &TransactionId) -> Result<Self, StorageError> {
        tx.transactions_get(tx_id)
    }

    pub fn exists<TTx: StateStoreReadTransaction>(tx: &TTx, tx_id: &TransactionId) -> Result<bool, StorageError> {
        tx.transactions_exists(tx_id)
    }

    pub fn exists_any<'a, TTx: StateStoreReadTransaction, I: IntoIterator<Item = &'a TransactionId>>(
        tx: &TTx,
        tx_ids: I,
    ) -> Result<bool, StorageError> {
        for tx_id in tx_ids {
            if tx.transactions_exists(tx_id)? {
                return Ok(true);
            }
        }

        Ok(false)
    }

    pub fn get_any<'a, TTx: StateStoreReadTransaction, I: IntoIterator<Item = &'a TransactionId>>(
        tx: &TTx,
        tx_ids: I,
    ) -> Result<(Vec<Self>, HashSet<TransactionId>), StorageError> {
        let mut tx_ids = tx_ids.into_iter().copied().collect::<HashSet<_>>();
        if tx_ids.is_empty() {
            return Ok((vec![], tx_ids));
        }
        let recs = tx.transactions_get_any(tx_ids.iter())?;
        for rec in &recs {
            tx_ids.remove(rec.transaction.id());
        }

        Ok((recs, tx_ids))
    }

    pub fn get_any_or_build<TTx: StateStoreReadTransaction, I: IntoIterator<Item = Transaction> + Clone>(
        tx: &TTx,
        transactions: I,
    ) -> Result<Vec<Self>, StorageError> {
        let mut tx_ids = transactions
            .clone()
            .into_iter()
            .map(|t| (*t.id(), t))
            .collect::<HashMap<_, _>>();
        let mut recs = tx.transactions_get_any(tx_ids.keys())?;
        for rec in &recs {
            tx_ids.remove(rec.transaction.id());
        }
        recs.extend(tx_ids.into_values().map(Self::new));

        Ok(recs)
    }

    pub fn get_missing<'a, TTx: StateStoreReadTransaction, I: IntoIterator<Item = &'a TransactionId>>(
        tx: &TTx,
        tx_ids: I,
    ) -> Result<HashSet<TransactionId>, StorageError> {
        // TODO(perf): optimise
        let (_, missing) = Self::get_any(tx, tx_ids)?;
        Ok(missing)
    }

    pub fn get_paginated<TTx: StateStoreReadTransaction>(
        tx: &TTx,
        limit: u64,
        offset: u64,
        ordering: Option<Ordering>,
    ) -> Result<Vec<Self>, StorageError> {
        tx.transactions_get_paginated(limit, offset, ordering)
    }

    pub fn get_local_pledges<TTx: StateStoreReadTransaction>(&self, tx: &TTx) -> Result<SubstatePledges, StorageError> {
        let locked_values = LockedSubstateValue::get_all_for_transaction(tx, self.id())?;
        locked_values
            .into_iter()
            .filter(|lock| !lock.lock.is_output())
            .map(|mut lock| {
                let maybe_value = lock.take_value();
                let lock_intent = lock.to_substate_lock_intent();
                SubstatePledge::try_create(lock_intent, maybe_value).ok_or_else(|| StorageError::DataInconsistency {
                    details: format!("Invalid substate lock: {} ({})", lock.substate_id, lock.lock),
                })
            })
            .collect()
    }

    pub fn get_foreign_pledges<TTx: StateStoreReadTransaction>(
        &self,
        tx: &TTx,
    ) -> Result<SubstatePledges, StorageError> {
        tx.foreign_substate_pledges_get_all_by_transaction_id(self.id())
    }

    pub fn finalize_all<'a, TTx, I>(tx: &mut TTx, block_id: BlockId, transactions: I) -> Result<(), StorageError>
    where
        TTx: StateStoreWriteTransaction + Deref,
        TTx::Target: StateStoreReadTransaction,
        I: IntoIterator<Item = &'a TransactionPoolRecord>,
    {
        tx.transactions_finalize_all(block_id, transactions)
    }

    pub fn has_all_required_input_pledges<TTx: StateStoreReadTransaction>(
        &self,
        tx: &TTx,
        local_committee_info: &CommitteeInfo,
    ) -> Result<bool, StorageError> {
        let inputs = self
            .transaction()
            .all_inputs_iter()
            .map(|req| (local_committee_info.includes_substate_id(req.substate_id()), req));
        let locks = LockedSubstateValue::get_all_for_transaction(tx, self.id())?;
        let pledges = tx.foreign_substate_pledges_get_all_by_transaction_id(self.id())?;
        for (is_local, input) in inputs {
            if is_local {
                if locks.iter().all(|i| !i.satisfies_requirements(input)) {
                    debug!(
                        target: LOG_TARGET,
                        "Locks: {}",
                        locks.display(),
                    );
                    debug!(
                        target: LOG_TARGET,
                        "{} Transaction {} is missing a local lock for input {} ({} lock(s) found)",
                        local_committee_info.shard_group(),
                        self.id(),
                        input.substate_id(),
                        locks.len(),
                    );
                    return Ok(false);
                }
            } else if pledges.iter().all(|p| !p.satisfies_requirement(input)) {
                let remote_shard_group = input.or_zero_version().to_substate_address().to_shard_group(
                    local_committee_info.num_preshards(),
                    local_committee_info.num_committees(),
                );
                debug!(
                    target: LOG_TARGET,
                    "Pledges: {}",
                    pledges.display(),
                );
                debug!(
                    target: LOG_TARGET,
                    "{} Transaction {} is missing a pledge for input {} from {} ({} pledge(s) found)",
                    local_committee_info.shard_group(),
                    self.id(),
                    input.substate_id(),
                    remote_shard_group,
                    pledges.len(),
                );
                return Ok(false);
            } else {
                // We have a lock/pledge for the input, continue
            }
        }
        Ok(true)
    }

    pub fn has_all_foreign_input_pledges<TTx: StateStoreReadTransaction>(
        &self,
        tx: &TTx,
        local_committee_info: &CommitteeInfo,
    ) -> Result<bool, StorageError> {
        let mut foreign_inputs = self
            .transaction()
            .all_inputs_iter()
            .filter(|i| !local_committee_info.includes_substate_id(i.substate_id()))
            .peekable();

        if foreign_inputs.peek().is_none() {
            // Avoid query for pledges for no reason
            return Ok(true);
        }

        // TODO(perf): this could be a bespoke DB query
        let pledges = tx.foreign_substate_pledges_get_all_by_transaction_id(self.id())?;
        for input in foreign_inputs {
            if pledges.iter().all(|p| !p.satisfies_requirement(input)) {
                debug!(
                    target: LOG_TARGET,
                    "Transaction {} is missing a pledge for input {} ({} pledge(s) found)",
                    self.id(),
                    input.substate_id(),
                    pledges.len(),
                );
                return Ok(false);
            }
        }
        Ok(true)
    }
}

impl From<ExecutedTransaction> for TransactionRecord {
    fn from(tx: ExecutedTransaction) -> Self {
        let final_decision = tx.final_decision();
        let finalized_time = tx.finalized_time();
        let abort_details = tx.abort_reason().cloned();
        let (transaction, result, resolved_inputs, resulting_outputs) = tx.dissolve();

        Self {
            transaction,
            execution_result: Some(result),
            resolved_inputs: Some(resolved_inputs),
            final_decision,
            finalized_time,
            resulting_outputs: Some(resulting_outputs),
            abort_reason: abort_details,
        }
    }
}
