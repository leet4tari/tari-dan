//   Copyright 2024 The Tari Project
//   SPDX-License-Identifier: BSD-3-Clause

use std::{fmt::Display, time::Duration};

use tari_dan_common_types::{NumPreshards, ShardGroup, ToSubstateAddress};
use tari_engine_types::commit_result::{ExecuteResult, RejectReason};
use tari_transaction::TransactionId;

use crate::{
    consensus_models::{AbortReason, BlockId, Decision, Evidence, VersionedSubstateIdLockIntent},
    StateStoreReadTransaction,
    StateStoreWriteTransaction,
    StorageError,
};

#[derive(Debug, Clone)]
pub struct TransactionExecution {
    pub transaction_id: TransactionId,
    pub result: ExecuteResult,
    pub abort_reason: Option<RejectReason>,
    pub resolved_inputs: Vec<VersionedSubstateIdLockIntent>,
    pub resulting_outputs: Vec<VersionedSubstateIdLockIntent>,
}

impl TransactionExecution {
    pub fn new(
        transaction_id: TransactionId,
        result: ExecuteResult,
        resolved_inputs: Vec<VersionedSubstateIdLockIntent>,
        resulting_outputs: Vec<VersionedSubstateIdLockIntent>,
        abort_reason: Option<RejectReason>,
    ) -> Self {
        Self {
            transaction_id,
            result,
            resolved_inputs,
            resulting_outputs,
            abort_reason,
        }
    }

    pub fn id(&self) -> &TransactionId {
        &self.transaction_id
    }

    pub fn result(&self) -> &ExecuteResult {
        &self.result
    }

    pub fn decision(&self) -> Decision {
        if let Some(reject_reason) = &self.abort_reason {
            return Decision::Abort(AbortReason::from(reject_reason));
        }
        Decision::from(&self.result.finalize.result)
    }

    pub fn transaction_fee(&self) -> u64 {
        if self.decision().is_abort() {
            return 0;
        }

        self.result
            .finalize
            .fee_receipt
            .total_fees_paid()
            .as_u64_checked()
            .expect("invariant: engine calculated negative fee")
    }

    pub fn resolved_inputs(&self) -> &[VersionedSubstateIdLockIntent] {
        &self.resolved_inputs
    }

    pub fn resulting_outputs(&self) -> &[VersionedSubstateIdLockIntent] {
        &self.resulting_outputs
    }

    pub fn abort_reason(&self) -> Option<&RejectReason> {
        self.abort_reason.as_ref()
    }

    pub fn set_abort_reason(&mut self, abort_reason: RejectReason) -> &mut Self {
        self.abort_reason = Some(abort_reason);
        self
    }

    pub fn to_evidence(&self, num_preshards: NumPreshards, num_committees: u32) -> Evidence {
        let mut evidence = Evidence::from_inputs_and_outputs(
            num_preshards,
            num_committees,
            self.resolved_inputs(),
            self.resulting_outputs(),
        );
        if self.decision().is_abort() {
            evidence.abort();
        }
        evidence
    }

    pub fn is_involved(&self, num_preshards: NumPreshards, num_committees: u32, shard_group: ShardGroup) -> bool {
        self.resolved_inputs()
            .iter()
            .chain(self.resulting_outputs())
            .any(|obj| obj.to_substate_address().to_shard_group(num_preshards, num_committees) == shard_group)
    }

    pub fn for_block(self, block_id: BlockId) -> BlockTransactionExecution {
        BlockTransactionExecution {
            block_id,
            execution: self,
        }
    }
}

#[derive(Debug, Clone)]
pub struct BlockTransactionExecution {
    pub block_id: BlockId,
    pub execution: TransactionExecution,
}

impl BlockTransactionExecution {
    pub fn new(
        block_id: BlockId,
        transaction_id: TransactionId,
        result: ExecuteResult,
        resolved_inputs: Vec<VersionedSubstateIdLockIntent>,
        resulting_outputs: Vec<VersionedSubstateIdLockIntent>,
        abort_reason: Option<RejectReason>,
    ) -> Self {
        Self {
            block_id,
            execution: TransactionExecution::new(
                transaction_id,
                result,
                resolved_inputs,
                resulting_outputs,
                abort_reason,
            ),
        }
    }

    pub fn transaction_execution(&self) -> &TransactionExecution {
        &self.execution
    }

    pub fn into_transaction_execution(self) -> TransactionExecution {
        self.execution
    }

    pub fn block_id(&self) -> &BlockId {
        &self.block_id
    }

    pub fn decision(&self) -> Decision {
        self.execution.decision()
    }

    pub fn transaction_id(&self) -> &TransactionId {
        &self.execution.transaction_id
    }

    pub fn result(&self) -> &ExecuteResult {
        self.execution.result()
    }

    pub fn resolved_inputs(&self) -> &[VersionedSubstateIdLockIntent] {
        &self.execution.resolved_inputs
    }

    pub fn resulting_outputs(&self) -> &[VersionedSubstateIdLockIntent] {
        &self.execution.resulting_outputs
    }

    pub fn execution_time(&self) -> Duration {
        self.execution.result.execution_time
    }

    pub fn abort_reason(&self) -> Option<&RejectReason> {
        self.execution.abort_reason()
    }

    pub fn transaction_fee(&self) -> u64 {
        self.execution.transaction_fee()
    }
}

impl BlockTransactionExecution {
    pub fn insert_if_required<TTx: StateStoreWriteTransaction>(&self, tx: &mut TTx) -> Result<bool, StorageError> {
        tx.transaction_executions_insert_or_ignore(self)
    }

    /// Fetches any pending execution that happened before the given block until the commit block (parent of locked
    /// block)
    pub fn get_pending_for_block<TTx: StateStoreReadTransaction>(
        tx: &TTx,
        transaction_id: &TransactionId,
        from_block_id: &BlockId,
    ) -> Result<Self, StorageError> {
        tx.transaction_executions_get_pending_for_block(transaction_id, from_block_id)
    }

    /// Fetches any pending execution that happened in the given block
    pub fn get_by_block<TTx: StateStoreReadTransaction>(
        tx: &TTx,
        transaction_id: &TransactionId,
        block_id: &BlockId,
    ) -> Result<Self, StorageError> {
        tx.transaction_executions_get(transaction_id, block_id)
    }
}

impl Display for BlockTransactionExecution {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "BlockTransactionExecution(block_id: {}, transaction_id: {}, decision: {}, execution_time: {:.2?})",
            self.block_id,
            self.execution.transaction_id,
            self.decision(),
            self.execution_time()
        )
    }
}
