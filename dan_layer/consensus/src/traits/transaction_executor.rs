//   Copyright 2024 The Tari Project
//   SPDX-License-Identifier: BSD-3-Clause

use std::collections::HashMap;

use tari_dan_common_types::{optional::IsNotFoundError, Epoch, SubstateRequirement};
use tari_dan_storage::{
    consensus_models::{ExecutedTransaction, TransactionPoolError},
    StateStore,
    StorageError,
};
use tari_engine_types::substate::Substate;
use tari_transaction::Transaction;

use crate::hotstuff::substate_store::{LockFailedError, SubstateStoreError};

#[derive(thiserror::Error, Debug)]
pub enum BlockTransactionExecutorError {
    #[error("Execution thread failure: {0}")]
    ExecutionThreadFailure(String),
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error("State store error: {0}")]
    StateStoreError(String),
    #[error("Substate store error: {0}")]
    SubstateStoreError(#[from] SubstateStoreError),
    #[error("Transaction validation error: {0}")]
    TransactionValidationError(String),
    #[error("Transaction pool error: {0}")]
    TransactionPoolError(#[from] TransactionPoolError),
    #[error("BUG: Invariant error: {0}")]
    InvariantError(String),
}
impl BlockTransactionExecutorError {
    pub fn is_substate_down_error(&self) -> bool {
        matches!(
            self,
            BlockTransactionExecutorError::SubstateStoreError(SubstateStoreError::SubstateIsDown { .. }) |
                BlockTransactionExecutorError::SubstateStoreError(SubstateStoreError::LockFailed(
                    LockFailedError::SubstateIsDown { .. }
                ))
        )
    }
}

impl IsNotFoundError for BlockTransactionExecutorError {
    fn is_not_found_error(&self) -> bool {
        match self {
            BlockTransactionExecutorError::StorageError(err) => err.is_not_found_error(),
            BlockTransactionExecutorError::SubstateStoreError(err) => err.is_not_found_error(),
            _ => false,
        }
    }
}

pub trait BlockTransactionExecutor<TStateStore: StateStore> {
    fn validate(
        &self,
        tx: &TStateStore::ReadTransaction<'_>,
        current_epoch: Epoch,
        transaction: &Transaction,
    ) -> Result<(), BlockTransactionExecutorError>;

    fn execute(
        &self,
        transaction: Transaction,
        current_epoch: Epoch,
        resolved_inputs: &HashMap<SubstateRequirement, Substate>,
    ) -> Result<ExecutedTransaction, BlockTransactionExecutorError>;
}
