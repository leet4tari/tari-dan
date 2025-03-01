//  Copyright 2022. The Tari Project
//
//  Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
//  following conditions are met:
//
//  1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following
//  disclaimer.
//
//  2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the
//  following disclaimer in the documentation and/or other materials provided with the distribution.
//
//  3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote
//  products derived from this software without specific prior written permission.
//
//  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
//  INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
//  DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
//  SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
//  SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
//  WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
//  USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

use log::info;
use tari_dan_app_utilities::{
    substate_file_cache::SubstateFileCache,
    transaction_executor::{TariDanTransactionProcessor, TransactionExecutor, TransactionProcessorError},
};
use tari_dan_common_types::PeerAddress;
use tari_dan_engine::state_store::{new_memory_store, StateStoreError};
use tari_dan_storage::StorageError;
use tari_engine_types::{
    commit_result::ExecuteResult,
    virtual_substate::{VirtualSubstate, VirtualSubstateId, VirtualSubstates},
};
use tari_epoch_manager::{base_layer::EpochManagerHandle, EpochManagerError, EpochManagerReader};
use tari_rpc_framework::RpcStatus;
use tari_state_store_sqlite::SqliteStateStore;
use tari_template_manager::implementation::TemplateManager;
use tari_transaction::Transaction;
use tari_validator_node_client::ValidatorNodeClientError;
use tari_validator_node_rpc::client::TariValidatorNodeRpcClientFactory;
use thiserror::Error;
use tokio::task;

use crate::{
    p2p::services::mempool::{ResolvedSubstates, SubstateResolver},
    substate_resolver::{SubstateResolverError, TariSubstateResolver},
};

const LOG_TARGET: &str = "tari::dan::validator_node::dry_run_transaction_processor";

#[derive(Error, Debug)]
pub enum DryRunTransactionProcessorError {
    #[error("PayloadProcessor error: {0}")]
    PayloadProcessor(#[from] TransactionProcessorError),
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("EpochManager error: {0}")]
    EpochManager(#[from] EpochManagerError),
    #[error("Validator node client error: {0}")]
    ValidatorNodeClient(#[from] ValidatorNodeClientError),
    #[error("Rpc error: {0}")]
    RpcRequestFailed(#[from] RpcStatus),
    #[error("State store error: {0}")]
    StateStoreError(#[from] StateStoreError),
    #[error("Substate resolver error: {0}")]
    SubstateResoverError(#[from] SubstateResolverError),
    #[error("Execution thread failed: {0}")]
    ExecutionThreadFailed(#[from] task::JoinError),
}

#[derive(Clone, Debug)]
pub struct DryRunTransactionProcessor {
    substate_resolver: TariSubstateResolver<
        SqliteStateStore<PeerAddress>,
        EpochManagerHandle<PeerAddress>,
        TariValidatorNodeRpcClientFactory,
        SubstateFileCache,
    >,
    epoch_manager: EpochManagerHandle<PeerAddress>,
    payload_processor: TariDanTransactionProcessor<TemplateManager<PeerAddress>>,
}

impl DryRunTransactionProcessor {
    pub fn new(
        epoch_manager: EpochManagerHandle<PeerAddress>,
        payload_processor: TariDanTransactionProcessor<TemplateManager<PeerAddress>>,
        substate_resolver: TariSubstateResolver<
            SqliteStateStore<PeerAddress>,
            EpochManagerHandle<PeerAddress>,
            TariValidatorNodeRpcClientFactory,
            SubstateFileCache,
        >,
    ) -> Self {
        Self {
            substate_resolver,
            epoch_manager,
            payload_processor,
        }
    }

    pub async fn process_transaction(
        &self,
        transaction: Transaction,
    ) -> Result<ExecuteResult, DryRunTransactionProcessorError> {
        // Resolve all local and foreign substates
        let mut temp_state_store = new_memory_store();

        // TODO: the current epoch should come from consensus
        let current_epoch = self.epoch_manager.current_epoch().await?;
        let mut virtual_substates = VirtualSubstates::new();
        virtual_substates.insert(
            VirtualSubstateId::CurrentEpoch,
            VirtualSubstate::CurrentEpoch(current_epoch.as_u64()),
        );

        let ResolvedSubstates {
            local: inputs,
            unresolved_foreign: foreign,
        } = self.substate_resolver.try_resolve_local(&transaction)?;
        temp_state_store.set_many(inputs)?;
        // Dry-run we can request the foreign inputs from validator nodes. The execution result may vary if inputs are
        // mutated between the dry-run and live execution.
        let foreign_inputs = self.substate_resolver.try_resolve_foreign(&foreign).await?;
        temp_state_store.set_many(foreign_inputs)?;

        // execute the payload in the WASM engine and return the result
        let processor = self.payload_processor.clone();
        let exec_output = task::spawn_blocking(move || {
            processor.execute(transaction, temp_state_store.into_read_only(), virtual_substates)
        })
        .await??;
        let result = exec_output.result;

        let fees = &result.finalize.fee_receipt;
        info!(target: LOG_TARGET, "Transaction fees: {}", fees.total_fees_charged());

        Ok(result)
    }
}
