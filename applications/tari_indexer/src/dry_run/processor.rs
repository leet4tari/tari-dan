//  Copyright 2023. The Tari Project
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

use std::{collections::HashMap, sync::Arc};

use log::{debug, info};
use tari_dan_app_utilities::transaction_executor::{TariDanTransactionProcessor, TransactionExecutor as _};
use tari_dan_common_types::{Epoch, PeerAddress, SubstateRequirement};
use tari_dan_engine::{fees::FeeTable, state_store::new_memory_store, transaction::TransactionProcessorConfig};
use tari_engine_types::{
    commit_result::ExecuteResult,
    substate::{Substate, SubstateId},
    virtual_substate::{VirtualSubstate, VirtualSubstateId, VirtualSubstates},
};
use tari_epoch_manager::{base_layer::EpochManagerHandle, EpochManagerReader};
use tari_indexer_lib::{
    substate_cache::SubstateCache,
    substate_scanner::SubstateScanner,
    transaction_autofiller::TransactionAutofiller,
};
use tari_template_manager::implementation::TemplateManager;
use tari_transaction::Transaction;
use tari_validator_node_rpc::client::{
    SubstateResult,
    TariValidatorNodeRpcClientFactory,
    ValidatorNodeClientFactory,
    ValidatorNodeRpcClient,
};
use tokio::task;

use crate::dry_run::error::DryRunTransactionProcessorError;

const LOG_TARGET: &str = "tari::indexer::dry_run_transaction_processor";

pub struct DryRunTransactionProcessor<TSubstateCache> {
    config: TransactionProcessorConfig,
    epoch_manager: EpochManagerHandle<PeerAddress>,
    client_provider: TariValidatorNodeRpcClientFactory,
    transaction_autofiller:
        TransactionAutofiller<EpochManagerHandle<PeerAddress>, TariValidatorNodeRpcClientFactory, TSubstateCache>,
    template_manager: TemplateManager<PeerAddress>,
}

impl<TSubstateCache> DryRunTransactionProcessor<TSubstateCache>
where TSubstateCache: SubstateCache + 'static
{
    pub fn new(
        config: TransactionProcessorConfig,
        epoch_manager: EpochManagerHandle<PeerAddress>,
        client_provider: TariValidatorNodeRpcClientFactory,
        substate_scanner: Arc<
            SubstateScanner<EpochManagerHandle<PeerAddress>, TariValidatorNodeRpcClientFactory, TSubstateCache>,
        >,
        template_manager: TemplateManager<PeerAddress>,
    ) -> Self {
        let transaction_autofiller = TransactionAutofiller::new(substate_scanner);

        Self {
            config,
            epoch_manager,
            client_provider,
            transaction_autofiller,
            template_manager,
        }
    }

    pub async fn process_transaction(
        &self,
        transaction: Transaction,
        substate_requirements: Vec<SubstateRequirement>,
    ) -> Result<ExecuteResult, DryRunTransactionProcessorError> {
        info!(target: LOG_TARGET, "process_transaction: {}", transaction.hash());

        // automatically scan the inputs and add all related involved objects
        // note that this operation does not alter the transaction hash
        let (transaction, mut found_substates) = self
            .transaction_autofiller
            .autofill_transaction(transaction, substate_requirements)
            .await?;

        let epoch = self.epoch_manager.current_epoch().await?;
        found_substates.extend(self.fetch_input_substates(&transaction, epoch).await?);

        let payload_processor = self.build_payload_processor(&transaction);

        let virtual_substates = self.get_virtual_substates(&transaction, epoch).await?;

        let mut state_store = new_memory_store();
        state_store.set_many(found_substates)?;

        // execute the payload in the WASM engine and return the result
        let exec_output = task::block_in_place(|| {
            payload_processor.execute(transaction, state_store.into_read_only(), virtual_substates)
        })?;

        Ok(exec_output.result)
    }

    fn build_payload_processor(
        &self,
        transaction: &Transaction,
    ) -> TariDanTransactionProcessor<TemplateManager<PeerAddress>> {
        // simulate fees if the transaction requires it
        let fee_table = if Self::transaction_includes_fees(transaction) {
            // TODO: should match the VN fee table, should the fee table values be a consensus constant?
            FeeTable {
                per_transaction_weight_cost: 1,
                per_module_call_cost: 1,
                per_byte_storage_cost: 1,
                per_event_cost: 1,
                per_log_cost: 1,
            }
        } else {
            FeeTable::zero_rated()
        };

        TariDanTransactionProcessor::new(self.config.clone(), self.template_manager.clone(), fee_table)
    }

    fn transaction_includes_fees(transaction: &Transaction) -> bool {
        !transaction.fee_instructions().is_empty()
    }

    async fn fetch_input_substates(
        &self,
        transaction: &Transaction,
        epoch: Epoch,
    ) -> Result<HashMap<SubstateId, Substate>, DryRunTransactionProcessorError> {
        let mut substates = HashMap::new();

        // Fetch explicit inputs that may not have been resolved by the autofiller
        for requirement in transaction.inputs() {
            // If the input has been filled, we've already fetched the substate
            // Note: this works because VersionedSubstateId hashes the same as SubstateId internally.
            if transaction.filled_inputs().contains(&requirement.substate_id) {
                continue;
            }

            let (id, substate) = self.fetch_substate(requirement, epoch).await?;
            substates.insert(id, substate);
        }

        Ok(substates)
    }

    pub async fn fetch_substate(
        &self,
        substate_requirement: &SubstateRequirement,
        epoch: Epoch,
    ) -> Result<(SubstateId, Substate), DryRunTransactionProcessorError> {
        let address = substate_requirement.to_substate_address_zero_version();
        let mut committee = self.epoch_manager.get_committee_for_substate(epoch, address).await?;
        committee.shuffle();

        let max_failures = committee.max_failures() + 1;

        let mut nexist_count = 0;
        let mut err_count = 0;

        for vn_addr in committee.addresses() {
            // build a client with the VN
            let mut client = self.client_provider.create_client(vn_addr);

            match client.get_substate(substate_requirement).await {
                Ok(SubstateResult::Up { substate, id, .. }) => {
                    return Ok((id, substate));
                },
                Ok(SubstateResult::Down { id, version, .. }) => {
                    // TODO: we should seek proof of this.
                    return Err(DryRunTransactionProcessorError::SubstateDowned { id, version });
                },
                Ok(SubstateResult::DoesNotExist) => {
                    debug!(
                        target: LOG_TARGET,
                        "Substate {} does not exist on validator node {}",
                        substate_requirement,
                        vn_addr
                    );
                    // we do not stop when an individual claims DoesNotExist, we try $f + 1$ Vns
                    nexist_count += 1;
                    if nexist_count >= max_failures {
                        break;
                    }
                    continue;
                },
                Err(e) => {
                    info!(target: LOG_TARGET, "Unable to get substate from peer: {} ", e.to_string());
                    // we do not stop when an individual request errors, we try all Vns
                    err_count += 1;
                    continue;
                },
            };
        }

        // The substate does not exist on any VN or all validator nodes are offline, we return an error
        Err(DryRunTransactionProcessorError::AllValidatorsFailedToReturnSubstate {
            substate_requirement: substate_requirement.clone(),
            epoch,
            nexist_count,
            err_count,
            max_failures,
            committee_size: committee.members().count(),
        })
    }

    async fn get_virtual_substates(
        &self,
        _transaction: &Transaction,
        epoch: Epoch,
    ) -> Result<VirtualSubstates, DryRunTransactionProcessorError> {
        let mut virtual_substates = VirtualSubstates::new();

        virtual_substates.insert(
            VirtualSubstateId::CurrentEpoch,
            VirtualSubstate::CurrentEpoch(epoch.as_u64()),
        );

        Ok(virtual_substates)
    }
}
