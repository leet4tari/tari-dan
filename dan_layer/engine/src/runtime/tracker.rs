//   Copyright 2022. The Tari Project
//
//   Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
//   following conditions are met:
//
//   1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following
//   disclaimer.
//
//   2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the
//   following disclaimer in the documentation and/or other materials provided with the distribution.
//
//   3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote
//   products derived from this software without specific prior written permission.
//
//   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
//   INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
//   DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
//   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
//   SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
//   WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
//   USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

use std::{
    convert::TryFrom,
    sync::{Arc, Mutex, RwLock},
};

use indexmap::IndexMap;
use log::*;
use tari_dan_common_types::Epoch;
use tari_engine_types::{
    commit_result::{FinalizeResult, TransactionResult},
    component::{ComponentBody, ComponentHeader},
    confidential::UnclaimedConfidentialOutput,
    events::Event,
    fees::FeeSource,
    indexed_value::{IndexedValue, IndexedWellKnownTypes},
    lock::LockFlag,
    logs::LogEntry,
    substate::{InvalidSubstateIdVariant, SubstateId, SubstateValue},
    virtual_substate::VirtualSubstates,
    TemplateAddress,
};
use tari_template_lib::{
    auth::{ComponentAccessRules, OwnerRule},
    crypto::RistrettoPublicKeyBytes,
    models::{AddressAllocation, Amount, BucketId, ComponentAddress, Metadata, UnclaimedConfidentialOutputAddress},
    Hash,
};
use tari_transaction::TransactionWeight;

use crate::{
    runtime::{
        locking::LockedSubstate,
        scope::{CallScope, PushCallFrame},
        working_state::WorkingState,
        workspace::Workspace,
        RuntimeError,
    },
    state_store::memory::ReadOnlyMemoryStateStore,
};

const LOG_TARGET: &str = "tari::dan::engine::runtime::state_tracker";

#[derive(Debug, Clone)]
pub struct StateTracker {
    working_state: Arc<RwLock<WorkingState>>,
    fee_checkpoint: Arc<Mutex<Option<WorkingState>>>,
    transaction_weight: TransactionWeight,
}

impl StateTracker {
    pub fn new(
        state_store: ReadOnlyMemoryStateStore,
        virtual_substates: VirtualSubstates,
        initial_call_scope: CallScope,
        transaction_hash: Hash,
        transaction_weight: TransactionWeight,
    ) -> Self {
        Self {
            working_state: Arc::new(RwLock::new(WorkingState::new(
                state_store,
                virtual_substates,
                initial_call_scope,
                transaction_hash,
            ))),
            fee_checkpoint: Arc::new(Mutex::new(None)),
            transaction_weight,
        }
    }

    pub fn get_transaction_weight(&self) -> TransactionWeight {
        self.transaction_weight
    }

    pub fn get_current_epoch(&self) -> Result<Epoch, RuntimeError> {
        self.read_with(|state| state.get_current_epoch())
    }

    pub fn get_pseudorandom_bytes(&self, length: usize) -> Result<Vec<u8>, RuntimeError> {
        self.read_with(|state| {
            let id_provider = state.id_provider()?;
            let bytes = id_provider.get_random_bytes(length)?;
            Ok(bytes)
        })
    }

    pub fn add_event(&self, event: Event) {
        debug!(target: LOG_TARGET, "Emit: {event}");
        self.write_with(|state| state.push_event(event));
    }

    pub fn add_log(&self, log: LogEntry) {
        self.write_with(|state| state.push_log(log));
    }

    pub fn take_events(&self) -> Vec<Event> {
        self.write_with(|state| state.take_events())
    }

    pub fn num_events(&self) -> usize {
        self.read_with(|state| state.events().len())
    }

    pub fn num_logs(&self) -> usize {
        self.read_with(|state| state.logs().len())
    }

    pub fn get_template_address(&self) -> Result<TemplateAddress, RuntimeError> {
        self.read_with(|state| state.current_template().map(|(a, _)| *a))
    }

    pub fn list_buckets(&self) -> Vec<BucketId> {
        self.read_with(|state| state.buckets().keys().copied().collect())
    }

    pub fn take_unclaimed_confidential_output(
        &self,
        address: UnclaimedConfidentialOutputAddress,
    ) -> Result<UnclaimedConfidentialOutput, RuntimeError> {
        self.write_with(|state| {
            let output_lock = state.lock_substate(&address.into(), LockFlag::Write)?;
            let output = state
                .get_locked_substate(&output_lock)?
                .as_unclaimed_confidential_output()
                .cloned()
                .ok_or_else(|| RuntimeError::InvariantError {
                    function: "StateTracker::take_unclaimed_confidential_output",
                    details: format!(
                        "Expected substate at address {} to be an UnclaimedConfidentialOutput",
                        address
                    ),
                })?;
            state.claim_confidential_output(&address)?;
            state.unlock_substate(output_lock)?;
            Ok(output)
        })
    }

    pub fn new_component(
        &self,
        component_state: tari_bor::Value,
        owner_key: Option<RistrettoPublicKeyBytes>,
        owner_rule: OwnerRule,
        access_rules: ComponentAccessRules,
        address_allocation: Option<AddressAllocation<ComponentAddress>>,
    ) -> Result<ComponentAddress, RuntimeError> {
        self.write_with(|state| {
            let (template_address, module_name) =
                state.current_template().map(|(addr, name)| (*addr, name.to_string()))?;

            let component_address = match address_allocation {
                Some(address_allocation) => {
                    let addr = state.take_allocated_address(address_allocation.id())?;
                    addr.try_into().map_err(|error: InvalidSubstateIdVariant| {
                        RuntimeError::AddressAllocationTypeMismatch {
                            id: error.substate_id,
                            expected: error.expected,
                        }
                    })?
                },
                None => state.id_provider()?.new_component_address(template_address, None)?,
            };

            let component = ComponentBody { state: component_state };
            let component = ComponentHeader {
                template_address,
                module_name: module_name.clone(),
                owner_key,
                access_rules,
                owner_rule,
                entity_id: component_address.entity_id(),
                body: component,
            };
            let substate_id = SubstateId::Component(component_address);

            // The template address/component_id combination will not necessarily be unique so we need to check this.
            if state.substate_exists(&substate_id)? {
                return Err(RuntimeError::ComponentAlreadyExists {
                    address: component_address,
                });
            }

            let indexed = IndexedWellKnownTypes::from_value(&component.body.state)?;
            state.validate_component_state(None, &indexed)?;

            state.new_substate(substate_id.clone(), SubstateValue::Component(component))?;

            state.push_event(Event::std(
                Some(substate_id),
                template_address,
                state.transaction_hash(),
                "component",
                "created",
                Metadata::from([("module_name".to_string(), module_name)]),
            ));

            debug!(target: LOG_TARGET, "New component created: {}", component_address);
            Ok(component_address)
        })
    }

    pub fn lock_substate(&self, address: &SubstateId, lock_flag: LockFlag) -> Result<LockedSubstate, RuntimeError> {
        self.write_with(|state| state.lock_substate(address, lock_flag))
    }

    pub fn unlock_substate(&self, locked: LockedSubstate) -> Result<(), RuntimeError> {
        self.write_with(|state| state.unlock_substate(locked))
    }

    pub fn push_call_frame(&self, push_frame: PushCallFrame, max_call_depth: usize) -> Result<(), RuntimeError> {
        self.write_with(|state| {
            // If substates used in args are in scope for the current frame, we can bring then into scope for the new
            // frame
            trace!(
                 target: LOG_TARGET,
                "CALL FRAME before:\n{}",
                state.current_call_scope()?,
            );
            state.check_all_substates_in_scope(push_frame.arg_scope())?;

            let new_frame = push_frame.into_new_call_frame();
            trace!(target: LOG_TARGET, "NEW CALL FRAME:\n{}", new_frame.scope());

            state.push_frame(new_frame, max_call_depth)
        })
    }

    pub fn pop_call_frame(&self) -> Result<(), RuntimeError> {
        self.write_with(|state| state.pop_frame())
    }

    pub fn take_last_instruction_output(&self) -> Option<IndexedValue> {
        self.write_with(|state| state.take_last_instruction_output())
    }

    pub fn get_from_workspace(&self, key: &[u8]) -> Result<IndexedValue, RuntimeError> {
        self.read_with(|state| {
            state
                .workspace()
                .get(key)
                .cloned()
                .ok_or(RuntimeError::ItemNotOnWorkspace {
                    key: String::from_utf8_lossy(key).to_string(),
                })
        })
    }

    pub fn with_workspace<F: FnOnce(&Workspace) -> R, R>(&self, f: F) -> R {
        self.read_with(|state| f(state.workspace()))
    }

    pub fn with_workspace_mut<F: FnOnce(&mut Workspace) -> R, R>(&self, f: F) -> R {
        self.write_with(|state| f(state.workspace_mut()))
    }

    pub fn add_fee_charge(&self, source: FeeSource, amount: u64) {
        if amount == 0 {
            debug!(target: LOG_TARGET, "Add fee: source: {:?}, amount: {}", source, amount);
            return;
        }

        self.write_with(|state| {
            debug!(target: LOG_TARGET, "Add fee: source: {:?}, amount: {}", source, amount);
            state.fee_state_mut().fee_charges.insert(source, amount);
        })
    }

    pub fn finalize(
        &self,
        mut substates_to_persist: IndexMap<SubstateId, SubstateValue>,
    ) -> Result<FinalizeResult, RuntimeError> {
        // Finalise will always reset the state
        let mut state = self.take_working_state();
        if state.call_frame_depth() > 0 {
            return Err(RuntimeError::CallFrameRemainingOnStack {
                remaining: state.call_frame_depth(),
            });
        }
        // Resolve the transfers to the fee pool resource and vault refunds
        let transaction_receipt = state.finalize_fees(&mut substates_to_persist)?;

        let fee_receipt = transaction_receipt.fee_receipt.clone();

        let result = state.generate_substate_diff(transaction_receipt, substates_to_persist);

        let result = match result {
            Ok(substate_diff) => TransactionResult::Accept(substate_diff),
            Err(err) => TransactionResult::Reject(err.to_reject_reason()),
        };

        let finalized = FinalizeResult::new(
            state.transaction_hash(),
            state.take_logs(),
            state.take_events(),
            result,
            fee_receipt,
        );

        Ok(finalized)
    }

    pub fn fee_checkpoint(&self) -> Result<(), RuntimeError> {
        self.read_with(|state| {
            // Check that the checkpoint is in a valid state
            state.validate_finalized()?;
            let mut checkpoint = self.fee_checkpoint.lock().unwrap();
            *checkpoint = Some(state.clone());
            Ok(())
        })
    }

    pub fn reset_to_fee_checkpoint(&self) -> Result<(), RuntimeError> {
        let mut checkpoint = self.fee_checkpoint.lock().unwrap();
        if let Some(checkpoint) = checkpoint.take() {
            self.write_with(|state| {
                let fee_state = state.fee_state().clone();
                *state = checkpoint;
                // Preserve fee state across resets
                *state.fee_state_mut() = fee_state;
            });
            Ok(())
        } else {
            Err(RuntimeError::NoFeeCheckpoint)
        }
    }

    fn take_working_state(&self) -> WorkingState {
        self.write_with(|current_state| current_state.take_state())
    }

    pub fn take_substates_to_persist(&self) -> IndexMap<SubstateId, SubstateValue> {
        self.write_with(|state| state.take_mutated_substates())
    }

    pub fn with_substates_to_persist<F: FnMut(&IndexMap<SubstateId, SubstateValue>) -> R, R>(&self, mut f: F) -> R {
        self.write_with(|state| f(state.mutated_substates()))
    }

    pub fn are_fees_paid_in_full(&self) -> bool {
        self.read_with(|state| {
            let total_payments = state.fee_state().total_payments();
            let total_charges = state.fee_state().total_charges();
            total_payments >= total_charges
        })
    }

    pub fn total_fee_payments(&self) -> Amount {
        self.read_with(|state| state.fee_state().total_payments())
    }

    pub fn total_fee_charges(&self) -> Amount {
        self.read_with(|state| Amount::try_from(state.fee_state().total_charges()).expect("fee overflowed i64::MAX"))
    }

    pub(super) fn read_with<R, F: FnOnce(&WorkingState) -> R>(&self, f: F) -> R {
        f(&self.working_state.read().unwrap())
    }

    pub(super) fn write_with<R, F: FnOnce(&mut WorkingState) -> R>(&self, f: F) -> R {
        f(&mut self.working_state.write().unwrap())
    }
}
