//   Copyright 2023 The Tari Project
//   SPDX-License-Identifier: BSD-3-Clause

use std::{
    cmp,
    collections::{BTreeSet, HashMap, HashSet},
    mem,
};

use indexmap::IndexMap;
use log::*;
use tari_dan_common_types::{optional::Optional, Epoch};
use tari_engine_types::{
    bucket::Bucket,
    component::ComponentHeader,
    events::Event,
    fees::FeeReceipt,
    id_provider::{IdProvider, ObjectIds},
    indexed_value::{IndexedValue, IndexedWellKnownTypes},
    lock::LockFlag,
    logs::LogEntry,
    non_fungible::NonFungibleContainer,
    proof::{ContainerRef, LockedResource, Proof},
    resource::Resource,
    resource_container::{ResourceContainer, ResourceError},
    substate::{Substate, SubstateDiff, SubstateId, SubstateValue},
    transaction_receipt::TransactionReceipt,
    vault::Vault,
    virtual_substate::{VirtualSubstate, VirtualSubstateId, VirtualSubstates},
    vn_fee_pool::ValidatorFeePoolAddress,
    TemplateAddress,
};
use tari_template_lib::{
    args::{MintArg, ResourceDiscriminator},
    constants::CONFIDENTIAL_TARI_RESOURCE_ADDRESS,
    models::{
        AddressAllocation,
        Amount,
        BucketId,
        ComponentAddress,
        NonFungibleAddress,
        ProofId,
        UnclaimedConfidentialOutputAddress,
        VaultId,
    },
    prelude::{AuthHookCaller, PUBLIC_IDENTITY_RESOURCE_ADDRESS},
    Hash,
};

use super::workspace::Workspace;
use crate::{
    runtime::{
        address_allocation::AllocatedAddress,
        fee_state::FeeState,
        locking::LockedSubstate,
        scope::{CallFrame, CallScope},
        state_store::WorkingStateStore,
        tracker_auth::Authorization,
        ActionIdent,
        NativeAction,
        RuntimeError,
        TransactionCommitError,
    },
    state_store::memory::ReadOnlyMemoryStateStore,
};

const LOG_TARGET: &str = "dan::engine::runtime::working_state";

#[derive(Debug, Clone)]
pub(super) struct WorkingState {
    transaction_hash: Hash,
    events: Vec<Event>,
    logs: Vec<LogEntry>,
    buckets: HashMap<BucketId, Bucket>,
    address_allocations: HashMap<u32, AllocatedAddress>,
    address_allocation_id: u32,
    proofs: HashMap<ProofId, Proof>,
    object_ids: ObjectIds,

    store: WorkingStateStore,

    claimed_confidential_outputs: Vec<UnclaimedConfidentialOutputAddress>,
    virtual_substates: VirtualSubstates,

    last_instruction_output: Option<IndexedValue>,
    workspace: Workspace,
    call_frames: Vec<CallFrame>,
    initial_call_scope: CallScope,

    fee_state: FeeState,
}

impl WorkingState {
    pub fn new(
        state_store: ReadOnlyMemoryStateStore,
        virtual_substates: VirtualSubstates,
        initial_call_scope: CallScope,
        transaction_hash: Hash,
    ) -> Self {
        Self {
            transaction_hash,
            events: Vec::new(),
            logs: Vec::new(),
            buckets: HashMap::new(),
            proofs: HashMap::new(),
            address_allocation_id: 0,
            address_allocations: HashMap::new(),

            store: WorkingStateStore::new(state_store),

            claimed_confidential_outputs: Vec::new(),
            last_instruction_output: None,

            workspace: Workspace::default(),
            virtual_substates,
            call_frames: Vec::new(),
            initial_call_scope,
            fee_state: FeeState::new(),
            object_ids: ObjectIds::new(1000),
        }
    }

    pub fn transaction_hash(&self) -> Hash {
        self.transaction_hash
    }

    pub fn substate_exists(&self, address: &SubstateId) -> Result<bool, RuntimeError> {
        // All public identity resources exist
        if address
            .as_non_fungible_address()
            .map(|a| *a.resource_address() == PUBLIC_IDENTITY_RESOURCE_ADDRESS)
            .unwrap_or(false)
        {
            return Ok(true);
        }

        self.store.exists(address)
    }

    pub fn new_substate<K: Into<SubstateId>, V: Into<SubstateValue>>(
        &mut self,
        address: K,
        value: V,
    ) -> Result<(), RuntimeError> {
        let address = address.into();
        self.current_call_scope_mut()?.add_substate_to_scope(address.clone())?;
        self.store.insert(address, value.into())?;
        Ok(())
    }

    pub fn lock_substate(&mut self, addr: &SubstateId, lock_flag: LockFlag) -> Result<LockedSubstate, RuntimeError> {
        let lock_id = self.store.try_lock(addr, lock_flag)?;
        Ok(LockedSubstate::new(addr.clone(), lock_id, lock_flag))
    }

    pub fn unlock_substate(&mut self, lock: LockedSubstate) -> Result<(), RuntimeError> {
        self.store.try_unlock(lock.lock_id())?;
        Ok(())
    }

    pub fn get_component(&self, locked: &LockedSubstate) -> Result<&ComponentHeader, RuntimeError> {
        let (address, substate) = self.store.get_locked_substate(locked.lock_id())?;
        let component = substate.component().ok_or_else(|| RuntimeError::LockSubstateMismatch {
            lock_id: locked.lock_id(),
            address,
            expected_type: "Component",
        })?;
        Ok(component)
    }

    pub fn modify_component_with<F: FnOnce(&mut ComponentHeader) -> bool>(
        &mut self,
        locked: &LockedSubstate,
        f: F,
    ) -> Result<(), RuntimeError> {
        let maybe_before_and_after = self
            .store
            .mutate_locked_substate_with(locked.lock_id(), |_, substate_mut| {
                let component_mut = substate_mut
                    .component_mut()
                    .ok_or_else(|| RuntimeError::LockSubstateMismatch {
                        lock_id: locked.lock_id(),
                        address: locked.address().clone(),
                        expected_type: "Component",
                    })?;

                let before = IndexedWellKnownTypes::from_value(component_mut.state())?;
                if !f(component_mut) {
                    // rollback
                    return Ok(None);
                }

                let after = IndexedWellKnownTypes::from_value(component_mut.state())?;
                Ok(Some((before, after)))
            })?;

        let Some((before, after)) = maybe_before_and_after else {
            return Ok(());
        };

        self.validate_component_state(Some(&before), &after)?;

        // add event to indicate that there is a change in component
        let (template_address, module_name) = self.current_template().map(|(addr, name)| (*addr, name.to_string()))?;
        self.push_event(Event::std(
            Some(locked.address().clone()),
            template_address,
            self.transaction_hash(),
            "component",
            "updated",
            tari_template_lib::models::Metadata::from([("module_name".to_string(), module_name)]),
        ));

        Ok(())
    }

    pub fn get_resource(&self, locked: &LockedSubstate) -> Result<&Resource, RuntimeError> {
        let (addr, substate) = self.store.get_locked_substate(locked.lock_id())?;

        let resource = substate
            .as_resource()
            .ok_or_else(|| RuntimeError::LockSubstateMismatch {
                lock_id: locked.lock_id(),
                address: addr,
                expected_type: "Resource",
            })?;

        Ok(resource)
    }

    pub fn get_non_fungible(&self, locked: &LockedSubstate) -> Result<&NonFungibleContainer, RuntimeError> {
        let (address, value) = self.store.get_locked_substate(locked.lock_id())?;
        let non_fungible = value
            .as_non_fungible()
            .ok_or_else(|| RuntimeError::LockSubstateMismatch {
                lock_id: 0,
                address: address.clone(),
                expected_type: "NonFungible",
            })?;
        Ok(non_fungible)
    }

    pub fn get_non_fungible_mut(&mut self, locked: &LockedSubstate) -> Result<&mut NonFungibleContainer, RuntimeError> {
        let (address, value) = self.store.get_locked_substate_mut(locked.lock_id())?;
        let non_fungible = value
            .as_non_fungible_mut()
            .ok_or_else(|| RuntimeError::LockSubstateMismatch {
                lock_id: 0,
                address: address.clone(),
                expected_type: "NonFungible",
            })?;
        Ok(non_fungible)
    }

    pub fn claim_confidential_output(&mut self, addr: &UnclaimedConfidentialOutputAddress) -> Result<(), RuntimeError> {
        if self.claimed_confidential_outputs.contains(addr) {
            return Err(RuntimeError::ConfidentialOutputAlreadyClaimed { address: *addr });
        }
        self.claimed_confidential_outputs.push(*addr);
        Ok(())
    }

    pub fn get_locked_substate(&self, lock: &LockedSubstate) -> Result<&SubstateValue, RuntimeError> {
        let (_, substate) = self.store.get_locked_substate(lock.lock_id())?;
        Ok(substate)
    }

    pub fn get_locked_substate_mut(&mut self, lock: &LockedSubstate) -> Result<&mut SubstateValue, RuntimeError> {
        let (_, substate) = self.store.get_locked_substate_mut(lock.lock_id())?;
        Ok(substate)
    }

    pub fn get_vault(&self, locked: &LockedSubstate) -> Result<&Vault, RuntimeError> {
        let (addr, substate) = self.store.get_locked_substate(locked.lock_id())?;

        let vault = substate.as_vault().ok_or_else(|| RuntimeError::LockSubstateMismatch {
            lock_id: locked.lock_id(),
            address: addr,
            expected_type: "Vault",
        })?;

        Ok(vault)
    }

    pub fn get_vault_mut(&mut self, locked: &LockedSubstate) -> Result<&mut Vault, RuntimeError> {
        let (addr, substate) = self.store.get_locked_substate_mut(locked.lock_id())?;

        let vault_mut = substate
            .as_vault_mut()
            .ok_or_else(|| RuntimeError::LockSubstateMismatch {
                lock_id: locked.lock_id(),
                address: addr,
                expected_type: "Vault",
            })?;

        Ok(vault_mut)
    }

    pub fn get_resource_mut(&mut self, locked: &LockedSubstate) -> Result<&mut Resource, RuntimeError> {
        let (addr, substate) = self.store.get_locked_substate_mut(locked.lock_id())?;

        let resource_mut = substate
            .as_resource_mut()
            .ok_or_else(|| RuntimeError::LockSubstateMismatch {
                lock_id: locked.lock_id(),
                address: addr,
                expected_type: "Resource",
            })?;

        Ok(resource_mut)
    }

    pub fn get_current_epoch(&self) -> Result<Epoch, RuntimeError> {
        let address = VirtualSubstateId::CurrentEpoch;
        let current_epoch =
            self.virtual_substates
                .get(&address)
                .ok_or_else(|| RuntimeError::VirtualSubstateNotFound {
                    address: address.clone(),
                })?;
        let VirtualSubstate::CurrentEpoch(epoch) = current_epoch;
        Ok(Epoch(*epoch))
    }

    pub(super) fn validate_finalized(&self) -> Result<(), RuntimeError> {
        if !self.buckets.is_empty() {
            return Err(TransactionCommitError::DanglingBuckets {
                count: self.buckets.len(),
            }
            .into());
        }

        if !self.proofs.is_empty() {
            return Err(TransactionCommitError::DanglingProofs {
                count: self.proofs.len(),
            }
            .into());
        }

        if !self.address_allocations.is_empty() {
            return Err(TransactionCommitError::DanglingAddressAllocations {
                count: self.address_allocations.len(),
            }
            .into());
        }

        for (vault_id, vault) in self.store.new_vaults() {
            if !vault.locked_balance().is_zero() {
                return Err(TransactionCommitError::DanglingLockedValueInVault {
                    vault_id,
                    locked_amount: vault.locked_balance(),
                }
                .into());
            }
        }

        if self.call_frame_depth() != 0 {
            return Err(RuntimeError::CallFrameRemainingOnStack {
                remaining: self.call_frame_depth(),
            });
        }
        // Final call frame can be none if there are no instructions (due to either fee instructions or instructions
        // being empty)
        let call_scope = self.base_call_scope();
        if !call_scope.orphans().is_empty() {
            return Err(TransactionCommitError::OrphanedSubstates {
                substates: call_scope.orphans().iter().map(ToString::to_string).collect(),
            }
            .into());
        }

        Ok(())
    }

    pub fn get_proof(&self, proof_id: ProofId) -> Result<&Proof, RuntimeError> {
        self.proofs
            .get(&proof_id)
            .ok_or(RuntimeError::ProofNotFound { proof_id })
    }

    pub fn proof_exists(&self, proof_id: ProofId) -> bool {
        self.proofs.contains_key(&proof_id)
    }

    pub fn get_bucket(&self, bucket_id: BucketId) -> Result<&Bucket, RuntimeError> {
        if !self.current_call_scope()?.is_bucket_in_scope(bucket_id) {
            return Err(RuntimeError::BucketNotFound { bucket_id });
        }
        self.buckets
            .get(&bucket_id)
            .ok_or(RuntimeError::BucketNotFound { bucket_id })
    }

    pub fn get_bucket_mut(&mut self, bucket_id: BucketId) -> Result<&mut Bucket, RuntimeError> {
        if !self.current_call_scope()?.is_bucket_in_scope(bucket_id) {
            return Err(RuntimeError::BucketNotFound { bucket_id });
        }
        self.buckets
            .get_mut(&bucket_id)
            .ok_or(RuntimeError::BucketNotFound { bucket_id })
    }

    pub fn take_bucket(&mut self, bucket_id: BucketId) -> Result<Bucket, RuntimeError> {
        if !self.current_call_scope()?.is_bucket_in_scope(bucket_id) {
            return Err(RuntimeError::BucketNotFound { bucket_id });
        }
        let bucket = self
            .buckets
            .remove(&bucket_id)
            .ok_or(RuntimeError::BucketNotFound { bucket_id })?;

        // Use of the bucket adds the resource to the scope
        let resource_addr = *bucket.resource_address();
        {
            let scope_mut = self.current_call_scope_mut()?;
            scope_mut.remove_bucket_from_scope(bucket_id);
            scope_mut.add_substate_to_owned(resource_addr.into());
        }
        Ok(bucket)
    }

    pub fn burn_bucket(&mut self, bucket: Bucket) -> Result<(), RuntimeError> {
        if bucket.amount().is_zero() {
            return Ok(());
        }
        let resource_address = *bucket.resource_address();
        // Burn Non-fungibles (if resource is nf). Fungibles are burnt by removing the bucket from the tracker state
        // and not depositing it.
        for token_id in bucket.into_non_fungible_ids().into_iter().flatten() {
            let address = NonFungibleAddress::new(resource_address, token_id);
            let locked_nft = self.lock_substate(&SubstateId::NonFungible(address.clone()), LockFlag::Write)?;
            let nft = self.get_non_fungible_mut(&locked_nft)?;

            if nft.is_burnt() {
                return Err(RuntimeError::InvalidOpNonFungibleBurnt {
                    op: "burn_bucket",
                    resource_address,
                    nf_id: address.id().clone(),
                });
            }
            nft.burn();
            self.unlock_substate(locked_nft)?;
        }

        Ok(())
    }

    pub fn drop_proof(&mut self, proof_id: ProofId) -> Result<(), RuntimeError> {
        // Remove it from the auth scope if is in scope
        let call_frame_mut = self.current_call_scope_mut()?;
        if !call_frame_mut.is_proof_in_scope(proof_id) {
            return Err(RuntimeError::ProofNotFound { proof_id });
        }
        call_frame_mut.auth_scope_mut().remove_proof(&proof_id);

        // Fetch the proof
        let proof = self
            .proofs
            .remove(&proof_id)
            .ok_or(RuntimeError::ProofNotFound { proof_id })?;

        // Unlock funds
        match *proof.container() {
            ContainerRef::Bucket(bucket_id) => {
                self.buckets
                    .get_mut(&bucket_id)
                    .ok_or(RuntimeError::BucketNotFound { bucket_id })?
                    .unlock(proof)?;
            },
            ContainerRef::Vault(vault_id) => {
                let vault_lock = self.lock_substate(&SubstateId::Vault(vault_id), LockFlag::Write)?;
                self.get_vault_mut(&vault_lock)?.unlock(proof)?;
                self.unlock_substate(vault_lock)?;
            },
        }

        Ok(())
    }

    pub fn mint_resource(
        &mut self,
        locked_resource: &LockedSubstate,
        mint_arg: MintArg,
    ) -> Result<ResourceContainer, RuntimeError> {
        let resource_address =
            locked_resource
                .address()
                .as_resource_address()
                .ok_or_else(|| RuntimeError::InvariantError {
                    function: "mint_resource",
                    details: "LockedSubstate substate_id is not a ResourceAddress".to_string(),
                })?;

        let resource_container = match mint_arg {
            MintArg::Fungible { amount } => {
                if amount.is_negative() {
                    return Err(RuntimeError::InvalidAmount {
                        amount,
                        reason: "Amount must be positive".to_string(),
                    });
                }

                debug!(
                    target: LOG_TARGET,
                    "Minting {} fungible tokens on resource: {}", amount, resource_address
                );

                ResourceContainer::fungible(resource_address, amount)
            },
            MintArg::NonFungible { tokens } => {
                debug!(
                    target: LOG_TARGET,
                    "Minting {} NFT token(s) on resource: {}",
                    tokens.len(),
                    resource_address
                );
                let mut token_ids = BTreeSet::new();

                // let resource = self.get_resource(locked_resource)?;
                // TODO: This isn't correct (assumes tokens are never burnt), we'll need to rethink this
                // let mut index = resource
                //     .total_supply()
                //     .as_u64_checked()
                //     .ok_or(RuntimeError::InvalidAmount {
                //         amount: resource.total_supply(),
                //         reason: "Could not convert to u64".to_owned(),
                //     })?;

                for (id, (data, mut_data)) in tokens {
                    let nft_address = NonFungibleAddress::new(resource_address, id);
                    let token_id = nft_address.id().clone();
                    let addr = SubstateId::NonFungible(nft_address);
                    if self.substate_exists(&addr)? {
                        return Err(RuntimeError::DuplicateNonFungibleId { token_id });
                    } else {
                        token_ids.insert(token_id);
                        self.new_substate(addr.clone(), NonFungibleContainer::new(data, mut_data))?;
                    }

                    // for each new nft we also create an index to be allow resource scanning
                    // let index_address = NonFungibleIndexAddress::new(resource_address, index);
                    // index += 1;
                    // let nft_index = NonFungibleIndex::new(nft_address);
                    // self.new_substate(index_address, nft_index)?;
                }

                ResourceContainer::non_fungible(resource_address, token_ids)
            },
            MintArg::Confidential { proof } => {
                let resource = self.get_resource(locked_resource)?;
                debug!(
                    target: LOG_TARGET,
                    "Minting confidential tokens on resource: {}", resource_address
                );
                ResourceContainer::mint_confidential(resource_address, *proof, resource.view_key())?
            },
        };

        // Increase the total supply, this also validates that the resource already exists.
        {
            let resource_mut = self.get_resource_mut(locked_resource)?;
            if resource_mut.resource_type() != resource_container.resource_type() {
                return Err(ResourceError::ResourceTypeMismatch {
                    operate: "mint",
                    expected: resource_mut.resource_type(),
                    given: resource_container.resource_type(),
                }
                .into());
            }
            resource_mut.increase_total_supply(resource_container.amount());
        }

        Ok(resource_container)
    }

    pub fn recall_resource_from_vault(
        &mut self,
        vault_lock: &LockedSubstate,
        resource_discriminator: ResourceDiscriminator,
    ) -> Result<ResourceContainer, RuntimeError> {
        let vault_id = vault_lock
            .address()
            .as_vault_id()
            .ok_or_else(|| RuntimeError::InvariantError {
                function: "recall_resource_from_vault",
                details: "LockedSubstate substate_id is not a VaultId".to_string(),
            })?;

        let vault_mut = self.get_vault_mut(vault_lock)?;
        let resource_address = *vault_mut.resource_address();

        let resource_container = match resource_discriminator {
            ResourceDiscriminator::Everything => vault_mut.recall_all()?,
            ResourceDiscriminator::Fungible { amount } => {
                if amount.is_negative() {
                    return Err(RuntimeError::InvalidAmount {
                        amount,
                        reason: "Amount must be positive".to_string(),
                    });
                }

                if !vault_mut.resource_type().is_fungible() {
                    return Err(RuntimeError::InvalidArgument {
                        argument: "resource",
                        reason: format!(
                            "Vault {} contains a {} resource but a fungible was requested",
                            vault_id,
                            vault_mut.resource_type()
                        ),
                    });
                }

                debug!(
                    target: LOG_TARGET,
                    "Recalling {} fungible tokens on resource: {}", amount, resource_address
                );
                vault_mut.withdraw(amount)?
            },
            ResourceDiscriminator::NonFungible { tokens } => {
                debug!(
                    target: LOG_TARGET,
                    "Recalling {} NFT token(s) on vault: {}",
                    tokens.len(),
                    vault_id
                );

                if !vault_mut.resource_type().is_non_fungible() {
                    return Err(RuntimeError::InvalidArgument {
                        argument: "resource",
                        reason: format!(
                            "Vault {} contains a {} resource but a non-fungible was requested",
                            vault_id,
                            vault_mut.resource_type()
                        ),
                    });
                }

                vault_mut.withdraw_non_fungibles(&tokens)?
            },
            ResourceDiscriminator::Confidential {
                commitments,
                revealed_amount,
            } => {
                debug!(
                    target: LOG_TARGET,
                    "Recalling confidential tokens on vault: {}", vault_id
                );

                if !vault_mut.resource_type().is_confidential() {
                    return Err(RuntimeError::InvalidArgument {
                        argument: "resource",
                        reason: format!(
                            "Vault contains a {} resource but a confidential was requested",
                            vault_mut.resource_type()
                        ),
                    });
                }

                vault_mut.recall_confidential(commitments, revealed_amount)?
            },
        };

        Ok(resource_container)
    }

    pub fn new_bucket(&mut self, bucket_id: BucketId, resource: ResourceContainer) -> Result<(), RuntimeError> {
        debug!(
            target: LOG_TARGET,
            "New bucket {} for resource {} {:?}", bucket_id, resource.resource_address(), resource.resource_type()
        );

        // Mark Resource and NFT substates as owned since they are going into a bucket
        {
            let scope_mut = self.current_call_scope_mut()?;
            scope_mut.move_node_to_owned(&(*resource.resource_address()).into())?;
            for id in resource.non_fungible_token_ids() {
                scope_mut
                    .move_node_to_owned(&NonFungibleAddress::new(*resource.resource_address(), id.clone()).into())?;
            }
        }

        let bucket = Bucket::new(bucket_id, resource);
        if self.buckets.insert(bucket_id, bucket).is_some() {
            return Err(RuntimeError::DuplicateBucket { bucket_id });
        }
        self.current_call_scope_mut()?.add_bucket_to_scope(bucket_id);
        Ok(())
    }

    pub fn new_proof(&mut self, proof_id: ProofId, locked_funds: LockedResource) -> Result<(), RuntimeError> {
        debug!(target: LOG_TARGET, "New proof {}", proof_id);
        if self.proofs.insert(proof_id, Proof::new(locked_funds)).is_some() {
            return Err(RuntimeError::DuplicateProof { proof_id });
        }

        self.current_call_scope_mut()?.add_proof_to_scope(proof_id);
        Ok(())
    }

    pub fn new_address_allocation<T: Into<SubstateId> + Clone>(
        &mut self,
        address: T,
    ) -> Result<AddressAllocation<T>, RuntimeError> {
        let id = self.address_allocation_id;
        self.address_allocation_id += 1;
        let (current_template, _) = self.current_template()?;
        let current_template = *current_template;
        self.address_allocations
            .insert(id, AllocatedAddress::new(current_template, address.clone().into()));
        let allocation = AddressAllocation::new(id, address);
        Ok(allocation)
    }

    pub fn get_allocated_address_by_address<T: Into<SubstateId>>(&mut self, address: T) -> Option<&AllocatedAddress> {
        let substate_id = address.into();
        self.address_allocations
            .values()
            .find(|alloc| *alloc.address() == substate_id)
    }

    pub fn get_template_for_component(
        &mut self,
        component_address: &ComponentAddress,
    ) -> Result<TemplateAddress, RuntimeError> {
        match self.get_allocated_address_by_address(*component_address) {
            Some(alloc) => Ok(*alloc.template_address()),
            None => {
                let component = self.store.load_component(component_address)?;
                Ok(component.template_address)
            },
        }
    }

    pub fn take_allocated_address(&mut self, id: u32) -> Result<AllocatedAddress, RuntimeError> {
        self.address_allocations
            .remove(&id)
            .ok_or(RuntimeError::AddressAllocationNotFound { id })
    }

    pub fn pay_fee(&mut self, resource: ResourceContainer, return_vault: VaultId) -> Result<(), RuntimeError> {
        self.fee_state.fee_payments.push((resource, return_vault));
        Ok(())
    }

    pub fn withdraw_all_fees_from_pool(
        &mut self,
        address: ValidatorFeePoolAddress,
    ) -> Result<ResourceContainer, RuntimeError> {
        let locked_substate = self.lock_substate(&SubstateId::ValidatorFeePool(address), LockFlag::Write)?;
        let fee_pool = self
            .get_locked_substate(&locked_substate)?
            .as_validator_fee_pool()
            .ok_or_else(|| RuntimeError::InvariantError {
                function: "StateTracker::claim_fee",
                details: format!("Expected substate at address {address} to be an ValidatorFeePool",),
            })?;

        self.authorization()
            .require_ownership(NativeAction::WithdrawValidatorFunds, fee_pool.as_ownership())?;

        let pool_mut = self
            .get_locked_substate_mut(&locked_substate)?
            .as_validator_fee_pool_mut()
            .ok_or_else(|| RuntimeError::InvariantError {
                function: "StateTracker::claim_fee",
                details: format!("Expected substate at address {address} to be an ValidatorFeePool",),
            })?;

        let resource_container = pool_mut.withdraw_all()?;
        Ok(resource_container)
    }

    pub fn validate_component_state(
        &mut self,
        previous_state: Option<&IndexedWellKnownTypes>,
        next_state: &IndexedWellKnownTypes,
    ) -> Result<(), RuntimeError> {
        // Check that no vaults were dropped
        if let Some(prev_state) = previous_state {
            for existing_vault in prev_state.vault_ids() {
                // Vaults can never be removed from components
                if !next_state.vault_ids().contains(existing_vault) {
                    return Err(RuntimeError::OrphanedSubstate {
                        address: (*existing_vault).into(),
                    });
                }
            }
        }

        // Check that no vaults are duplicated
        let mut dup_check = HashSet::with_capacity(next_state.vault_ids().len());
        for vault_id in next_state.vault_ids() {
            if !dup_check.insert(vault_id) {
                return Err(RuntimeError::DuplicateReference {
                    address: (*vault_id).into(),
                });
            }
        }

        let diff_values = previous_state.map(|prev_state| next_state.diff(prev_state));

        // We only require newly added values to be in scope since previous values were already checked. For instance,
        // if a transaction uses an account does not have to input all vaults and resources just to transact on a
        // single vault.
        let new_values = diff_values.as_ref().unwrap_or(next_state);
        self.check_all_substates_in_scope(new_values)?;

        let scope_mut = self.current_call_scope_mut()?;
        for address in next_state.referenced_substates() {
            // Mark any orphaned objects as owned
            scope_mut.move_node_to_owned(&address)?
        }

        Ok(())
    }

    pub fn authorization(&self) -> Authorization {
        Authorization::new(self)
    }

    pub fn take_mutated_substates(&mut self) -> IndexMap<SubstateId, SubstateValue> {
        self.store.take_mutated_substates()
    }

    pub fn mutated_substates(&mut self) -> &IndexMap<SubstateId, SubstateValue> {
        self.store.mutated_substates()
    }

    pub fn fee_state(&self) -> &FeeState {
        &self.fee_state
    }

    pub fn fee_state_mut(&mut self) -> &mut FeeState {
        &mut self.fee_state
    }

    pub fn set_last_instruction_output(&mut self, output: IndexedValue) {
        self.last_instruction_output = Some(output);
    }

    pub fn finalize_fees(
        &mut self,
        substates_to_persist: &mut IndexMap<SubstateId, SubstateValue>,
    ) -> Result<TransactionReceipt, RuntimeError> {
        let total_fees =
            Amount::try_from(self.fee_state.total_charges()).map_err(|_| RuntimeError::InvariantError {
                function: "finalize_fees",
                details: format!(
                    "Total fees {} could not be converted to Amount",
                    self.fee_state.total_charges()
                ),
            })?;

        let total_fee_payment = self.fee_state.total_payments();

        let mut fee_resource =
            ResourceContainer::confidential(CONFIDENTIAL_TARI_RESOURCE_ADDRESS, None, Amount::zero());

        // Collect the fee
        let mut remaining_fees = total_fees;
        for (resx, _) in &mut self.fee_state.fee_payments {
            if remaining_fees.is_zero() {
                break;
            }
            let amount_to_withdraw = cmp::min(resx.amount(), remaining_fees);
            remaining_fees -= amount_to_withdraw;
            fee_resource.deposit(resx.withdraw(amount_to_withdraw)?)?;
        }

        // Refund the remaining payments if any
        for (mut resx, refund_vault) in self.fee_state.fee_payments.drain(..) {
            if resx.amount().is_zero() {
                continue;
            }

            let vault_mut = substates_to_persist
                .get_mut(&SubstateId::Vault(refund_vault))
                .expect("invariant: vault that made fee payment not in changeset")
                .as_vault_mut()
                .expect("invariant: substate substate_id for fee refund is not a vault");
            vault_mut.resource_container_mut().deposit(resx.recall_all()?)?;
        }

        Ok(TransactionReceipt {
            transaction_hash: self.transaction_hash,
            events: self.events.clone(),
            logs: self.logs.clone(),
            fee_receipt: FeeReceipt {
                total_fee_payment,
                total_fees_paid: fee_resource.amount(),
                cost_breakdown: mem::take(&mut self.fee_state.fee_charges),
            },
        })
    }

    pub(super) fn current_call_scope_mut(&mut self) -> Result<&mut CallScope, RuntimeError> {
        Ok(self
            .call_frames
            .last_mut()
            .map(|s| s.scope_mut())
            .unwrap_or(&mut self.initial_call_scope))
    }

    pub fn current_call_scope(&self) -> Result<&CallScope, RuntimeError> {
        Ok(self
            .call_frames
            .last()
            .map(|f| f.scope())
            .unwrap_or(&self.initial_call_scope))
    }

    pub fn call_frame_depth(&self) -> usize {
        self.call_frames.len()
    }

    /// Returns template address and module name
    pub fn current_template(&self) -> Result<(&TemplateAddress, &str), RuntimeError> {
        self.call_frames
            .last()
            .map(|frame| frame.current_template())
            .ok_or(RuntimeError::NoActiveCallFrame)
    }

    pub fn id_provider(&self) -> Result<IdProvider<'_>, RuntimeError> {
        self.call_frames
            .last()
            .map(|frame| IdProvider::new(frame.entity_id(), self.transaction_hash, &self.object_ids))
            .ok_or(RuntimeError::NoActiveCallFrame)
    }

    pub fn new_bucket_id(&mut self) -> BucketId {
        self.object_ids.next_bucket_id()
    }

    /// Returns the component that is currently in scope (if any)
    pub fn current_component(&self) -> Result<Option<ComponentAddress>, RuntimeError> {
        let frame = self.call_frames.last().ok_or(RuntimeError::NoActiveCallFrame)?;
        Ok(frame
            .scope()
            .get_current_component_lock()
            .and_then(|lock| lock.address().as_component_address()))
    }

    pub fn get_auth_caller(&self) -> Result<AuthHookCaller, RuntimeError> {
        let frame = self.call_frames.last().ok_or(RuntimeError::NoActiveCallFrame)?;
        let (template, _) = frame.current_template();
        let component = frame
            .scope()
            .get_current_component_lock()
            .and_then(|lock| lock.address().as_component_address());

        Ok(AuthHookCaller::new(*template, component))
    }

    pub fn push_frame(&mut self, mut new_frame: CallFrame, max_call_depth: usize) -> Result<(), RuntimeError> {
        if self.call_frame_depth() + 1 > max_call_depth {
            return Err(RuntimeError::MaxCallDepthExceeded {
                max_depth: max_call_depth,
            });
        }

        let current = self.current_call_scope()?;
        new_frame.scope_mut().update_from_parent(current);

        if self.call_frame_depth() == 0 {
            // If this is the first call frame, then we use the base auth scope (virtual proofs are carried from the
            // base to the first call scope)
            new_frame
                .scope_mut()
                .set_auth_scope(self.initial_call_scope.auth_scope().clone());
        }

        self.call_frames.push(new_frame);
        Ok(())
    }

    pub fn pop_frame(&mut self) -> Result<(), RuntimeError> {
        let current_frame = self.call_frames.pop().ok_or(RuntimeError::NoActiveCallFrame)?;

        let scope = current_frame.into_scope();
        // Unlock the component
        if let Some(component_lock) = scope.get_current_component_lock() {
            self.unlock_substate(component_lock.clone())?;
        }

        if !scope.lock_scope().is_empty() {
            return Err(RuntimeError::DanglingSubstateLocks {
                count: scope.lock_scope().len(),
            });
        }

        if !scope.orphans().is_empty() {
            return Err(RuntimeError::OrphanedSubstates {
                substates: scope.orphans().iter().map(ToString::to_string).collect(),
            });
        }

        // Update the parent call scope
        debug!(target: LOG_TARGET, "pop_frame:\n{}", scope);
        self.current_call_scope_mut()?.update_from_child_scope(scope);

        Ok(())
    }

    pub fn base_call_scope(&self) -> &CallScope {
        &self.initial_call_scope
    }

    pub fn take_state(&mut self) -> Self {
        let new_state = WorkingState::new(
            self.store.state_store().clone(),
            VirtualSubstates::new(),
            CallScope::new(),
            self.transaction_hash,
        );
        mem::replace(self, new_state)
    }

    pub fn workspace(&self) -> &Workspace {
        &self.workspace
    }

    pub fn workspace_mut(&mut self) -> &mut Workspace {
        &mut self.workspace
    }

    pub fn take_last_instruction_output(&mut self) -> Option<IndexedValue> {
        self.last_instruction_output.take()
    }

    pub fn load_component(&mut self, component_address: &ComponentAddress) -> Result<&ComponentHeader, RuntimeError> {
        self.store.load_component(component_address)
    }

    pub fn check_all_substates_known(&self, value: &IndexedWellKnownTypes) -> Result<(), RuntimeError> {
        for id in value.referenced_substates() {
            if !self.substate_exists(&id)? {
                return Err(RuntimeError::ReferencedSubstateNotFound { id: id.clone() });
            }
        }
        for bucket_id in value.bucket_ids() {
            if !self.buckets().contains_key(bucket_id) {
                return Err(RuntimeError::ValidationFailedBucketNotInScope { bucket_id: *bucket_id });
            }
        }
        for proof_id in value.proof_ids() {
            if !self.proofs().contains_key(proof_id) {
                return Err(RuntimeError::ValidationFailedProofNotInScope { proof_id: *proof_id });
            }
        }

        Ok(())
    }

    pub fn check_all_substates_in_scope(&self, value: &IndexedWellKnownTypes) -> Result<(), RuntimeError> {
        let scope = self.current_call_scope()?;

        for id in value.referenced_substates() {
            // You are allowed to reference existing root substates
            if id.is_root() {
                if !self.substate_exists(&id)? {
                    return Err(RuntimeError::RootSubstateNotFound { id: id.clone() });
                }
            } else if !scope.is_substate_in_scope(&id) {
                if !self.substate_exists(&id)? {
                    return Err(RuntimeError::ReferencedSubstateNotFound { id: id.clone() });
                }
                return Err(RuntimeError::SubstateOutOfScope { id: id.clone() });
            } else {
                // OK
            }
        }
        for bucket_id in value.bucket_ids() {
            if !scope.is_bucket_in_scope(*bucket_id) {
                return Err(RuntimeError::ValidationFailedBucketNotInScope { bucket_id: *bucket_id });
            }
        }
        for proof_id in value.proof_ids() {
            if !scope.is_proof_in_scope(*proof_id) {
                return Err(RuntimeError::ValidationFailedProofNotInScope { proof_id: *proof_id });
            }
        }

        Ok(())
    }

    pub fn buckets(&self) -> &HashMap<BucketId, Bucket> {
        &self.buckets
    }

    pub fn proofs(&self) -> &HashMap<ProofId, Proof> {
        &self.proofs
    }

    pub fn push_log(&mut self, log: LogEntry) {
        self.logs.push(log);
    }

    pub fn take_logs(&mut self) -> Vec<LogEntry> {
        mem::take(&mut self.logs)
    }

    pub fn push_event(&mut self, event: Event) {
        self.events.push(event);
    }

    pub fn take_events(&mut self) -> Vec<Event> {
        mem::take(&mut self.events)
    }

    pub fn events(&self) -> &[Event] {
        &self.events
    }

    pub fn logs(&self) -> &[LogEntry] {
        &self.logs
    }

    pub fn generate_substate_diff(
        &self,
        transaction_receipt: TransactionReceipt,
        substates_to_persist: IndexMap<SubstateId, SubstateValue>,
    ) -> Result<SubstateDiff, RuntimeError> {
        let mut substate_diff = SubstateDiff::new();

        for (address, substate) in substates_to_persist {
            let new_substate = match self.store.get_unmodified_substate(&address).optional()? {
                Some(existing_state) => {
                    substate_diff.down(address.clone(), existing_state.version());
                    if substate.as_validator_fee_pool().is_some_and(|fee| fee.amount.is_zero()) {
                        // If there are no fees left, do not up the fee pool
                        continue;
                    }
                    Substate::new(existing_state.version() + 1, substate)
                },
                None => Substate::new(0, substate),
            };
            substate_diff.up(address, new_substate);
        }

        // Special case: unclaimed confidential outputs are downed without being upped if claimed
        for claimed in &self.claimed_confidential_outputs {
            substate_diff.down(SubstateId::UnclaimedConfidentialOutput(*claimed), 0);
        }

        substate_diff.up(
            SubstateId::TransactionReceipt(transaction_receipt.transaction_hash.into()),
            Substate::new(0, SubstateValue::TransactionReceipt(transaction_receipt)),
        );

        Ok(substate_diff)
    }

    pub fn store(&self) -> &WorkingStateStore {
        &self.store
    }

    pub fn check_component_scope<T: Into<ActionIdent>>(
        &self,
        address: &SubstateId,
        action: T,
    ) -> Result<(), RuntimeError> {
        // Since we dont propagate _owned_ substate references up the call stack, if the substate is in scope, then it
        // was created in this scope and therefore owned.
        if self.current_call_scope()?.is_substate_in_scope(address) {
            return Ok(());
        }

        let component_lock = self
            .current_call_scope()?
            .get_current_component_lock()
            .ok_or(RuntimeError::NotInComponentContext { action: action.into() })?;

        let component = self.get_component(component_lock)?;
        if !component.contains_substate(address)? {
            warn!(
                target: LOG_TARGET,
                "Component {} attempted access to {} that is does not own",
                component_lock.address(),
                address
            );
            return Err(RuntimeError::SubstateNotOwned {
                id: address.clone(),
                requested_owner: Box::new(component_lock.address().clone()),
            });
        }

        Ok(())
    }
}
