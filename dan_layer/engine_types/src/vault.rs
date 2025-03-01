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

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use tari_common_types::types::{Commitment, PrivateKey, PublicKey};
use tari_template_lib::{
    crypto::PedersonCommitmentBytes,
    models::{Amount, ConfidentialWithdrawProof, NonFungibleId, ResourceAddress, VaultId},
    prelude::ResourceType,
};

use crate::{
    bucket::Bucket,
    confidential::{ConfidentialOutput, ElgamalVerifiableBalance, ValueLookupTable},
    proof::{ContainerRef, LockedResource, Proof},
    resource_container::{ResourceContainer, ResourceError},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(
    feature = "ts",
    derive(ts_rs::TS),
    ts(export, export_to = "../../bindings/src/types/")
)]
pub struct Vault {
    resource_container: ResourceContainer,
}

impl Vault {
    pub fn new(resource: ResourceContainer) -> Self {
        Self {
            resource_container: resource,
        }
    }

    pub fn deposit(&mut self, bucket: Bucket) -> Result<(), ResourceError> {
        self.resource_container.deposit(bucket.into_resource_container())?;
        Ok(())
    }

    pub fn withdraw(&mut self, amount: Amount) -> Result<ResourceContainer, ResourceError> {
        self.resource_container.withdraw(amount)
    }

    pub fn withdraw_non_fungibles(
        &mut self,
        ids: &BTreeSet<NonFungibleId>,
    ) -> Result<ResourceContainer, ResourceError> {
        self.resource_container.withdraw_by_ids(ids)
    }

    pub fn withdraw_confidential(
        &mut self,
        proof: ConfidentialWithdrawProof,
        view_key: Option<&PublicKey>,
    ) -> Result<ResourceContainer, ResourceError> {
        self.resource_container.withdraw_confidential(proof, view_key)
    }

    pub fn recall_all(&mut self) -> Result<ResourceContainer, ResourceError> {
        self.resource_container.recall_all()
    }

    pub fn recall_confidential(
        &mut self,
        commitments: BTreeSet<PedersonCommitmentBytes>,
        revealed_amount: Amount,
    ) -> Result<ResourceContainer, ResourceError> {
        self.resource_container
            .recall_confidential_commitments(commitments, revealed_amount)
    }

    pub fn balance(&self) -> Amount {
        self.resource_container.amount()
    }

    pub fn locked_balance(&self) -> Amount {
        self.resource_container.locked_amount()
    }

    pub fn get_commitment_count(&self) -> u32 {
        self.resource_container.get_commitment_count()
    }

    pub fn get_confidential_commitments(&self) -> Option<&BTreeMap<Commitment, ConfidentialOutput>> {
        self.resource_container.get_confidential_commitments()
    }

    pub fn try_brute_force_confidential_balance<I, TValueLookup>(
        &self,
        secret_view_key: &PrivateKey,
        value_range: I,
        value_lookup: &mut TValueLookup,
    ) -> Result<Option<u64>, TValueLookup::Error>
    where
        I: IntoIterator<Item = u64> + Clone,
        TValueLookup: ValueLookupTable,
    {
        let Some(utxos) = self.get_confidential_commitments() else {
            return Ok(None);
        };

        let balances = ElgamalVerifiableBalance::batched_brute_force(
            secret_view_key,
            value_range,
            value_lookup,
            utxos.values().filter_map(|utxo| utxo.viewable_balance.as_ref()),
        )?;

        // If any of the commitments cannot be brute forced, then we return None
        Ok(balances.into_iter().sum())
    }

    pub fn resource_address(&self) -> &ResourceAddress {
        self.resource_container.resource_address()
    }

    pub fn resource_type(&self) -> ResourceType {
        self.resource_container.resource_type()
    }

    pub fn get_non_fungible_ids(&self) -> &BTreeSet<NonFungibleId> {
        self.resource_container.non_fungible_token_ids()
    }

    pub fn reveal_confidential(
        &mut self,
        proof: ConfidentialWithdrawProof,
        view_key: Option<&PublicKey>,
    ) -> Result<ResourceContainer, ResourceError> {
        self.resource_container.reveal_confidential(proof, view_key)
    }

    pub fn resource_container_mut(&mut self) -> &mut ResourceContainer {
        &mut self.resource_container
    }

    pub fn lock_all(&mut self, vault_id: VaultId) -> Result<LockedResource, ResourceError> {
        let locked_resource = self.resource_container.lock_all()?;
        Ok(LockedResource::new(ContainerRef::Vault(vault_id), locked_resource))
    }

    pub fn lock_by_non_fungible_ids(
        &mut self,
        vault_id: VaultId,
        ids: BTreeSet<NonFungibleId>,
    ) -> Result<LockedResource, ResourceError> {
        let locked_resource = self.resource_container.lock_by_non_fungible_ids(ids)?;
        Ok(LockedResource::new(ContainerRef::Vault(vault_id), locked_resource))
    }

    pub fn lock_by_amount(&mut self, vault_id: VaultId, amount: Amount) -> Result<LockedResource, ResourceError> {
        let locked_resource = self.resource_container.lock_by_amount(amount)?;
        Ok(LockedResource::new(ContainerRef::Vault(vault_id), locked_resource))
    }

    pub fn unlock(&mut self, proof: Proof) -> Result<(), ResourceError> {
        self.resource_container.unlock(proof.into_resource_container())
    }
}
