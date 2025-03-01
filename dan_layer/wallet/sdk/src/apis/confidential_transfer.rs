//   Copyright 2023 The Tari Project
//   SPDX-License-Identifier: BSD-3-Clause

use std::cmp;

use digest::crypto_common::rand_core::OsRng;
use log::*;
use tari_bor::{Deserialize, Serialize};
use tari_common_types::types::{PrivateKey, PublicKey};
use tari_crypto::keys::PublicKey as _;
use tari_dan_common_types::{
    optional::{IsNotFoundError, Optional},
    substate_type::SubstateType,
    SubstateRequirement,
};
use tari_dan_wallet_crypto::{ConfidentialOutputMaskAndValue, ConfidentialProofStatement};
use tari_engine_types::{
    component::new_component_address_from_public_key,
    indexed_value::IndexedWellKnownTypes,
    substate::SubstateId,
};
use tari_template_builtin::ACCOUNT_TEMPLATE_ADDRESS;
use tari_template_lib::{
    args,
    constants::CONFIDENTIAL_TARI_RESOURCE_ADDRESS,
    models::{Amount, ComponentAddress, ResourceAddress, VaultId},
};
use tari_transaction::Transaction;

use crate::{
    apis::{
        accounts::{AccountsApi, AccountsApiError},
        confidential_crypto::{ConfidentialCryptoApi, ConfidentialCryptoApiError},
        confidential_outputs::{ConfidentialOutputsApi, ConfidentialOutputsApiError},
        config::{ConfigApi, ConfigApiError},
        key_manager,
        key_manager::{KeyManagerApi, KeyManagerApiError},
        substate::{SubstateApiError, SubstatesApi, ValidatorScanResult},
    },
    models::{ConfidentialOutputModel, ConfidentialProofId, OutputStatus},
    network::WalletNetworkInterface,
    storage::{WalletStorageError, WalletStore},
};

const LOG_TARGET: &str = "tari::dan::wallet_sdk::apis::confidential_transfers";

pub struct ConfidentialTransferApi<'a, TStore, TNetworkInterface> {
    key_manager_api: KeyManagerApi<'a, TStore>,
    accounts_api: AccountsApi<'a, TStore>,
    outputs_api: ConfidentialOutputsApi<'a, TStore>,
    substate_api: SubstatesApi<'a, TStore, TNetworkInterface>,
    crypto_api: ConfidentialCryptoApi,
    config_api: ConfigApi<'a, TStore>,
}

impl<'a, TStore, TNetworkInterface> ConfidentialTransferApi<'a, TStore, TNetworkInterface>
where
    TStore: WalletStore,
    TNetworkInterface: WalletNetworkInterface,
    TNetworkInterface::Error: IsNotFoundError,
{
    pub fn new(
        key_manager_api: KeyManagerApi<'a, TStore>,
        accounts_api: AccountsApi<'a, TStore>,
        outputs_api: ConfidentialOutputsApi<'a, TStore>,
        substate_api: SubstatesApi<'a, TStore, TNetworkInterface>,
        crypto_api: ConfidentialCryptoApi,
        config_api: ConfigApi<'a, TStore>,
    ) -> Self {
        Self {
            key_manager_api,
            accounts_api,
            outputs_api,
            substate_api,
            crypto_api,
            config_api,
        }
    }

    #[allow(clippy::too_many_lines)]
    fn resolved_inputs_for_transfer(
        &self,
        from_account: ComponentAddress,
        resource_address: ResourceAddress,
        spend_amount: Amount,
        input_selection: ConfidentialTransferInputSelection,
    ) -> Result<InputsToSpend, ConfidentialTransferApiError> {
        let src_vault = self
            .accounts_api
            .get_vault_by_resource(&from_account.into(), &resource_address)?;

        let available_revealed_funds = src_vault.available_revealed_balance();

        let proof_id = self.outputs_api.add_proof(&src_vault.address)?;

        match &input_selection {
            ConfidentialTransferInputSelection::ConfidentialOnly => {
                let (confidential_inputs, _) =
                    self.outputs_api
                        .lock_outputs_by_amount(&src_vault.address, spend_amount, proof_id)?;
                let confidential_inputs = self
                    .outputs_api
                    .resolve_output_masks(confidential_inputs, key_manager::TRANSACTION_BRANCH)?;

                info!(
                    target: LOG_TARGET,
                    "ConfidentialOnly: Locked {} confidential inputs for transfer from {}",
                    confidential_inputs.len(),
                    src_vault.address,
                );

                Ok(InputsToSpend {
                    confidential: confidential_inputs,
                    proof_id,
                    revealed: Amount::zero(),
                })
            },
            ConfidentialTransferInputSelection::RevealedOnly => {
                if available_revealed_funds < spend_amount {
                    return Err(ConfidentialTransferApiError::InsufficientFunds);
                }

                self.outputs_api.lock_revealed_funds(proof_id, spend_amount)?;

                info!(
                    target: LOG_TARGET,
                    "RevealedOnly: Spending {} revealed balance for transfer from {}",
                    spend_amount,
                    src_vault.address,
                );

                Ok(InputsToSpend {
                    confidential: vec![],
                    proof_id,
                    revealed: spend_amount,
                })
            },
            ConfidentialTransferInputSelection::PreferRevealed => {
                let revealed_to_spend = cmp::min(src_vault.revealed_balance, spend_amount);
                let confidential_to_spend = spend_amount - revealed_to_spend;
                if confidential_to_spend.is_zero() {
                    info!(
                        target: LOG_TARGET,
                        "PreferRevealed: Spending {} revealed balance for transfer from {}",
                        revealed_to_spend,
                        src_vault.address,
                    );

                    self.outputs_api.lock_revealed_funds(proof_id, revealed_to_spend)?;

                    return Ok(InputsToSpend {
                        confidential: vec![],
                        proof_id,
                        revealed: revealed_to_spend,
                    });
                }

                let proof_id = self.outputs_api.add_proof(&src_vault.address)?;
                let (confidential_inputs, _) =
                    self.outputs_api
                        .lock_outputs_by_amount(&src_vault.address, confidential_to_spend, proof_id)?;
                let confidential_inputs = self
                    .outputs_api
                    .resolve_output_masks(confidential_inputs, key_manager::TRANSACTION_BRANCH)?;

                let total_confidential_spent = confidential_inputs.iter().map(|i| i.value).sum::<u64>();

                self.outputs_api.lock_revealed_funds(proof_id, revealed_to_spend)?;

                info!(
                    target: LOG_TARGET,
                    "PreferRevealed: Locked {} confidential inputs (target: {}, spent: {}) and {} revealed for amount {} from {}",
                    confidential_inputs.len(),
                    confidential_to_spend,
                    total_confidential_spent,
                    revealed_to_spend,
                    spend_amount,
                    src_vault.address,
                );

                Ok(InputsToSpend {
                    confidential: confidential_inputs,
                    proof_id,
                    revealed: revealed_to_spend,
                })
            },
            ConfidentialTransferInputSelection::PreferConfidential => {
                let proof_id = self.outputs_api.add_proof(&src_vault.address)?;
                let (confidential_inputs, amount_locked) =
                    self.outputs_api
                        .lock_outputs_until_partial_amount(&src_vault.address, spend_amount, proof_id)?;

                let revealed_to_spend =
                    spend_amount.saturating_sub_positive(amount_locked.try_into().map_err(|_| {
                        ConfidentialTransferApiError::InvalidParameter {
                            param: "transfer_param",
                            reason: "Attempt to spend more than Amount::MAX".to_string(),
                        }
                    })?);

                if src_vault.revealed_balance < revealed_to_spend {
                    return Err(ConfidentialTransferApiError::InsufficientFunds);
                }

                self.outputs_api.lock_revealed_funds(proof_id, revealed_to_spend)?;

                let confidential_inputs = self
                    .outputs_api
                    .resolve_output_masks(confidential_inputs, key_manager::TRANSACTION_BRANCH)?;

                Ok(InputsToSpend {
                    confidential: confidential_inputs,
                    proof_id,
                    revealed: revealed_to_spend,
                })
            },
        }
    }

    async fn resolve_destination_account(
        &self,
        destination_pk: &PublicKey,
    ) -> Result<AccountDetails, ConfidentialTransferApiError> {
        let account_component = new_component_address_from_public_key(&ACCOUNT_TEMPLATE_ADDRESS, destination_pk);
        // Is it an account we own?
        if let Some(vaults) = self
            .accounts_api
            .get_vaults_by_account(&account_component.into())
            .optional()?
        {
            return Ok(AccountDetails {
                address: account_component,
                vaults: vaults
                    .into_iter()
                    .map(|v| v.address.as_vault_id().expect("BUG: not vault id"))
                    .collect(),
                exists: true,
            });
        }

        match self
            .substate_api
            .scan_for_substate(&account_component.into(), None)
            .await
            .optional()?
        {
            Some(ValidatorScanResult { address, substate, .. }) => {
                let indexed_component = substate
                    .component()
                    .map(|c| IndexedWellKnownTypes::from_value(c.state()))
                    .transpose()
                    .map_err(|e| ConfidentialTransferApiError::UnexpectedIndexerResponse {
                        details: format!("Failed to extract vaults from account component: {e}"),
                    })?
                    .ok_or_else(|| ConfidentialTransferApiError::UnexpectedIndexerResponse {
                        details: format!(
                            "Expected indexer to return a component for account {} but it returned {} (value type: {})",
                            account_component,
                            address,
                            SubstateType::from(&substate)
                        ),
                    })?;
                Ok(AccountDetails {
                    address: account_component,
                    vaults: indexed_component.vault_ids().to_vec(),
                    exists: true,
                })
            },
            None => Ok(AccountDetails {
                address: account_component,
                vaults: vec![],
                exists: false,
            }),
        }
    }

    #[allow(clippy::too_many_lines)]
    pub async fn transfer(&self, params: TransferParams) -> Result<TransferOutput, ConfidentialTransferApiError> {
        let from_account = self.accounts_api.get_account_by_address(&params.from_account.into())?;
        let to_account = self.resolve_destination_account(&params.destination_public_key).await?;
        let from_account_address = from_account.address.as_component_address().unwrap();

        // Determine Transaction Inputs
        let mut inputs = Vec::new();

        let dest_account_exists = to_account.exists;
        if dest_account_exists {
            inputs.push(SubstateRequirement::unversioned(to_account.address));
            inputs.extend(to_account.vaults.into_iter().map(SubstateRequirement::unversioned))
        }

        let account = self.accounts_api.get_account_by_address(&params.from_account.into())?;
        let account_substate = self.substate_api.get_substate(&params.from_account.into())?;
        inputs.push(account_substate.substate_id.into_unversioned_requirement());

        // Add all versioned account child addresses as inputs
        let child_addresses = self.substate_api.load_dependent_substates(&[&account.address])?;
        inputs.extend(child_addresses.into_iter().map(|a| a.into_unversioned()));

        let src_vault = self
            .accounts_api
            .get_vault_by_resource(&account.address, &params.resource_address)?;
        let src_vault_substate = self.substate_api.get_substate(&src_vault.address)?;
        inputs.push(src_vault_substate.substate_id.into_unversioned_requirement());

        // add the input for the resource address to be transferred
        inputs.push(SubstateRequirement::unversioned(params.resource_address));

        // We need to fetch the resource substate to check if there is a view key present.
        let resource_substate = self
            .substate_api
            .scan_for_substate(&SubstateId::Resource(params.resource_address), None)
            .await?;

        if let Some(ref resource_address) = params.proof_from_resource {
            inputs.push(SubstateRequirement::unversioned(*resource_address));
        }

        // Reserve and lock input funds for fees
        let fee_inputs_to_spend = self.resolved_inputs_for_transfer(
            from_account_address,
            CONFIDENTIAL_TARI_RESOURCE_ADDRESS,
            params.max_fee,
            ConfidentialTransferInputSelection::PreferRevealed,
        )?;

        let account_secret = self
            .key_manager_api
            .derive_key(key_manager::TRANSACTION_BRANCH, account.key_index)?;
        let account_public_key = PublicKey::from_secret_key(&account_secret.key);

        // Generate fee proof
        let fee_not_paid_by_revealed = params
            .max_fee
            .checked_sub_positive(fee_inputs_to_spend.revealed)
            .expect("BUG: PreferRevealed did not pay <= the max_fee in revealed fees");
        let confidential_change = fee_inputs_to_spend.total_confidential_amount() - fee_not_paid_by_revealed;
        let maybe_fee_change_statement = if confidential_change.is_zero() {
            // No change necessary
            None
        } else {
            let statement = self.create_confidential_proof_statement(&account_public_key, confidential_change, None)?;

            self.outputs_api.add_output(ConfidentialOutputModel {
                account_address: account.address.clone(),
                vault_address: src_vault.address.clone(),
                commitment: statement.to_commitment(),
                value: confidential_change.as_u64_checked().unwrap(),
                sender_public_nonce: Some(statement.sender_public_nonce.clone()),
                encryption_secret_key_index: account_secret.key_index,
                encrypted_data: statement.encrypted_data.clone(),
                public_asset_tag: None,
                // TODO: We could technically spend this output in the main transaction, however, we cannot mark it
                //       as unspent e.g. in the case of tx failure. We should allow spending of LockedUnconfirmed if
                //       the locking transaction is the same.
                status: OutputStatus::LockedUnconfirmed,
                locked_by_proof: Some(fee_inputs_to_spend.proof_id),
            })?;

            Some(statement)
        };

        let fee_withdraw_proof = self.crypto_api.generate_withdraw_proof(
            fee_inputs_to_spend.confidential.as_slice(),
            fee_inputs_to_spend.revealed,
            None,
            params.max_fee,
            maybe_fee_change_statement.as_ref(),
            // We always withdraw the exact amount of revealed required
            Amount::zero(),
        )?;

        // Reserve and lock input funds
        // TODO: preserve atomicity across api calls - needed in many places
        let inputs_to_spend = match self.resolved_inputs_for_transfer(
            params.from_account,
            params.resource_address,
            params.amount,
            params.input_selection,
        ) {
            Ok(inputs) => inputs,
            Err(e) => {
                warn!(target: LOG_TARGET, "Unlocking fee fund locks after error: {}", e);
                // This is a hack that addresses the case where input locking fails after the fee transaction. However
                // any error after this point do not undo locking. This is a limitation of the current
                // design - the db transaction should be passed in and automatically rolled back on error.
                if let Err(err) = self.outputs_api.release_revealed_funds(fee_inputs_to_spend.proof_id) {
                    error!(
                        target: LOG_TARGET,
                        "Failed to unlock revealed funds for transfer: {}",
                        err
                    );
                }
                if let Err(err) = self.outputs_api.release_proof_outputs(fee_inputs_to_spend.proof_id) {
                    error!(
                        target: LOG_TARGET,
                        "Failed to release fee inputs for transfer: {}",
                        err
                    );
                }

                return Err(e);
            },
        };

        // Generate outputs
        let resource_view_key = resource_substate
            .substate
            .as_resource()
            .ok_or_else(|| ConfidentialTransferApiError::UnexpectedIndexerResponse {
                details: format!(
                    "Expected indexer to return resource for address {}. It returned {}",
                    params.resource_address, resource_substate.address
                ),
            })?
            .view_key()
            .cloned();

        let output_statement = self.create_confidential_proof_statement(
            &params.destination_public_key,
            params.confidential_amount(),
            resource_view_key.clone(),
        )?;

        let remaining_left_to_pay = params
            .amount
            .checked_sub_positive(inputs_to_spend.revealed)
            .unwrap_or_else(|| {
                panic!(
                    "BUG: paid more revealed funds ({}) than the amount to pay ({})",
                    inputs_to_spend.revealed, params.amount
                )
            });
        let change_confidential_amount = inputs_to_spend.total_confidential_amount() - remaining_left_to_pay;

        let maybe_change_statement = if change_confidential_amount.is_zero() {
            None
        } else {
            let statement = self.create_confidential_proof_statement(
                &account_public_key,
                change_confidential_amount,
                resource_view_key,
            )?;

            let change_value = statement.amount.as_u64_checked().unwrap();

            if !statement.amount.is_zero() {
                self.outputs_api.add_output(ConfidentialOutputModel {
                    account_address: account.address,
                    vault_address: src_vault.address,
                    commitment: statement.to_commitment(),
                    value: change_value,
                    sender_public_nonce: Some(statement.sender_public_nonce.clone()),
                    encryption_secret_key_index: account_secret.key_index,
                    encrypted_data: statement.encrypted_data.clone(),
                    public_asset_tag: None,
                    status: OutputStatus::LockedUnconfirmed,
                    locked_by_proof: Some(inputs_to_spend.proof_id),
                })?;
            }

            Some(statement)
        };

        let proof = self.crypto_api.generate_withdraw_proof(
            &inputs_to_spend.confidential,
            inputs_to_spend.revealed,
            Some(&output_statement).filter(|o| !o.amount.is_zero()),
            params.revealed_amount(),
            maybe_change_statement.as_ref(),
            Amount::zero(),
        )?;

        let network = self.config_api.get_network()?;
        let transaction = Transaction::builder()
            .for_network(network.as_byte())
            .fee_transaction_pay_from_component_confidential(from_account_address, fee_withdraw_proof)
            .then(|builder| {
                if dest_account_exists {
                    builder
                } else {
                    builder.create_account(params.destination_public_key.clone())
                }
            })
            .then(|builder| {
                if let Some(ref badge) = params.proof_from_resource {
                    builder
                        .call_method(from_account_address, "create_proof_for_resource", args![badge])
                        .put_last_instruction_output_on_workspace("proof")
                } else {
                    builder
                }
            })
            .call_method(from_account_address, "withdraw_confidential", args![
                params.resource_address,
                proof
            ])
            .put_last_instruction_output_on_workspace("bucket")
            .call_method(to_account.address, "deposit", args![Workspace("bucket")])
            .then(|builder| {
                if params.proof_from_resource.is_some() {
                    builder.drop_all_proofs_in_workspace()
                } else {
                    builder
                }
            })
            .with_inputs(inputs)
            .build_and_seal(&account_secret.key);

        self.outputs_api
            .proofs_set_transaction_hash(inputs_to_spend.proof_id, *transaction.id())?;
        self.outputs_api
            .proofs_set_transaction_hash(fee_inputs_to_spend.proof_id, *transaction.id())?;

        Ok(TransferOutput {
            transaction,
            autofill_inputs: vec![],
            fee_transaction_proof_id: Some(fee_inputs_to_spend.proof_id),
            transaction_proof_id: Some(inputs_to_spend.proof_id),
        })
    }

    fn create_confidential_proof_statement(
        &self,
        dest_public_key: &PublicKey,
        confidential_amount: Amount,
        resource_view_key: Option<PublicKey>,
    ) -> Result<ConfidentialProofStatement, ConfidentialTransferApiError> {
        let mask = if confidential_amount.is_zero() {
            PrivateKey::default()
        } else {
            self.key_manager_api.next_key(key_manager::TRANSACTION_BRANCH)?.key
        };

        let (nonce, public_nonce) = PublicKey::random_keypair(&mut OsRng);
        let encrypted_data = self.crypto_api.encrypt_value_and_mask(
            confidential_amount
                .as_u64_checked()
                .unwrap_or_else(|| panic!("BUG: confidential_amount {} is negative", confidential_amount)),
            &mask,
            dest_public_key,
            &nonce,
        )?;

        Ok(ConfidentialProofStatement {
            amount: confidential_amount,
            mask,
            sender_public_nonce: public_nonce,
            encrypted_data,
            minimum_value_promise: 0,
            resource_view_key,
        })
    }
}

pub struct TransferOutput {
    pub transaction: Transaction,
    pub autofill_inputs: Vec<SubstateRequirement>,
    pub fee_transaction_proof_id: Option<ConfidentialProofId>,
    pub transaction_proof_id: Option<ConfidentialProofId>,
}

#[derive(Debug)]
pub struct TransferParams {
    /// Spend from this account
    pub from_account: ComponentAddress,
    /// Strategy for input selection
    pub input_selection: ConfidentialTransferInputSelection,
    /// Amount to spend to destination
    pub amount: Amount,
    /// Destination public key used to derive the destination account component
    pub destination_public_key: PublicKey,
    /// Address of the resource to transfer
    pub resource_address: ResourceAddress,
    /// Fee to lock for the transaction
    pub max_fee: Amount,
    /// If true, the output will contain only a revealed amount. Otherwise, only confidential amounts.
    pub output_to_revealed: bool,
    /// If some, instructions are added that create a access rule proof for this resource before calling withdraw
    pub proof_from_resource: Option<ResourceAddress>,
    /// Run as a dry run, no funds will be transferred if true
    pub is_dry_run: bool,
}

impl TransferParams {
    pub fn confidential_amount(&self) -> Amount {
        if self.output_to_revealed {
            Amount::zero()
        } else {
            self.amount
        }
    }

    pub fn revealed_amount(&self) -> Amount {
        if self.output_to_revealed {
            self.amount
        } else {
            Amount::zero()
        }
    }
}

impl TransferParams {
    pub fn total_amount(&self) -> Amount {
        self.amount + self.max_fee
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[cfg_attr(
    feature = "ts",
    derive(ts_rs::TS),
    ts(export, export_to = "../../bindings/src/types/")
)]
pub enum ConfidentialTransferInputSelection {
    ConfidentialOnly,
    RevealedOnly,
    PreferRevealed,
    PreferConfidential,
}

#[derive(Debug)]
pub struct InputsToSpend {
    pub confidential: Vec<ConfidentialOutputMaskAndValue>,
    pub proof_id: ConfidentialProofId,
    pub revealed: Amount,
}

impl InputsToSpend {
    pub fn total_amount(&self) -> Amount {
        self.total_confidential_amount() + self.revealed
    }

    pub fn total_confidential_amount(&self) -> Amount {
        let confidential_amt = self.confidential.iter().map(|o| o.value).sum::<u64>();
        Amount::try_from(confidential_amt).unwrap()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfidentialTransferApiError {
    #[error("Store error: {0}")]
    StoreError(#[from] WalletStorageError),
    #[error("Confidential crypto error: {0}")]
    ConfidentialCrypto(#[from] ConfidentialCryptoApiError),
    #[error("Confidential outputs error: {0}")]
    OutputsApi(#[from] ConfidentialOutputsApiError),
    #[error("Substate API error: {0}")]
    SubstateApi(#[from] SubstateApiError),
    #[error("Insufficient funds")]
    InsufficientFunds,
    #[error("Key manager error: {0}")]
    KeyManager(#[from] KeyManagerApiError),
    #[error("Accounts API error: {0}")]
    Accounts(#[from] AccountsApiError),
    #[error("Invalid parameter `{param}`: {reason}")]
    InvalidParameter { param: &'static str, reason: String },
    #[error("Unexpected indexer response: {details}")]
    UnexpectedIndexerResponse { details: String },
    #[error("Config API error: {0}")]
    ConfigApi(#[from] ConfigApiError),
}

impl IsNotFoundError for ConfidentialTransferApiError {
    fn is_not_found_error(&self) -> bool {
        matches!(self, Self::StoreError(e) if e.is_not_found_error() )
    }
}

pub struct AccountDetails {
    pub address: ComponentAddress,
    pub vaults: Vec<VaultId>,
    pub exists: bool,
}
