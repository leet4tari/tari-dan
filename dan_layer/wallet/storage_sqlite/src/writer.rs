//   Copyright 2023 The Tari Project
//   SPDX-License-Identifier: BSD-3-Clause

use std::{
    collections::HashSet,
    ops::{Add, Deref, DerefMut, Sub},
    str::FromStr,
    sync::MutexGuard,
    time::Duration,
};

use chrono::NaiveDateTime;
use diesel::{OptionalExtension, QueryDsl, RunQueryDsl, SqliteConnection};
use log::*;
use serde::Serialize;
use tari_bor::json_encoding::CborValueJsonSerializeWrapper;
use tari_common_types::types::{Commitment, PublicKey};
use tari_dan_common_types::{SubstateRequirement, VersionedSubstateId};
use tari_dan_storage::consensus_models::QuorumCertificate;
use tari_dan_wallet_sdk::{
    models::{
        ConfidentialOutputModel,
        ConfidentialProofId,
        NewAccountInfo,
        NonFungibleToken,
        OutputStatus,
        SubstateModel,
        TransactionStatus,
        VaultModel,
    },
    storage::{WalletStorageError, WalletStoreReader, WalletStoreWriter},
};
use tari_engine_types::{commit_result::FinalizeResult, substate::SubstateId, TemplateAddress};
use tari_template_lib::models::{Amount, EncryptedData};
use tari_transaction::{Transaction, TransactionId};
use tari_utilities::hex::Hex;

use crate::{
    diesel::ExpressionMethods,
    models::{self},
    reader::ReadTransaction,
    serialization::serialize_json,
};

const LOG_TARGET: &str = "auth::tari::dan::wallet_sdk::storage_sqlite::writer";

pub struct WriteTransaction<'a> {
    /// In SQLite any transaction is writable. We keep a ReadTransaction to satisfy the Deref requirement of the
    /// WalletStore.
    transaction: ReadTransaction<'a>,
}

impl<'a> WriteTransaction<'a> {
    pub fn new(connection: MutexGuard<'a, SqliteConnection>) -> Self {
        Self {
            transaction: ReadTransaction::new(connection),
        }
    }

    fn get_proof(&mut self, proof_id: ConfidentialProofId) -> Result<models::Proof, WalletStorageError> {
        use crate::schema::proofs;

        proofs::table
            .filter(proofs::id.eq(proof_id as i32))
            .first(self.connection())
            .map_err(|e| WalletStorageError::general("get_proof", e))
    }
}

impl WalletStoreWriter for WriteTransaction<'_> {
    fn commit(mut self) -> Result<(), WalletStorageError> {
        self.transaction.commit()?;
        Ok(())
    }

    fn rollback(mut self) -> Result<(), WalletStorageError> {
        self.transaction.rollback()?;
        Ok(())
    }

    fn jwt_add_empty_token(&mut self) -> Result<u64, WalletStorageError> {
        use crate::schema::auth_status;

        diesel::insert_into(auth_status::table)
            .values((auth_status::user_decided.eq(false), auth_status::granted.eq(false)))
            .execute(self.connection())
            .map_err(|e| WalletStorageError::general("jwt_add_empty_token", e))?;
        let last_inserted_id: i32 =
            diesel::select(diesel::dsl::sql::<diesel::sql_types::Integer>("last_insert_rowid()"))
                .get_result(self.connection())
                .unwrap();
        Ok(last_inserted_id as u64)
    }

    fn jwt_store_decision(&mut self, id: u64, permissions_token: Option<String>) -> Result<(), WalletStorageError> {
        use crate::schema::auth_status;
        // let values = match token {
        //     Some(token) => (auth_status::user_decided.eq(true),auth_status::granted.eq(true),auth_status::token)
        // }
        diesel::update(auth_status::table)
            .set((
                auth_status::user_decided.eq(true),
                auth_status::granted.eq(permissions_token.is_some()),
                permissions_token.map(|token| auth_status::token.eq(token)),
            ))
            .filter(auth_status::id.eq(id as i32))
            .execute(self.connection())
            .map_err(|e| WalletStorageError::general("jwt_store_decision", e))?;
        Ok(())
    }

    fn jwt_is_revoked(&mut self, token: &str) -> Result<bool, WalletStorageError> {
        use crate::schema::auth_status;
        let revoked = auth_status::table
            .select(auth_status::revoked)
            .filter(auth_status::token.eq(token))
            .first(self.connection())
            .optional()
            .map_err(|e| WalletStorageError::general("jwt_is_revoked", e))?;
        match revoked {
            Some(revoked) => Ok(revoked),
            None => {
                // We don't know this token. Store it as not revoked. Weirdly if the token is used with different daemon
                // it will work even if it's revoked in this one. But since the user will need to confirm any actions
                // there should be no security issue.
                diesel::insert_into(auth_status::table)
                    .values((
                        auth_status::granted.eq(true),
                        auth_status::user_decided.eq(true),
                        auth_status::token.eq(token),
                    ))
                    .execute(self.connection())
                    .map_err(|e| WalletStorageError::general("jwt_is_revoked", e))?;
                Ok(false)
            },
        }
    }

    fn jwt_revoke(&mut self, token_id: i32) -> Result<(), WalletStorageError> {
        use crate::schema::auth_status;
        if diesel::update(auth_status::table)
            .set(auth_status::revoked.eq(true))
            .filter(auth_status::id.eq(token_id))
            .execute(self.connection())
            .map_err(|e| WalletStorageError::general("jwt_revoke", e))? ==
            0
        {
            diesel::insert_into(auth_status::table)
                .values((auth_status::revoked.eq(true), auth_status::id.eq(token_id)))
                .execute(self.connection())
                .map_err(|e| WalletStorageError::general("jwt_revoke", e))?;
        }
        Ok(())
    }

    // -------------------------------- KeyManager -------------------------------- //

    fn key_manager_insert(&mut self, branch: &str, index: u64) -> Result<(), WalletStorageError> {
        use crate::schema::key_manager_states;
        let index =
            i64::try_from(index).map_err(|_| WalletStorageError::general("key_manager_insert", "index is negative"))?;
        let count = key_manager_states::table
            .select(key_manager_states::id)
            .filter(key_manager_states::branch_seed.eq(branch))
            .limit(1)
            .count()
            .first::<i64>(self.connection())
            .map_err(|e| WalletStorageError::general("key_manager_insert", e))?;

        // Set active if this is the only key branch
        let is_active = count == 0;

        let value_set = (
            key_manager_states::branch_seed.eq(branch),
            key_manager_states::index.eq(index),
            key_manager_states::is_active.eq(is_active),
        );

        diesel::insert_into(key_manager_states::table)
            .values(value_set)
            .execute(self.connection())
            .map_err(|e| WalletStorageError::general("key_manager_insert", e))?;

        Ok(())
    }

    fn key_manager_set_active_index(&mut self, branch: &str, index: u64) -> Result<(), WalletStorageError> {
        use crate::schema::key_manager_states;
        let index = i64::try_from(index)
            .map_err(|_| WalletStorageError::general("key_manager_set_active_index", "index too large"))?;

        let active_id = key_manager_states::table
            .select(key_manager_states::id)
            .filter(key_manager_states::branch_seed.eq(branch))
            .filter(key_manager_states::index.eq(index))
            .limit(1)
            .first::<i32>(self.connection())
            .optional()
            .map_err(|e| WalletStorageError::general("key_manager_set_active_index", e))?
            .ok_or_else(|| WalletStorageError::NotFound {
                operation: "key_manager_set_active_index",
                entity: "key_manager_states".to_string(),
                key: format!("branch = {}, index = {}", branch, index),
            })?;

        diesel::update(key_manager_states::table)
            .set((
                key_manager_states::is_active.eq(false),
                key_manager_states::updated_at.eq(diesel::dsl::now),
            ))
            .filter(key_manager_states::branch_seed.eq(branch))
            .filter(key_manager_states::is_active.eq(true))
            .execute(self.connection())
            .map_err(|e| WalletStorageError::general("key_manager_set_active_index", e))?;

        diesel::update(key_manager_states::table)
            .set((
                key_manager_states::is_active.eq(true),
                key_manager_states::updated_at.eq(diesel::dsl::now),
            ))
            .filter(key_manager_states::id.eq(active_id))
            .execute(self.connection())
            .map_err(|e| WalletStorageError::general("key_manager_set_active_index", e))?;

        Ok(())
    }

    // -------------------------------- Config -------------------------------- //

    fn config_set<T: Serialize + ?Sized>(
        &mut self,
        key: &str,
        value: &T,
        is_encrypted: bool,
    ) -> Result<(), WalletStorageError> {
        use crate::schema::config;

        let exists = config::table
            .filter(config::key.eq(key))
            .limit(1)
            .count()
            .get_result(self.connection())
            .map(|count: i64| count > 0)
            .map_err(|e| WalletStorageError::general("key_manager_set_index", e))?;

        if exists {
            diesel::update(config::table)
                .set((
                    config::value.eq(serialize_json(value)?),
                    config::is_encrypted.eq(is_encrypted),
                    config::updated_at.eq(diesel::dsl::now),
                ))
                .filter(config::key.eq(key))
                .execute(self.connection())
                .map_err(|e| WalletStorageError::general("key_manager_set_index", e))?;
        } else {
            diesel::insert_into(config::table)
                .values((
                    config::key.eq(key),
                    config::value.eq(serialize_json(value)?),
                    config::is_encrypted.eq(is_encrypted),
                ))
                .execute(self.connection())
                .map_err(|e| WalletStorageError::general("key_manager_set_index", e))?;
        }

        Ok(())
    }

    // -------------------------------- Transactions -------------------------------- //
    fn transactions_insert(
        &mut self,
        transaction: &Transaction,
        required_substates: &[SubstateRequirement],
        new_account_info: Option<&NewAccountInfo>,
        is_dry_run: bool,
    ) -> Result<(), WalletStorageError> {
        use crate::schema::transactions;

        diesel::insert_into(transactions::table)
            .values((
                transactions::hash.eq(transaction.id().to_string()),
                transactions::network.eq(i32::from(transaction.network())),
                transactions::fee_instructions.eq(serialize_json(transaction.fee_instructions())?),
                transactions::instructions.eq(serialize_json(transaction.instructions())?),
                transactions::signatures.eq(serialize_json(transaction.signatures())?),
                transactions::seal_signature.eq(serialize_json(transaction.seal_signature())?),
                transactions::is_seal_signer_authorized.eq(transaction.is_seal_signer_authorized()),
                transactions::inputs.eq(serialize_json(transaction.inputs())?),
                transactions::status.eq(TransactionStatus::New.as_key_str()),
                transactions::required_substates.eq(serialize_json(&required_substates)?),
                transactions::new_account_info.eq(new_account_info.map(serialize_json).transpose()?),
                transactions::dry_run.eq(is_dry_run),
            ))
            .execute(self.connection())
            .map_err(|e| WalletStorageError::general("transactions_insert", e))?;

        Ok(())
    }

    fn transactions_set_result_and_status(
        &mut self,
        transaction_id: TransactionId,
        result: Option<&FinalizeResult>,
        final_fee: Option<Amount>,
        qcs: Option<&[QuorumCertificate]>,
        new_status: TransactionStatus,
        execution_time: Option<Duration>,
        finalized_time: Option<Duration>,
    ) -> Result<(), WalletStorageError> {
        use crate::schema::transactions;

        let num_rows = diesel::update(transactions::table)
            .set((
                transactions::result.eq(result.map(serialize_json).transpose()?),
                transactions::status.eq(new_status.as_key_str()),
                transactions::final_fee.eq(final_fee.map(|v| v.value())),
                transactions::qcs.eq(qcs.map(serialize_json).transpose()?),
                transactions::executed_time_ms
                    .eq(execution_time.map(|v| i64::try_from(v.as_millis()).unwrap_or(i64::MAX))),
                transactions::finalized_time_ms
                    .eq(finalized_time.map(|v| i64::try_from(v.as_millis()).unwrap_or(i64::MAX))),
                transactions::updated_at.eq(diesel::dsl::now),
            ))
            .filter(transactions::hash.eq(transaction_id.to_string()))
            .execute(self.connection())
            .map_err(|e| WalletStorageError::general("transactions_set_result_and_status", e))?;

        if num_rows == 0 {
            return Err(WalletStorageError::NotFound {
                operation: "transactions_set_result_and_status",
                entity: "transaction".to_string(),
                key: transaction_id.to_string(),
            });
        }

        Ok(())
    }

    // -------------------------------- Substates -------------------------------- //
    fn substates_upsert_root(
        &mut self,
        transaction_id: TransactionId,
        address: VersionedSubstateId,
        referenced_substates: HashSet<SubstateId>,
        module_name: Option<String>,
        template_addr: Option<TemplateAddress>,
    ) -> Result<(), WalletStorageError> {
        use crate::schema::substates;

        diesel::insert_into(substates::table)
            .values((
                substates::address.eq(address.substate_id().to_string()),
                substates::transaction_hash.eq(transaction_id.to_string()),
                substates::module_name.eq(&module_name),
                substates::template_address.eq(template_addr.map(|a| a.to_string())),
                substates::referenced_substates.eq(serialize_json(&referenced_substates)?),
                substates::version.eq(address.version() as i32),
            ))
            .on_conflict(substates::address)
            .do_update()
            .set((
                substates::transaction_hash.eq(transaction_id.to_string()),
                substates::module_name.eq(&module_name),
                substates::template_address.eq(template_addr.map(|a| a.to_string())),
                substates::referenced_substates.eq(serialize_json(&referenced_substates)?),
                substates::version.eq(address.version() as i32),
            ))
            .execute(self.connection())
            .map_err(|e| WalletStorageError::general("substates_upsert_root", e))?;

        Ok(())
    }

    fn substates_upsert_child(
        &mut self,
        transaction_id: TransactionId,
        parent: SubstateId,
        child: VersionedSubstateId,
        referenced_substates: HashSet<SubstateId>,
    ) -> Result<(), WalletStorageError> {
        use crate::schema::substates;

        diesel::insert_into(substates::table)
            .values((
                substates::address.eq(child.substate_id().to_string()),
                substates::transaction_hash.eq(transaction_id.to_string()),
                substates::parent_address.eq(Some(parent.to_string())),
                substates::referenced_substates.eq(serialize_json(&referenced_substates)?),
                substates::version.eq(child.version() as i32),
            ))
            .on_conflict(substates::address)
            .do_update()
            .set((
                substates::transaction_hash.eq(transaction_id.to_string()),
                substates::parent_address.eq(Some(parent.to_string())),
                substates::referenced_substates.eq(serialize_json(&referenced_substates)?),
                substates::version.eq(child.version() as i32),
            ))
            .execute(self.connection())
            .map_err(|e| WalletStorageError::general("substates_upsert_child", e))?;

        Ok(())
    }

    fn substates_remove(&mut self, substate_addr: &SubstateId) -> Result<SubstateModel, WalletStorageError> {
        use crate::schema::substates;

        let substate = self.transaction.substates_get(substate_addr)?;
        let num_rows = diesel::delete(substates::table)
            .filter(substates::address.eq(substate_addr.to_string()))
            .execute(self.connection())
            .map_err(|e| WalletStorageError::general("substates_remove", e))?;

        if num_rows == 0 {
            return Err(WalletStorageError::NotFound {
                operation: "substates_remove",
                entity: "substate".to_string(),
                key: substate.substate_id.to_string(),
            });
        }

        Ok(substate)
    }

    // -------------------------------- Accounts -------------------------------- //

    fn accounts_set_default(&mut self, address: &SubstateId) -> Result<(), WalletStorageError> {
        use crate::schema::accounts;

        diesel::update(accounts::table)
            .set(accounts::is_default.eq(false))
            .execute(self.connection())
            .map_err(|e| WalletStorageError::general("accounts_set_default clear previous default", e))?;

        let num_rows = diesel::update(accounts::table)
            .set(accounts::is_default.eq(true))
            .filter(accounts::address.eq(address.to_string()))
            .execute(self.connection())
            .map_err(|e| WalletStorageError::general("accounts_set_default", e))?;

        if num_rows == 0 {
            return Err(WalletStorageError::NotFound {
                operation: "accounts_set_default",
                entity: "account".to_string(),
                key: address.to_string(),
            });
        }

        Ok(())
    }

    fn accounts_insert(
        &mut self,
        account_name: Option<&str>,
        address: &SubstateId,
        owner_key_index: u64,
        is_default: bool,
    ) -> Result<(), WalletStorageError> {
        use crate::schema::accounts;

        if is_default {
            diesel::update(accounts::table)
                .set(crate::schema::accounts::is_default.eq(false))
                .execute(self.connection())
                .map_err(|e| WalletStorageError::general("accounts_insert clear previous default", e))?;
        }

        diesel::insert_into(accounts::table)
            .values((
                accounts::name.eq(account_name),
                accounts::address.eq(address.to_string()),
                accounts::owner_key_index.eq(owner_key_index as i64),
                accounts::is_default.eq(is_default),
            ))
            .execute(self.connection())
            .map_err(|e| WalletStorageError::general("accounts_insert", e))?;

        Ok(())
    }

    fn accounts_update(&mut self, address: &SubstateId, new_name: Option<&str>) -> Result<(), WalletStorageError> {
        use crate::schema::accounts;

        let changeset = (new_name.map(|n| accounts::name.eq(n)),);

        let num_rows = diesel::update(accounts::table)
            .set(changeset)
            .filter(accounts::address.eq(address.to_string()))
            .execute(self.connection())
            .map_err(|e| WalletStorageError::general("accounts_update", e))?;

        if num_rows == 0 {
            return Err(WalletStorageError::NotFound {
                operation: "accounts_update",
                entity: "account".to_string(),
                key: address.to_string(),
            });
        }

        Ok(())
    }

    fn vaults_insert(&mut self, vault: VaultModel) -> Result<(), WalletStorageError> {
        use crate::schema::{accounts, vaults};

        let account_id = accounts::table
            .select(accounts::id)
            .filter(accounts::address.eq(vault.account_address.to_string()))
            .first::<i32>(self.connection())
            .map_err(|e| WalletStorageError::general("vaults_insert", e))?;

        let values = (
            vaults::account_id.eq(account_id),
            vaults::address.eq(vault.address.to_string()),
            vaults::revealed_balance.eq(vault.revealed_balance.value()),
            vaults::confidential_balance.eq(vault.confidential_balance.value()),
            vaults::resource_address.eq(vault.resource_address.to_string()),
            vaults::resource_type.eq(format!("{:?}", vault.resource_type)),
            vaults::token_symbol.eq(vault.token_symbol),
        );
        diesel::insert_into(vaults::table)
            .values(values)
            .execute(self.connection())
            .map_err(|e| WalletStorageError::general("vaults_insert", e))?;

        Ok(())
    }

    fn vaults_update(
        &mut self,
        vault_address: &SubstateId,
        revealed_balance: Amount,
        confidential_balance: Amount,
    ) -> Result<(), WalletStorageError> {
        use crate::schema::vaults;

        let changeset = (
            vaults::revealed_balance.eq(revealed_balance.value()),
            vaults::confidential_balance.eq(confidential_balance.value()),
        );

        let num_rows = diesel::update(vaults::table)
            .set(changeset)
            .filter(vaults::address.eq(vault_address.to_string()))
            .execute(self.connection())
            .map_err(|e| WalletStorageError::general("vaults_update", e))?;

        if num_rows == 0 {
            return Err(WalletStorageError::NotFound {
                operation: "vaults_update",
                entity: "vault".to_string(),
                key: vault_address.to_string(),
            });
        }

        Ok(())
    }

    fn vaults_lock_revealed_funds(
        &mut self,
        proof_id: ConfidentialProofId,
        amount_to_lock: Amount,
    ) -> Result<(), WalletStorageError> {
        use crate::schema::{proofs, vaults};

        let changeset = proofs::locked_revealed_amount.eq(proofs::locked_revealed_amount.add(amount_to_lock.value()));

        let num_rows = diesel::update(proofs::table)
            .set(changeset)
            .filter(proofs::id.eq(proof_id as i32))
            .execute(self.connection())
            .map_err(|e| WalletStorageError::general("vaults_lock_revealed_funds", e))?;

        if num_rows == 0 {
            return Err(WalletStorageError::NotFound {
                operation: "vaults_lock_revealed_funds",
                entity: "proof".to_string(),
                key: proof_id.to_string(),
            });
        }

        let proof = self.get_proof(proof_id)?;

        let changeset = vaults::locked_revealed_balance.eq(vaults::locked_revealed_balance.add(amount_to_lock.value()));

        let num_rows = diesel::update(vaults::table)
            .set(changeset)
            .filter(vaults::id.eq(proof.vault_id))
            .execute(self.connection())
            .map_err(|e| WalletStorageError::general("vaults_lock_revealed_funds", e))?;

        if num_rows == 0 {
            return Err(WalletStorageError::NotFound {
                operation: "vaults_lock_revealed_funds",
                entity: "vault".to_string(),
                key: proof.vault_id.to_string(),
            });
        }

        Ok(())
    }

    fn vaults_finalized_locked_revealed_funds(
        &mut self,
        proof_id: ConfidentialProofId,
    ) -> Result<(), WalletStorageError> {
        use crate::schema::vaults;

        let proof = self.get_proof(proof_id)?;

        let changeset = (
            vaults::revealed_balance.eq(vaults::revealed_balance.sub(proof.locked_revealed_amount)),
            vaults::locked_revealed_balance.eq(vaults::locked_revealed_balance.sub(proof.locked_revealed_amount)),
        );

        let num_rows = diesel::update(vaults::table)
            .set(changeset)
            .filter(vaults::id.eq(proof.vault_id))
            .execute(self.connection())
            .map_err(|e| WalletStorageError::general("vaults_finalized_locked_funds", e))?;

        if num_rows == 0 {
            return Err(WalletStorageError::NotFound {
                operation: "vaults_finalized_locked_funds",
                entity: "vault".to_string(),
                key: proof.vault_id.to_string(),
            });
        }

        Ok(())
    }

    fn vaults_unlock_revealed_funds(&mut self, proof_id: ConfidentialProofId) -> Result<(), WalletStorageError> {
        use crate::schema::vaults;

        let proof = self.get_proof(proof_id)?;

        let changeset =
            vaults::locked_revealed_balance.eq(vaults::locked_revealed_balance.sub(proof.locked_revealed_amount));

        let num_rows = diesel::update(vaults::table)
            .set(changeset)
            .filter(vaults::id.eq(proof.vault_id))
            .execute(self.connection())
            .map_err(|e| WalletStorageError::general("vaults_unlock_revealed_funds", e))?;

        if num_rows == 0 {
            return Err(WalletStorageError::NotFound {
                operation: "vaults_unlock_revealed_funds",
                entity: "vault".to_string(),
                key: proof.vault_id.to_string(),
            });
        }

        Ok(())
    }

    // -------------------------------- Outputs -------------------------------- //

    fn outputs_lock_smallest_amount(
        &mut self,
        vault_address: &SubstateId,
        locked_by_proof: ConfidentialProofId,
    ) -> Result<ConfidentialOutputModel, WalletStorageError> {
        use crate::schema::{accounts, outputs, vaults};

        let vault_id = vaults::table
            .select(vaults::id)
            .filter(vaults::address.eq(vault_address.to_string()))
            .first::<i32>(self.connection())
            .map_err(|e| WalletStorageError::general("outputs_lock_smallest_amount", e))?;

        let locked_output = outputs::table
            .filter(outputs::vault_id.eq(vault_id))
            .filter(outputs::status.eq(OutputStatus::Unspent.as_key_str()))
            .order_by(outputs::value.asc())
            .first::<models::ConfidentialOutput>(self.connection())
            .optional()
            .map_err(|e| WalletStorageError::general("outputs_lock_smallest_amount", e))?;

        let locked_output = locked_output.ok_or_else(|| WalletStorageError::NotFound {
            operation: "outputs_lock_smallest_amount",
            entity: "output".to_string(),
            key: format!("vault={}, locked_by_proof={}", vault_address, locked_by_proof),
        })?;

        let account_address = accounts::table
            .select(accounts::address)
            .filter(accounts::id.eq(locked_output.account_id))
            .first::<String>(self.connection())
            .map_err(|e| WalletStorageError::general("outputs_lock_smallest_amount", e))?;

        let changeset = (
            outputs::status.eq(OutputStatus::Locked.as_key_str()),
            outputs::locked_by_proof.eq(locked_by_proof as i32),
            outputs::locked_at.eq(diesel::dsl::now),
        );
        diesel::update(outputs::table)
            .set(changeset)
            .filter(outputs::id.eq(locked_output.id))
            .execute(self.connection())
            .map_err(|e| WalletStorageError::general("outputs_lock_smallest_amount", e))?;

        Ok(ConfidentialOutputModel {
            account_address: SubstateId::from_str(&account_address).map_err(|e| WalletStorageError::DecodingError {
                operation: "outputs_lock_smallest_amount",
                item: "account address",
                details: e.to_string(),
            })?,
            vault_address: vault_address.clone(),
            commitment: Commitment::from_hex(&locked_output.commitment).map_err(|_| {
                WalletStorageError::DecodingError {
                    operation: "outputs_lock_smallest_amount",
                    item: "output commitment",
                    details: "Corrupt db: invalid hex representation".to_string(),
                }
            })?,
            value: locked_output.value as u64,
            sender_public_nonce: locked_output
                .sender_public_nonce
                .map(|nonce| PublicKey::from_hex(&nonce).unwrap()),
            encryption_secret_key_index: locked_output.encryption_secret_key_index as u64,
            encrypted_data: EncryptedData::try_from(locked_output.encrypted_data).map_err(|len| {
                WalletStorageError::DecodingError {
                    operation: "outputs_lock_smallest_amount",
                    item: "encrypted data",
                    details: format!("Corrupt db: invalid encrypted data length {len}"),
                }
            })?,
            public_asset_tag: None,
            status: OutputStatus::Locked,
            locked_by_proof: Some(locked_by_proof),
        })
    }

    fn outputs_insert(&mut self, output: ConfidentialOutputModel) -> Result<(), WalletStorageError> {
        use crate::schema::{accounts, outputs, vaults};

        let account_id = accounts::table
            .select(accounts::id)
            .filter(accounts::address.eq(&output.account_address.to_string()))
            .first::<i32>(self.connection())
            .map_err(|e| WalletStorageError::general("outputs_insert", e))?;

        let vault_id = vaults::table
            .select(vaults::id)
            .filter(vaults::address.eq(&output.vault_address.to_string()))
            .first::<i32>(self.connection())
            .map_err(|e| WalletStorageError::general("outputs_insert", e))?;

        diesel::insert_into(outputs::table)
            .values((
                outputs::account_id.eq(account_id),
                outputs::vault_id.eq(vault_id),
                outputs::commitment.eq(output.commitment.to_hex()),
                outputs::value.eq(output.value as i64),
                outputs::sender_public_nonce.eq(output.sender_public_nonce.map(|pk| pk.to_hex())),
                outputs::encryption_secret_key_index.eq(output.encryption_secret_key_index as i64),
                outputs::encrypted_data.eq(output.encrypted_data.as_ref()),
                outputs::status.eq(output.status.as_key_str()),
                outputs::locked_by_proof.eq(output.locked_by_proof.map(|v| v as i32)),
            ))
            .execute(self.connection())
            .map_err(|e| WalletStorageError::general("outputs_insert", e))?;

        Ok(())
    }

    fn outputs_finalize_by_proof_id(&mut self, proof_id: ConfidentialProofId) -> Result<(), WalletStorageError> {
        use crate::schema::outputs;

        // Unlock locked unconfirmed outputs
        diesel::update(outputs::table)
            .filter(outputs::locked_by_proof.eq(proof_id as i32))
            .filter(outputs::status.eq(OutputStatus::LockedUnconfirmed.as_key_str()))
            .set((
                outputs::status.eq(OutputStatus::Unspent.as_key_str()),
                outputs::locked_by_proof.eq::<Option<i32>>(None),
                outputs::locked_at.eq::<Option<NaiveDateTime>>(None),
            ))
            .execute(self.connection())
            .map_err(|e| WalletStorageError::general("outputs_finalize_by_proof_id", e))?;

        // Mark locked outputs as spent
        diesel::update(outputs::table)
            .filter(outputs::locked_by_proof.eq(proof_id as i32))
            .filter(outputs::status.eq(OutputStatus::Locked.as_key_str()))
            .set((
                outputs::status.eq(OutputStatus::Spent.as_key_str()),
                outputs::locked_by_proof.eq::<Option<i32>>(None),
                outputs::locked_at.eq::<Option<NaiveDateTime>>(None),
            ))
            .execute(self.connection())
            .map_err(|e| WalletStorageError::general("outputs_finalize_by_proof_id", e))?;

        Ok(())
    }

    fn outputs_release_by_proof_id(&mut self, proof_id: ConfidentialProofId) -> Result<(), WalletStorageError> {
        use crate::schema::outputs;

        // Unlock locked unspent outputs
        diesel::update(outputs::table)
            .filter(outputs::locked_by_proof.eq(proof_id as i32))
            .filter(outputs::status.eq(OutputStatus::Locked.as_key_str()))
            .set((
                outputs::status.eq(OutputStatus::Unspent.as_key_str()),
                outputs::locked_by_proof.eq::<Option<i32>>(None),
                outputs::locked_at.eq::<Option<NaiveDateTime>>(None),
            ))
            .execute(self.connection())
            .map_err(|e| WalletStorageError::general("outputs_unlock_by_proof_id", e))?;

        // Remove outputs that were created by this proof
        diesel::delete(outputs::table)
            .filter(outputs::status.eq(OutputStatus::LockedUnconfirmed.as_key_str()))
            .filter(outputs::locked_by_proof.eq(proof_id as i32))
            .execute(self.connection())
            .map_err(|e| WalletStorageError::general("outputs_unlock_by_proof_id", e))?;

        Ok(())
    }

    // Proofs
    fn proofs_insert(&mut self, vault_address: &SubstateId) -> Result<ConfidentialProofId, WalletStorageError> {
        use crate::schema::{proofs, vaults};

        let (vault_id, account_id) = vaults::table
            .select((vaults::id, vaults::account_id))
            .filter(vaults::address.eq(vault_address.to_string()))
            .first::<(i32, i32)>(self.connection())
            .map_err(|e| WalletStorageError::general("proof_insert", e))?;

        diesel::insert_into(proofs::table)
            .values((proofs::account_id.eq(account_id), proofs::vault_id.eq(vault_id)))
            .execute(self.connection())
            .map_err(|e| WalletStorageError::general("proof_insert", e))?;

        // RETURNING only available from SQLite 3.35 https://www.sqlite.org/lang_returning.html
        // TODO: See if we can upgrade SQLite
        let proof_id = proofs::table
            .select(proofs::id)
            .order_by(proofs::id.desc())
            .first::<i32>(self.connection())
            .map_err(|e| WalletStorageError::general("proof_insert", e))?;

        Ok(proof_id as ConfidentialProofId)
    }

    fn proofs_delete(&mut self, proof_id: ConfidentialProofId) -> Result<(), WalletStorageError> {
        use crate::schema::proofs;

        diesel::delete(proofs::table.filter(proofs::id.eq(proof_id as i32)))
            .execute(self.connection())
            .map_err(|e| WalletStorageError::general("proof_delete", e))?;

        Ok(())
    }

    fn proofs_set_transaction_id(
        &mut self,
        proof_id: ConfidentialProofId,
        transaction_id: TransactionId,
    ) -> Result<(), WalletStorageError> {
        use crate::schema::proofs;

        diesel::update(proofs::table.filter(proofs::id.eq(proof_id as i32)))
            .set(proofs::transaction_hash.eq(transaction_id.to_string()))
            .execute(self.connection())
            .map_err(|e| WalletStorageError::general("proofs_set_transaction_hash", e))?;

        Ok(())
    }

    // -------------------------------- Non fungible tokens -------------------------------- //
    fn non_fungible_token_upsert(&mut self, non_fungible_token: &NonFungibleToken) -> Result<(), WalletStorageError> {
        use crate::schema::{non_fungible_tokens, vaults};

        info!(
            target: "tari::dan::wallet_daemon::account_monitor",
            "Inserting new non fungible token with id = {}", non_fungible_token.nft_id
        );

        let data = serde_json::to_string(&CborValueJsonSerializeWrapper(&non_fungible_token.data)).map_err(|e| {
            WalletStorageError::DecodingError {
                operation: "non_fungible_token_upsert",
                item: "non_fungible_tokens.data",
                details: e.to_string(),
            }
        })?;

        let mutable_data = serde_json::to_string(&CborValueJsonSerializeWrapper(&non_fungible_token.mutable_data))
            .map_err(|e| WalletStorageError::DecodingError {
                operation: "non_fungible_token_upsert",
                item: "non_fungible_tokens.mutable_data",
                details: e.to_string(),
            })?;

        let vault_id = vaults::table
            .select(vaults::id)
            .filter(vaults::address.eq(non_fungible_token.vault_id.to_string()))
            .first::<i32>(self.connection())
            .map_err(|e| WalletStorageError::general("proof_insert", e))?;

        diesel::insert_into(non_fungible_tokens::table)
            .values((
                non_fungible_tokens::nft_id.eq(non_fungible_token.nft_id.to_canonical_string()),
                non_fungible_tokens::data.eq(&data),
                non_fungible_tokens::mutable_data.eq(&mutable_data),
                non_fungible_tokens::vault_id.eq(vault_id),
                non_fungible_tokens::is_burned.eq(non_fungible_token.is_burned),
            ))
            .on_conflict(non_fungible_tokens::nft_id)
            .do_update()
            .set((
                non_fungible_tokens::data.eq(&data),
                non_fungible_tokens::mutable_data.eq(&mutable_data),
                non_fungible_tokens::vault_id.eq(vault_id),
                non_fungible_tokens::is_burned.eq(non_fungible_token.is_burned),
            ))
            .execute(self.connection())
            .map_err(|e| WalletStorageError::general("non_fungible_token_upsert", e))?;

        info!(
            target: LOG_TARGET,
            "Inserted successfully new non fungible token with id = {}", non_fungible_token.nft_id
        );
        Ok(())
    }
}

impl Drop for WriteTransaction<'_> {
    fn drop(&mut self) {
        if !self.transaction.is_done() {
            warn!(target: LOG_TARGET, "WriteTransaction was not committed or rolled back");
            if let Err(err) = self.transaction.rollback() {
                warn!(target: LOG_TARGET, "Failed to rollback WriteTransaction: {}", err);
            }
        }
    }
}

impl<'a> Deref for WriteTransaction<'a> {
    type Target = ReadTransaction<'a>;

    fn deref(&self) -> &Self::Target {
        &self.transaction
    }
}

impl DerefMut for WriteTransaction<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.transaction
    }
}
