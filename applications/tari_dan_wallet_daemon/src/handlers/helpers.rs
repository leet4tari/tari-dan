//   Copyright 2023 The Tari Project
//   SPDX-License-Identifier: BSD-3-Clause

use std::{collections::HashSet, fmt::Display};

use tari_dan_common_types::{optional::Optional, SubstateRequirement};
use tari_dan_wallet_sdk::{
    apis::accounts::{AccountsApi, AccountsApiError},
    models::Account,
    DanWalletSdk,
};
use tari_dan_wallet_storage_sqlite::SqliteWalletStore;
use tari_engine_types::substate::SubstateId;
use tari_transaction::{Transaction, TransactionBuilder, TransactionId};
use tari_wallet_daemon_client::ComponentAddressOrName;
use tokio::sync::broadcast;

use crate::{
    handlers::HandlerContext,
    indexer_jrpc_impl::IndexerJsonRpcNetworkInterface,
    jrpc_server::ApplicationErrorCode,
    services::{TransactionFinalizedEvent, WalletEvent},
};

pub async fn wait_for_result(
    events: &mut broadcast::Receiver<WalletEvent>,
    transaction_id: TransactionId,
) -> Result<TransactionFinalizedEvent, anyhow::Error> {
    loop {
        let wallet_event = events.recv().await?;
        match wallet_event {
            WalletEvent::TransactionFinalized(event) if event.transaction_id == transaction_id => return Ok(event),
            WalletEvent::TransactionInvalid(event) if event.transaction_id == transaction_id => {
                return Err(anyhow::anyhow!(
                    "Transaction invalid: {} [status: {}]",
                    event
                        .finalize
                        .and_then(|finalize| finalize.reject().cloned())
                        .map(|f| f.to_string())
                        .unwrap_or_else(|| "Unknown".to_string()),
                    event.status,
                ));
            },
            _ => {},
        }
    }
}

pub async fn wait_for_result_and_account(
    events: &mut broadcast::Receiver<WalletEvent>,
    transaction_id: &TransactionId,
    account_address: &SubstateId,
) -> Result<(TransactionFinalizedEvent, Option<SubstateId>), anyhow::Error> {
    let mut maybe_account = None;
    let mut maybe_result = None;
    loop {
        let wallet_event = events.recv().await?;
        match wallet_event {
            WalletEvent::TransactionFinalized(event) if event.transaction_id == *transaction_id => {
                maybe_result = Some(event);
            },
            WalletEvent::TransactionInvalid(event) if event.transaction_id == *transaction_id => {
                return Err(anyhow::anyhow!(
                    "Transaction invalid: {} [status: {}]",
                    event
                        .finalize
                        .and_then(|finalize| finalize.reject().cloned())
                        .map(|f| f.to_string())
                        .unwrap_or_else(|| "Unknown".to_string()),
                    event.status,
                ));
            },
            WalletEvent::AccountCreated(event) if event.account.address == *account_address => {
                maybe_account = Some(event.account.address);
            },
            WalletEvent::AccountChanged(event) if event.account_address == *account_address => {
                maybe_account = Some(event.account_address);
            },
            _ => {},
        }
        if let Some(ref result) = maybe_result {
            // If accept, we wait for the account. If reject we return immediately
            if (result.finalize.result.is_accept() && maybe_account.is_some()) || result.finalize.result.is_reject() {
                return Ok((maybe_result.unwrap(), maybe_account));
            }
        }
    }
}

pub fn get_account_with_inputs(
    account: Option<ComponentAddressOrName>,
    sdk: &DanWalletSdk<SqliteWalletStore, IndexerJsonRpcNetworkInterface>,
) -> Result<(Account, HashSet<SubstateRequirement>), anyhow::Error> {
    let account = get_account_or_default(account, &sdk.accounts_api())?;

    // Add all versioned account child addresses as inputs
    let inputs = sdk.substate_api().load_dependent_substates(&[&account.address])?;

    Ok((account, inputs))
}

pub fn get_account<TStore>(
    account: &ComponentAddressOrName,
    accounts_api: &AccountsApi<'_, TStore>,
) -> Result<Account, AccountsApiError>
where
    TStore: tari_dan_wallet_sdk::storage::WalletStore,
{
    match account {
        ComponentAddressOrName::ComponentAddress(address) => {
            Ok(accounts_api.get_account_by_address(&(*address).into())?)
        },
        ComponentAddressOrName::Name(name) => Ok(accounts_api.get_account_by_name(name)?),
    }
}

pub fn get_account_or_default<T>(
    account: Option<ComponentAddressOrName>,
    accounts_api: &AccountsApi<'_, T>,
) -> Result<Account, anyhow::Error>
where
    T: tari_dan_wallet_sdk::storage::WalletStore,
{
    let result;
    if let Some(a) = account {
        result = get_account(&a, accounts_api)?;
    } else {
        result = accounts_api
            .get_default()
            .optional()?
            .ok_or_else(|| not_found("No default account found. Please create an account."))?;
    }
    Ok(result)
}

pub(super) fn invalid_params<T: Display>(field: &str, details: Option<T>) -> anyhow::Error {
    axum_jrpc::error::JsonRpcError::new(
        axum_jrpc::error::JsonRpcErrorReason::InvalidParams,
        format!(
            "Invalid param '{}'{}",
            field,
            details.map(|d| format!(": {}", d)).unwrap_or_default()
        ),
        serde_json::Value::Null,
    )
    .into()
}

pub(super) fn application<T: Display>(code: ApplicationErrorCode, details: T) -> anyhow::Error {
    axum_jrpc::error::JsonRpcError::new(
        axum_jrpc::error::JsonRpcErrorReason::ApplicationError(code as i32),
        format!("Application error: '{details}",),
        serde_json::Value::Null,
    )
    .into()
}
pub(super) fn not_found<T: Display>(details: T) -> anyhow::Error {
    axum_jrpc::error::JsonRpcError::new(
        axum_jrpc::error::JsonRpcErrorReason::ApplicationError(ApplicationErrorCode::NotFound as i32),
        format!("Not found: {details}",),
        serde_json::Value::Null,
    )
    .into()
}

/// Returns a TransactionBuilder with the current network configured.
pub fn transaction_builder(context: &HandlerContext) -> TransactionBuilder {
    Transaction::builder().for_network(context.config().network.as_byte())
}
