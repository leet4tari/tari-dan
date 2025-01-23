//   Copyright 2023 The Tari Project
//   SPDX-License-Identifier: BSD-3-Clause

use anyhow::anyhow;
use log::*;
use tari_dan_wallet_sdk::apis::{jwt::JrpcPermission, key_manager};
use tari_engine_types::instruction::Instruction;
use tari_template_lib::args;
use tari_wallet_daemon_client::types::{
    ClaimValidatorFeesRequest,
    ClaimValidatorFeesResponse,
    GetValidatorFeesRequest,
    GetValidatorFeesResponse,
};

use crate::{
    handlers::{
        helpers::{get_account_with_inputs, transaction_builder, wait_for_result},
        HandlerContext,
    },
    DEFAULT_FEE,
};

const LOG_TARGET: &str = "tari::dan::walletd::handlers::validator";

pub async fn handle_get_validator_fees(
    _context: &HandlerContext,
    _token: Option<String>,
    _req: GetValidatorFeesRequest,
) -> Result<GetValidatorFeesResponse, anyhow::Error> {
    // TODO: We need to proxy certain requests (e.g fee summary) to the correct validators
    Err(anyhow!("Not implemented"))
}

pub async fn handle_claim_validator_fees(
    context: &HandlerContext,
    token: Option<String>,
    req: ClaimValidatorFeesRequest,
) -> Result<ClaimValidatorFeesResponse, anyhow::Error> {
    let sdk = context.wallet_sdk().clone();
    sdk.jwt_api().check_auth(token, &[JrpcPermission::Admin])?;

    let mut fee_instructions = vec![];

    let (account, inputs) = get_account_with_inputs(req.account, &sdk)?;
    let account_address = account.address.as_component_address().unwrap();

    // build the transaction
    let max_fee = req.max_fee.unwrap_or(DEFAULT_FEE);
    fee_instructions.extend([
        Instruction::ClaimValidatorFees {
            validator_public_key: req.validator_public_key.clone(),
            epoch: req.epoch.as_u64(),
        },
        Instruction::PutLastInstructionOutputOnWorkspace {
            key: b"claim_bucket".to_vec(),
        },
        Instruction::CallMethod {
            component_address: account_address,
            method: "deposit".to_string(),
            args: args![Workspace("claim_bucket")],
        },
        Instruction::CallMethod {
            component_address: account_address,
            method: "pay_fee".to_string(),
            args: args![max_fee],
        },
    ]);

    // TODO: At the moment fees can only be claimed by the account of the wallet.
    // In future we should change it to allow a separate public key
    let account_secret_key = sdk
        .key_manager_api()
        .derive_key(key_manager::TRANSACTION_BRANCH, account.key_index)?;

    let transaction = transaction_builder(context)
        .with_fee_instructions(fee_instructions)
        .with_inputs(inputs)
        .build_and_seal(&account_secret_key.key);

    // send the transaction
    if req.dry_run {
        let transaction = sdk
            .transaction_api()
            .submit_dry_run_transaction(transaction, vec![])
            .await?;
        return Ok(ClaimValidatorFeesResponse {
            transaction_id: *transaction.transaction.id(),
            fee: transaction
                .finalize
                .as_ref()
                .map(|f| f.fee_receipt.total_fees_paid)
                .unwrap_or_default(),
            result: transaction
                .finalize
                .ok_or_else(|| anyhow!("No finalize result for dry run transaction"))?,
        });
    }

    let mut events = context.notifier().subscribe();
    let tx_id = context
        .transaction_service()
        .submit_transaction(transaction, vec![])
        .await?;

    let finalized = wait_for_result(&mut events, tx_id).await?;

    if let Some(reject) = finalized.finalize.reject() {
        return Err(anyhow::anyhow!("Fee transaction rejected: {}", reject));
    }
    if let Some(reason) = finalized.finalize.full_reject() {
        return Err(anyhow::anyhow!(
            "Fee transaction succeeded (fees charged) however the transaction failed: {reason}",
        ));
    }
    info!(
        target: LOG_TARGET,
        "✅ Claim fee transaction {} finalized. Fee: {}",
        finalized.transaction_id,
        finalized.final_fee
    );

    Ok(ClaimValidatorFeesResponse {
        transaction_id: tx_id,
        fee: finalized.final_fee,
        result: finalized.finalize,
    })
}
