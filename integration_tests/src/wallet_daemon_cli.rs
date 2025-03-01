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

use std::{collections::HashMap, str::FromStr, time::Duration};

use anyhow::{anyhow, bail};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use serde_json::json;
use tari_crypto::{
    ristretto::{RistrettoPublicKey, RistrettoSecretKey},
    signatures::CommitmentSignature,
    tari_utilities::ByteArray,
};
use tari_dan_common_types::{Epoch, SubstateRequirement};
use tari_dan_wallet_sdk::{
    apis::confidential_transfer::ConfidentialTransferInputSelection,
    models::{Account, NonFungibleToken},
};
use tari_engine_types::substate::SubstateId;
use tari_template_lib::{
    args,
    constants::CONFIDENTIAL_TARI_RESOURCE_ADDRESS,
    models::Amount,
    prelude::{ComponentAddress, ResourceAddress},
    resource::TOKEN_SYMBOL,
};
use tari_transaction::UnsignedTransaction;
use tari_transaction_manifest::{parse_manifest, ManifestValue};
use tari_validator_node_cli::command::transaction::CliArg;
use tari_wallet_daemon_client::{
    error::WalletDaemonClientError,
    types::{
        AccountGetResponse,
        AccountsCreateFreeTestCoinsRequest,
        AccountsCreateRequest,
        AccountsGetBalancesRequest,
        AccountsTransferRequest,
        ClaimBurnRequest,
        ClaimBurnResponse,
        ClaimValidatorFeesRequest,
        ClaimValidatorFeesResponse,
        ConfidentialTransferRequest,
        ListAccountNftRequest,
        MintAccountNftRequest,
        ProofsGenerateRequest,
        RevealFundsRequest,
        TransactionSubmitRequest,
        TransactionWaitResultRequest,
        TransactionWaitResultResponse,
    },
    ComponentAddressOrName,
    WalletDaemonClient,
};
use tokio::{task::JoinSet, time::timeout};

use crate::{
    helpers::get_address_from_output,
    util::transaction_builder,
    validator_node_cli::add_substate_ids,
    TariWorld,
};

pub async fn claim_burn(
    world: &mut TariWorld,
    account_name: String,
    commitment: Vec<u8>,
    range_proof: Vec<u8>,
    ownership_proof: CommitmentSignature<RistrettoPublicKey, RistrettoSecretKey>,
    reciprocal_claim_public_key: RistrettoPublicKey,
    wallet_daemon_name: String,
    max_fee: i64,
) -> Result<ClaimBurnResponse, WalletDaemonClientError> {
    let mut client = get_auth_wallet_daemon_client(world, &wallet_daemon_name).await;

    let claim_burn_request = ClaimBurnRequest {
        account: Some(ComponentAddressOrName::Name(account_name.clone())),
        claim_proof: json!({
            "commitment": BASE64.encode(commitment.as_bytes()),
            "ownership_proof": {
                "public_nonce": BASE64.encode(ownership_proof.public_nonce().as_bytes()),
                "u": BASE64.encode(ownership_proof.u().as_bytes()),
                "v": BASE64.encode(ownership_proof.v().as_bytes())
            },
            "reciprocal_claim_public_key": BASE64.encode(reciprocal_claim_public_key.as_bytes()),
            "range_proof": BASE64.encode(range_proof.as_bytes()),
        }),
        max_fee: Some(Amount(max_fee)),
        key_id: None,
    };

    client.claim_burn(claim_burn_request).await
}

pub async fn claim_fees(
    world: &mut TariWorld,
    wallet_daemon_name: String,
    account_name: String,
    validator_name: String,
    dry_run: bool,
) -> Result<ClaimValidatorFeesResponse, WalletDaemonClientError> {
    let mut client = get_auth_wallet_daemon_client(world, &wallet_daemon_name).await;

    let vn = world.get_validator_node(&validator_name);

    let mut vn_client = vn.create_client();
    let stats = vn_client.get_epoch_manager_stats().await.unwrap();

    let request = ClaimValidatorFeesRequest {
        account: Some(ComponentAddressOrName::Name(account_name)),
        claim_key_index: None,
        max_fee: None,
        shards: vec![stats
            .committee_info
            .expect("claim_fees: committee_info is None")
            .shard_group()
            .start()],
        dry_run,
    };

    client.claim_validator_fees(request).await
}

pub async fn reveal_burned_funds(world: &mut TariWorld, account_name: String, amount: u64, wallet_daemon_name: String) {
    let mut client = get_auth_wallet_daemon_client(world, &wallet_daemon_name).await;

    let request = RevealFundsRequest {
        account: Some(ComponentAddressOrName::Name(account_name)),
        amount_to_reveal: Amount(amount as i64),
        max_fee: Some(Amount(2000)),
        pay_fee_from_reveal: true,
    };

    let resp = client
        .accounts_reveal_funds(request)
        .await
        .expect("Failed to request reveal funds");

    let wait_req = TransactionWaitResultRequest {
        transaction_id: resp.transaction_id,
        timeout_secs: Some(120),
    };
    let wait_resp = client.wait_transaction_result(wait_req).await.unwrap();
    assert!(wait_resp.result.unwrap().result.is_accept());
}

pub async fn transfer_confidential(
    world: &mut TariWorld,
    source_account_name: String,
    dest_account_name: String,
    amount: u64,
    wallet_daemon_name: String,
    outputs_name: String,
    min_epoch: Option<Epoch>,
    max_epoch: Option<Epoch>,
) -> tari_wallet_daemon_client::types::TransactionSubmitResponse {
    let mut client = get_auth_wallet_daemon_client(world, &wallet_daemon_name).await;

    let source_account_name = ComponentAddressOrName::Name(source_account_name);
    let AccountGetResponse { account, .. } = client.accounts_get(source_account_name.clone()).await.unwrap();
    let source_component_address = account
        .address
        .as_component_address()
        .expect("Invalid component address for source address");

    let signing_key_index = account.key_index;

    let dest_account_name = ComponentAddressOrName::Name(dest_account_name);
    let destination_account_resp = client
        .accounts_get(dest_account_name)
        .await
        .expect("Failed to retrieve destination account address from its name");

    let destination_account = destination_account_resp
        .account
        .address
        .as_component_address()
        .expect("Failed to get component address from destination account");
    let destination_public_key = destination_account_resp.public_key;

    let resource_address = CONFIDENTIAL_TARI_RESOURCE_ADDRESS;

    let create_transfer_proof_req = ProofsGenerateRequest {
        account: Some(source_account_name),
        amount: Amount(amount as i64),
        reveal_amount: Amount(0),
        resource_address,
        destination_public_key,
    };

    let transfer_proof_resp = client.create_transfer_proof(create_transfer_proof_req).await.unwrap();
    let withdraw_proof = transfer_proof_resp.proof;
    let proof_id = transfer_proof_resp.proof_id;

    let transaction = transaction_builder()
        .fee_transaction_pay_from_component(source_component_address, Amount(2000))
        .call_method(source_component_address, "withdraw_confidential", args![
            resource_address,
            withdraw_proof
        ])
        .put_last_instruction_output_on_workspace(b"bucket")
        .call_method(destination_account, "deposit", args![Variable("bucket")])
        .with_min_epoch(min_epoch)
        .with_max_epoch(max_epoch)
        .build_unsigned_transaction();

    let submit_req = TransactionSubmitRequest {
        transaction,
        signing_key_index: Some(signing_key_index),
        proof_ids: vec![proof_id],
        detect_inputs: true,
        detect_inputs_use_unversioned: true,
        autofill_inputs: vec![],
    };

    let submit_resp = client.submit_transaction(submit_req).await.unwrap();

    let wait_req = TransactionWaitResultRequest {
        transaction_id: submit_resp.transaction_id,
        timeout_secs: Some(120),
    };
    let wait_resp = client.wait_transaction_result(wait_req).await.unwrap();

    add_substate_ids(
        world,
        outputs_name,
        &wait_resp
            .result
            .expect("Transaction has timed out")
            .result
            .expect("Transaction has failed"),
    );

    submit_resp
}

pub async fn create_account(world: &mut TariWorld, account_name: String, wallet_daemon_name: String) {
    let mut client = get_auth_wallet_daemon_client(world, &wallet_daemon_name).await;

    let request = AccountsCreateRequest {
        account_name: Some(account_name.clone()),
        custom_access_rules: None,
        is_default: false,
        max_fee: None,
        key_id: None,
    };

    let resp = timeout(Duration::from_secs(240), client.create_account(request))
        .await
        .unwrap()
        .unwrap();

    // TODO: store the secret key in the world, but we don't have a need for it at the moment
    world.account_keys.insert(
        account_name.clone(),
        (RistrettoSecretKey::default(), resp.public_key.clone()),
    );

    add_substate_ids(
        world,
        account_name,
        &resp.result.result.expect("Failed to obtain substate diffs"),
    );
}

pub async fn create_account_with_free_coins(
    world: &mut TariWorld,
    account_name: String,
    wallet_daemon_name: String,
    amount: Amount,
    key_name: Option<String>,
) {
    let mut client = get_auth_wallet_daemon_client(world, &wallet_daemon_name).await;

    let key_index = key_name.map(|k| {
        *world
            .wallet_keys
            .get(&k)
            .unwrap_or_else(|| panic!("Wallet {} not found", wallet_daemon_name))
    });
    let request = AccountsCreateFreeTestCoinsRequest {
        account: Some(ComponentAddressOrName::Name(account_name.clone())),
        amount,
        max_fee: None,
        key_id: key_index,
    };

    let resp = client.create_free_test_coins(request).await.unwrap();
    // TODO: store the secret key in the world, but we don't have a need for it at the moment
    world.account_keys.insert(
        account_name.clone(),
        (RistrettoSecretKey::default(), resp.public_key.clone()),
    );
    let wait_req = TransactionWaitResultRequest {
        transaction_id: resp.result.transaction_hash.into_array().into(),
        timeout_secs: Some(120),
    };
    let _wait_resp = client.wait_transaction_result(wait_req).await.unwrap();

    add_substate_ids(
        world,
        account_name,
        &resp.result.result.expect("Failed to obtain substate diffs"),
    );
}

pub async fn mint_new_nft_on_account(
    world: &mut TariWorld,
    nft_name: String,
    account_name: String,
    wallet_daemon_name: String,
    existing_nft_component: Option<ComponentAddress>,
    metadata: Option<serde_json::Value>,
) {
    let mut client = get_auth_wallet_daemon_client(world, &wallet_daemon_name).await;

    let metadata = metadata.unwrap_or_else(|| {
        json!({
            TOKEN_SYMBOL: nft_name,
            "name": "TariProject",
            "departure": "Now",
            "landing_on": "Moon"
        })
    });

    let request = MintAccountNftRequest {
        account: ComponentAddressOrName::Name(account_name.clone()),
        metadata,
        mint_fee: Some(Amount::new(1_000)),
        create_account_nft_fee: None,
        existing_nft_component,
    };
    let resp = client
        .mint_account_nft(request)
        .await
        .expect("Failed to mint new account NFT");

    let wait_req = TransactionWaitResultRequest {
        transaction_id: resp.result.transaction_hash.into_array().into(),
        timeout_secs: Some(120),
    };
    let _wait_resp = client
        .wait_transaction_result(wait_req)
        .await
        .expect("Wait response failed");

    add_substate_ids(
        world,
        account_name,
        &resp.result.result.expect("Failed to obtain substate diffs"),
    );
}

pub async fn list_account_nfts(
    world: &mut TariWorld,
    account_name: String,
    wallet_daemon_name: String,
) -> Vec<NonFungibleToken> {
    let mut client = get_auth_wallet_daemon_client(world, &wallet_daemon_name).await;

    let request = ListAccountNftRequest {
        account: Some(ComponentAddressOrName::Name(account_name.clone())),
        limit: 100,
        offset: 0,
    };
    let submit_resp = client
        .list_account_nfts(request)
        .await
        .expect("Failed to list account NFTs");

    submit_resp.nfts
}

pub async fn get_balance(world: &mut TariWorld, account_name: &str, wallet_daemon_name: &str) -> i64 {
    let account_name = ComponentAddressOrName::Name(account_name.to_string());
    let get_balance_req = AccountsGetBalancesRequest {
        account: Some(account_name),
        refresh: true,
    };
    let mut client = get_auth_wallet_daemon_client(world, wallet_daemon_name).await;

    let resp = client
        .get_account_balances(get_balance_req)
        .await
        .expect("Failed to get balance from account");
    eprintln!("resp = {}", serde_json::to_string_pretty(&resp).unwrap());
    resp.balances.iter().map(|e| e.balance.value()).sum()
}

pub async fn get_confidential_balance(
    world: &mut TariWorld,
    account_name: String,
    wallet_daemon_name: String,
) -> Amount {
    let account_name = ComponentAddressOrName::Name(account_name);
    let get_balance_req = AccountsGetBalancesRequest {
        account: Some(account_name),
        refresh: true,
    };
    let mut client = get_auth_wallet_daemon_client(world, &wallet_daemon_name).await;

    let resp = client
        .get_account_balances(get_balance_req)
        .await
        .expect("Failed to get balance from account");
    eprintln!("resp = {}", serde_json::to_string_pretty(&resp).unwrap());
    resp.balances.iter().map(|e| e.confidential_balance).sum()
}

pub async fn submit_manifest_with_signing_keys(
    world: &mut TariWorld,
    wallet_daemon_name: String,
    account_signing_key: String,
    manifest_content: String,
    inputs: String,
    outputs_name: String,
    min_epoch: Option<Epoch>,
    max_epoch: Option<Epoch>,
) {
    let input_groups = inputs.split(',').map(|s| s.trim()).collect::<Vec<_>>();

    // generate globals for component addresses
    let globals: HashMap<String, ManifestValue> = world
        .outputs
        .iter()
        .filter(|(name, _)| input_groups.contains(&name.as_str()))
        .flat_map(|(name, outputs)| {
            outputs
                .iter()
                .map(move |(child_name, addr)| (format!("{}/{}", name, child_name), addr.substate_id.clone().into()))
        })
        .collect();

    // Supply the inputs explicitly. If this is empty, the internal component manager
    // will attempt to supply the correct inputs
    let inputs = inputs
        .split(',')
        .flat_map(|s| {
            world
                .outputs
                .get(s.trim())
                .unwrap_or_else(|| panic!("No outputs named {}", s.trim()))
        })
        .map(|(_, addr)| addr.clone())
        .collect::<Vec<_>>();

    let mut client = get_auth_wallet_daemon_client(world, &wallet_daemon_name).await;

    let account_name = ComponentAddressOrName::Name(account_signing_key);
    let AccountGetResponse { account, .. } = client.accounts_get(account_name).await.unwrap();

    let instructions = parse_manifest(&manifest_content, globals, HashMap::new()).unwrap();

    let transaction = transaction_builder()
        .fee_transaction_pay_from_component(account.address.as_component_address().unwrap(), Amount(2000))
        .with_instructions(instructions.instructions)
        .with_min_epoch(min_epoch)
        .with_max_epoch(max_epoch)
        .with_inputs(inputs.into_iter().map(|i| i.into_unversioned()))
        .build_unsigned_transaction();

    let transaction_submit_req = TransactionSubmitRequest {
        transaction,
        signing_key_index: Some(account.key_index),
        detect_inputs: true,
        detect_inputs_use_unversioned: true,
        proof_ids: vec![],
        autofill_inputs: vec![],
    };

    let resp = client.submit_transaction(transaction_submit_req).await.unwrap();

    let wait_req = TransactionWaitResultRequest {
        transaction_id: resp.transaction_id,
        timeout_secs: Some(120),
    };
    let wait_resp = client.wait_transaction_result(wait_req).await.unwrap();
    if let Some(reason) = wait_resp.result.as_ref().and_then(|result| result.reject().cloned()) {
        panic!("Transaction failed: {}", reason);
    }

    add_substate_ids(
        world,
        outputs_name,
        &wait_resp
            .result
            .expect("Transaction has timed out")
            .result
            .expect("Transaction has failed"),
    );
}

pub async fn submit_manifest(
    world: &mut TariWorld,
    wallet_daemon_name: String,
    manifest_content: String,
    inputs: String,
    outputs_name: String,
    min_epoch: Option<Epoch>,
    max_epoch: Option<Epoch>,
) {
    let input_groups = inputs.split(',').map(|s| s.trim()).collect::<Vec<_>>();

    // generate globals for component addresses
    let globals: HashMap<String, ManifestValue> = world
        .outputs
        .iter()
        .filter(|(name, _)| input_groups.contains(&name.as_str()))
        .flat_map(|(name, outputs)| {
            outputs
                .iter()
                .map(move |(child_name, addr)| (format!("{}/{}", name, child_name), addr.substate_id.clone().into()))
        })
        .collect();

    // Supply the inputs explicitly. If this is empty, the internal component manager
    // will attempt to supply the correct inputs
    let inputs = inputs
        .split(',')
        .flat_map(|s| {
            world
                .outputs
                .get(s.trim())
                .unwrap_or_else(|| panic!("No outputs named {}", s.trim()))
        })
        .map(|(_, addr)| addr.clone().into_unversioned())
        .collect::<Vec<_>>();

    let instructions = parse_manifest(&manifest_content, globals, HashMap::new())
        .unwrap_or_else(|err| panic!("Attempted to parse manifest but failed: {err}"));

    let mut client = get_auth_wallet_daemon_client(world, &wallet_daemon_name).await;

    let AccountGetResponse { account, .. } = client.accounts_get_default().await.unwrap();

    let transaction = transaction_builder()
        .fee_transaction_pay_from_component(account.address.as_component_address().unwrap(), Amount(2000))
        .with_instructions(instructions.instructions)
        .with_min_epoch(min_epoch)
        .with_max_epoch(max_epoch)
        .with_inputs(inputs)
        .build_unsigned_transaction();

    let transaction_submit_req = TransactionSubmitRequest {
        transaction,
        signing_key_index: Some(account.key_index),
        detect_inputs: true,
        detect_inputs_use_unversioned: true,
        proof_ids: vec![],
        autofill_inputs: vec![],
    };

    let resp = client.submit_transaction(transaction_submit_req).await.unwrap();

    let wait_req = TransactionWaitResultRequest {
        transaction_id: resp.transaction_id,
        timeout_secs: Some(120),
    };
    let wait_resp = client.wait_transaction_result(wait_req).await.unwrap();

    if let Some(reason) = wait_resp.result.clone().and_then(|finalize| finalize.reject().cloned()) {
        panic!("Transaction failed: {:?}", reason);
    }
    add_substate_ids(
        world,
        outputs_name,
        &wait_resp
            .result
            .expect("Transaction has timed out")
            .result
            .expect("Transaction has failed"),
    );
}

// pub async fn submit_transaction(
//     world: &mut TariWorld,
//     wallet_daemon_name: String,
//     fee_instructions: Vec<Instruction>,
//     instructions: Vec<Instruction>,
//     inputs: Vec<SubstateRequirement>,
//     outputs_name: String,
//     min_epoch: Option<Epoch>,
//     max_epoch: Option<Epoch>,
// ) -> TransactionWaitResultResponse {
//     let mut client = get_auth_wallet_daemon_client(world, &wallet_daemon_name).await;
//
//     let transaction = Transaction::builder()
//         .with_fee_instructions(fee_instructions)
//         .with_instructions(instructions)
//         .with_min_epoch(min_epoch)
//         .with_max_epoch(max_epoch)
//         .with_inputs(inputs)
//         .build_unsigned_transaction();
//
//     let transaction_submit_req = TransactionSubmitRequest {
//         transaction,
//         signing_key_index: None,
//         detect_inputs: true,
//         detect_inputs_use_unversioned: false,
//         autofill_inputs: inputs,
//         proof_ids: vec![],
//     };
//
//     let resp = client.submit_transaction(transaction_submit_req).await.unwrap();
//
//     let wait_req = TransactionWaitResultRequest {
//         transaction_id: resp.transaction_id,
//         timeout_secs: Some(120),
//     };
//     let wait_resp = client.wait_transaction_result(wait_req).await.unwrap();
//
//     if let Some(diff) = wait_resp.result.as_ref().and_then(|r| r.result.accept()) {
//         add_substate_ids(world, outputs_name, diff);
//     }
//     wait_resp
// }

pub async fn create_component(
    world: &mut TariWorld,
    outputs_name: String,
    template_name: String,
    account_name: String,
    wallet_daemon_name: String,
    function_call: String,
    args: Vec<String>,
    min_epoch: Option<Epoch>,
    max_epoch: Option<Epoch>,
) {
    let mut client = get_auth_wallet_daemon_client(world, &wallet_daemon_name).await;
    let template_address = world
        .templates
        .get(&template_name)
        .unwrap_or_else(|| {
            panic!(
                "Create component failed, template not found with name {}",
                template_name
            )
        })
        .address;
    let args = args.iter().map(|a| CliArg::from_str(a).unwrap().into_arg()).collect();
    let AccountGetResponse { account, .. } = client
        .accounts_get(ComponentAddressOrName::Name(account_name.clone()))
        .await
        .unwrap();

    let transaction = transaction_builder()
        .fee_transaction_pay_from_component(account.address.as_component_address().unwrap(), Amount(2000))
        .call_function(template_address, &function_call, args)
        .with_min_epoch(min_epoch)
        .with_max_epoch(max_epoch)
        .build_unsigned_transaction();

    let transaction_submit_req = TransactionSubmitRequest {
        transaction,
        signing_key_index: Some(account.key_index),
        detect_inputs: true,
        detect_inputs_use_unversioned: true,
        proof_ids: vec![],
        autofill_inputs: vec![],
    };

    let resp = client.submit_transaction(transaction_submit_req).await.unwrap();

    let wait_req = TransactionWaitResultRequest {
        transaction_id: resp.transaction_id,
        timeout_secs: Some(120),
    };
    let wait_resp = client.wait_transaction_result(wait_req).await.unwrap();

    if wait_resp.timed_out {
        panic!("No result after 120s. Time out.");
    }

    if let Some(reason) = wait_resp.result.as_ref().and_then(|finalize| finalize.full_reject()) {
        panic!("Create component tx failed: {}", reason);
    }

    add_substate_ids(
        world,
        outputs_name,
        &wait_resp
            .result
            .expect("No result")
            .result
            .expect("Failed to obtain substate diffs"),
    );
}

pub fn find_output_version(
    world: &mut TariWorld,
    output_ref: &str,
    output_component_substate_id: SubstateId,
) -> anyhow::Result<Option<u32>> {
    let outputs_name = output_ref.split('/').next().ok_or(anyhow!("Output must have a name"))?;
    Ok(world
        .outputs
        .entry(outputs_name.to_string())
        .or_default()
        .iter()
        .filter(|(_, requirement)| requirement.substate_id == output_component_substate_id)
        .map(|(_, requirement)| requirement.version)
        .last()
        .unwrap_or_default())
}

pub async fn call_component(
    world: &mut TariWorld,
    account_name: String,
    output_ref: String,
    wallet_daemon_name: String,
    function_call: String,
    new_outputs_name: Option<String>,
    use_unversioned_inputs: bool,
) -> anyhow::Result<TransactionWaitResultResponse> {
    let mut client = get_auth_wallet_daemon_client(world, &wallet_daemon_name).await;

    let source_component_address = get_address_from_output(world, output_ref.clone())
        .as_component_address()
        .expect("Failed to get component address from output");
    let source_component_name = output_ref
        .split('/')
        .next()
        .ok_or(anyhow!("Output must have a name"))?
        .to_string();

    let account = get_account_from_name(&mut client, account_name).await;
    let account_component_address = account
        .address
        .as_component_address()
        .expect("Failed to get account component address");

    let inputs = if use_unversioned_inputs {
        [
            SubstateRequirement::unversioned(account_component_address),
            SubstateRequirement::unversioned(source_component_address),
        ]
    } else {
        // Typically only used in failing tests to assert that a substate is already DOWN
        [
            SubstateRequirement::new(
                account_component_address.into(),
                find_output_version(world, output_ref.as_str(), account_component_address.into())?,
            ),
            SubstateRequirement::new(
                source_component_address.into(),
                find_output_version(world, output_ref.as_str(), source_component_address.into())?,
            ),
        ]
    };

    let tx = transaction_builder()
        .fee_transaction_pay_from_component(account_component_address, Amount(1000))
        .call_method(source_component_address, &function_call, vec![])
        .with_inputs(inputs)
        .build_unsigned_transaction();

    let resp = submit_unsigned_tx_and_wait_for_response(client, tx, account, use_unversioned_inputs).await?;

    let final_outputs_name = if let Some(name) = new_outputs_name {
        name
    } else {
        source_component_name
    };

    add_substate_ids(
        world,
        final_outputs_name,
        &resp
            .clone()
            .result
            .expect("Call component transaction has timed out")
            .result
            .expect("Call component transaction has failed"),
    );

    Ok(resp)
}

pub async fn concurrent_call_component(
    world: &mut TariWorld,
    account_name: String,
    output_ref: String,
    wallet_daemon_name: String,
    function_call: String,
    times: usize,
) -> anyhow::Result<()> {
    log::info!(
        "concurrent_call_component: account_name={account_name}, output_ref={output_ref}, \
         wallet_daemon_name={wallet_daemon_name}"
    );
    let mut client = get_auth_wallet_daemon_client(world, &wallet_daemon_name).await;

    let source_component_address = get_address_from_output(world, output_ref.clone())
        .as_component_address()
        .expect("Failed to get component address from output");

    let account = get_account_from_name(&mut client, account_name).await;
    let account_component_address = account
        .address
        .as_component_address()
        .expect("Failed to get account component address");

    let mut join_set = JoinSet::new();
    for _ in 0..times {
        let acc = account.clone();
        let clt = client.clone();
        let tx = transaction_builder()
            .fee_transaction_pay_from_component(account_component_address, Amount(1000))
            .call_method(source_component_address, &function_call, vec![])
            .build_unsigned_transaction();
        join_set.spawn(submit_unsigned_tx_and_wait_for_response(clt, tx, acc, true));
    }

    while let Some(result) = join_set.join_next().await {
        let result = result.map_err(|e| e.to_string());
        match result {
            Ok(response) => match response {
                Ok(resp) => {
                    add_substate_ids(
                        world,
                        output_ref.clone(),
                        &resp
                            .result
                            .expect("no finalize result")
                            .result
                            .expect("no transaction result"),
                    );
                },
                Err(error) => bail!("Failed to submit transaction: {error:?}"),
            },
            Err(e) => bail!("Failed to get response from handler: {}", e),
        }
    }

    Ok(())
}

pub async fn transfer(
    world: &mut TariWorld,
    account_name: String,
    destination_public_key: RistrettoPublicKey,
    resource_address: ResourceAddress,
    amount: Amount,
    wallet_daemon_name: String,
    outputs_name: String,
) {
    let mut client = get_auth_wallet_daemon_client(world, &wallet_daemon_name).await;

    let account = Some(ComponentAddressOrName::Name(account_name));
    let max_fee = Some(Amount(2000));

    let request = AccountsTransferRequest {
        account,
        amount,
        resource_address,
        destination_public_key,
        max_fee,
        proof_from_badge_resource: None,
        dry_run: false,
    };

    let resp = client.accounts_transfer(request).await.unwrap();
    add_substate_ids(world, outputs_name, resp.result.result.accept().unwrap());
}

pub async fn confidential_transfer(
    world: &mut TariWorld,
    account_name: String,
    destination_public_key: RistrettoPublicKey,
    amount: Amount,
    wallet_daemon_name: String,
    outputs_name: String,
) {
    let mut client = get_auth_wallet_daemon_client(world, &wallet_daemon_name).await;

    let account = Some(ComponentAddressOrName::Name(account_name));
    let max_fee = Some(Amount(2000));

    let request = ConfidentialTransferRequest {
        account,
        amount,
        destination_public_key,
        max_fee,
        resource_address: CONFIDENTIAL_TARI_RESOURCE_ADDRESS,
        proof_from_badge_resource: None,
        dry_run: false,
        input_selection: ConfidentialTransferInputSelection::PreferRevealed,
        output_to_revealed: false,
    };

    let resp = client.accounts_confidential_transfer(request).await.unwrap();
    add_substate_ids(world, outputs_name, resp.result.result.accept().unwrap());
}

pub async fn get_auth_wallet_daemon_client(world: &TariWorld, wallet_daemon_name: &str) -> WalletDaemonClient {
    world
        .wallet_daemons
        .get(wallet_daemon_name)
        .unwrap_or_else(|| panic!("Wallet daemon not found with name {}", wallet_daemon_name))
        .get_authed_client()
        .await
}

async fn get_account_from_name(client: &mut WalletDaemonClient, account_name: String) -> Account {
    let source_account_name = ComponentAddressOrName::Name(account_name.clone());
    let AccountGetResponse { account, .. } =
        client
            .accounts_get(source_account_name.clone())
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "Failed to get account with name {}. Error: {:?}",
                    source_account_name, e
                )
            });
    account
}

async fn submit_unsigned_tx_and_wait_for_response(
    mut client: WalletDaemonClient,
    transaction: UnsignedTransaction,
    account: Account,
    use_unversioned_inputs: bool,
) -> anyhow::Result<TransactionWaitResultResponse> {
    log::info!(
        "submit_unsigned_tx_and_wait_for_response: account={account}, use_unversioned_inputs={use_unversioned_inputs}",
    );
    let submit_req = TransactionSubmitRequest {
        transaction,
        signing_key_index: Some(account.key_index),
        autofill_inputs: vec![],
        detect_inputs: true,
        detect_inputs_use_unversioned: use_unversioned_inputs,
        proof_ids: vec![],
    };

    let submit_resp = client.submit_transaction(submit_req).await?;
    let wait_req = TransactionWaitResultRequest {
        transaction_id: submit_resp.transaction_id,
        timeout_secs: Some(120),
    };
    let resp = client
        .wait_transaction_result(wait_req)
        .await
        .map_err(|e| anyhow::Error::msg(e.to_string()))?;

    if let Some(reason) = resp.result.as_ref().and_then(|finalize| finalize.reject()) {
        bail!("Calling component result rejected: {}", reason);
    }

    Ok(resp)
}
