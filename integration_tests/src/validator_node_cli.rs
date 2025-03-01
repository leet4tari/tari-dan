//  Copyright 2022 The Tari Project
//  SPDX-License-Identifier: BSD-3-Clause

use std::{collections::HashMap, path::PathBuf, str::FromStr};

use tari_dan_common_types::SubstateRequirement;
use tari_engine_types::{
    commit_result::RejectReason,
    instruction::Instruction,
    substate::{SubstateDiff, SubstateId},
};
use tari_template_builtin::ACCOUNT_TEMPLATE_ADDRESS;
use tari_template_lib::args;
use tari_transaction_manifest::{parse_manifest, ManifestValue};
use tari_validator_node_cli::{
    command::transaction::{handle_submit, submit_transaction, CliArg, CliInstruction, CommonSubmitArgs, SubmitArgs},
    from_hex::FromHex,
    key_manager::KeyManager,
};
use tari_validator_node_client::{types::SubmitTransactionResponse, ValidatorNodeClient};

use crate::{helpers::get_component_from_namespace, logging::get_base_dir_for_scenario, TariWorld};

fn get_key_manager(world: &mut TariWorld) -> KeyManager {
    let path = get_cli_data_dir(world);

    // initialize the account public/private keys
    KeyManager::init(path).unwrap()
}
pub fn create_or_use_key(world: &mut TariWorld, key_name: String) {
    let km = get_key_manager(world);
    if let Some((_, k)) = world.account_keys.get(&key_name) {
        km.set_active_key(&k.to_string()).unwrap();
    } else {
        let key = km.create().expect("Could not create a new key pair");
        km.set_active_key(&key.public_key.to_string()).unwrap();
        world.account_keys.insert(key_name, (key.secret_key, key.public_key));
    }
}
pub fn create_key(world: &mut TariWorld, key_name: String) {
    let key = get_key_manager(world)
        .create()
        .expect("Could not create a new key pair");

    world.account_keys.insert(key_name, (key.secret_key, key.public_key));
}

pub async fn create_account(world: &mut TariWorld, account_name: String, validator_node_name: String) {
    let data_dir = get_cli_data_dir(world);
    let key = get_key_manager(world).create().expect("Could not create keypair");
    let owner_token = key.to_owner_token();
    world
        .account_keys
        .insert(account_name.clone(), (key.secret_key.clone(), key.public_key.clone()));
    // create an account component
    let instruction = Instruction::CallFunction {
        // The "account" template is builtin in the validator nodes with a constant address
        template_address: ACCOUNT_TEMPLATE_ADDRESS,
        function: "create".to_string(),
        args: args!(owner_token),
    };
    let common = CommonSubmitArgs {
        wait_for_result: true,
        wait_for_result_timeout: Some(120),
        inputs: vec![],
        version: None,
        dump_outputs_into: None,
        account_template_address: None,
        dry_run: false,
    };
    let mut client = world.get_validator_node(&validator_node_name).get_client();
    let resp = submit_transaction(vec![instruction], common, data_dir, &mut client)
        .await
        .unwrap();

    if let Some(ref failure) = resp.dry_run_result.as_ref().unwrap().finalize.reject() {
        panic!("Transaction failed: {:?}", failure);
    }

    // store the account component address and other substate id for later reference
    add_substate_ids(
        world,
        account_name,
        resp.dry_run_result.unwrap().finalize.result.accept().unwrap(),
    );
}

pub async fn create_component(
    world: &mut TariWorld,
    outputs_name: String,
    template_name: String,
    vn_name: String,
    function_call: String,
    args: Vec<String>,
) {
    let data_dir = get_cli_data_dir(world);

    let template_address = world
        .templates
        .get(&template_name)
        .unwrap_or_else(|| panic!("Template not found with name {}", template_name))
        .address;
    let args: Vec<CliArg> = args.iter().map(|a| CliArg::from_str(a).unwrap()).collect();
    let instruction = CliInstruction::CallFunction {
        template_address: FromHex(template_address),
        function_name: function_call,
        args,
    };

    let args = SubmitArgs {
        instruction,
        common: CommonSubmitArgs {
            wait_for_result: true,
            wait_for_result_timeout: Some(300),
            inputs: vec![],
            version: None,
            dump_outputs_into: None,
            account_template_address: None,
            dry_run: false,
        },
    };
    let mut client = world.get_validator_node(&vn_name).get_client();
    let resp = handle_submit(args, data_dir, &mut client).await.unwrap();

    if let Some(ref failure) = resp.dry_run_result.as_ref().unwrap().finalize.reject() {
        panic!("Transaction failed: {:?}", failure);
    }
    // store the account component address and other substate ids for later reference
    add_substate_ids(
        world,
        outputs_name,
        resp.dry_run_result.unwrap().finalize.result.accept().unwrap(),
    );
}

pub(crate) fn add_substate_ids(world: &mut TariWorld, outputs_name: String, diff: &SubstateDiff) {
    let outputs = world.outputs.entry(outputs_name).or_default();
    let mut counters = [0usize, 0, 0, 0, 0, 0, 0, 0, 0, 0];
    for (addr, data) in diff.up_iter() {
        match addr {
            SubstateId::Component(_) => {
                let component = data.substate_value().component().unwrap();
                outputs.insert(format!("components/{}", component.module_name), SubstateRequirement {
                    substate_id: addr.clone(),
                    version: Some(data.version()),
                });
                counters[0] += 1;
            },
            SubstateId::Resource(_) => {
                outputs.insert(format!("resources/{}", counters[1]), SubstateRequirement {
                    substate_id: addr.clone(),
                    version: Some(data.version()),
                });
                counters[1] += 1;
            },
            SubstateId::Vault(_) => {
                outputs.insert(format!("vaults/{}", counters[2]), SubstateRequirement {
                    substate_id: addr.clone(),
                    version: Some(data.version()),
                });
                counters[2] += 1;
            },
            SubstateId::NonFungible(_) => {
                outputs.insert(format!("nfts/{}", counters[3]), SubstateRequirement {
                    substate_id: addr.clone(),
                    version: Some(data.version()),
                });
                counters[3] += 1;
            },
            SubstateId::UnclaimedConfidentialOutput(_) => {
                outputs.insert(format!("layer_one_commitments/{}", counters[4]), SubstateRequirement {
                    substate_id: addr.clone(),
                    version: Some(data.version()),
                });
                counters[4] += 1;
            },
            SubstateId::NonFungibleIndex(_) => {
                outputs.insert(format!("nft_indexes/{}", counters[5]), SubstateRequirement {
                    substate_id: addr.clone(),
                    version: Some(data.version()),
                });
                counters[5] += 1;
            },
            SubstateId::TransactionReceipt(_) => {
                outputs.insert(format!("transaction_receipt/{}", counters[6]), SubstateRequirement {
                    substate_id: addr.clone(),
                    version: Some(data.version()),
                });
                counters[6] += 1;
            },
            SubstateId::Template(_) => {
                outputs.insert(format!("published_template/{}", counters[8]), SubstateRequirement {
                    substate_id: addr.clone(),
                    version: Some(data.version()),
                });
                counters[7] += 1;
            },
            SubstateId::ValidatorFeePool(_) => {
                outputs.insert(format!("validator_fee_pool/{}", counters[8]), SubstateRequirement {
                    substate_id: addr.clone(),
                    version: Some(data.version()),
                });
                counters[8] += 1;
            },
        }
    }
}

pub async fn concurrent_call_method(
    world: &mut TariWorld,
    vn_name: String,
    fq_component_name: String,
    method_call: String,
    times: usize,
) -> Result<SubmitTransactionResponse, RejectReason> {
    let mut component = get_component_from_namespace(world, fq_component_name);
    // For concurrent transactions we DO NOT specify the versions
    component.version = None;

    let vn_data_dir = get_cli_data_dir(world);
    let vn_client = world.get_validator_node(&vn_name).get_client();
    let mut handles = Vec::new();
    for _ in 0..times {
        let handle = tokio::spawn(call_method_inner(
            vn_client.clone(),
            vn_data_dir.clone(),
            component.clone(),
            method_call.clone(),
        ));
        handles.push(handle);
    }

    let mut last_resp = None;
    for handle in handles {
        let result = handle
            .await
            .map_err(|e| RejectReason::ExecutionFailure(e.to_string()))?;
        match result {
            Ok(response) => last_resp = Some(response),
            Err(e) => return Err(e),
        }
    }

    if let Some(res) = last_resp {
        Ok(res)
    } else {
        Err(RejectReason::ExecutionFailure(
            "No responses from any of the concurrent calls".to_owned(),
        ))
    }
}

pub async fn call_method(
    world: &mut TariWorld,
    vn_name: String,
    fq_component_name: String,
    outputs_name: String,
    method_call: String,
) -> Result<SubmitTransactionResponse, RejectReason> {
    let data_dir = get_cli_data_dir(world);
    let component = get_component_from_namespace(world, fq_component_name);
    let vn_client = world.get_validator_node(&vn_name).get_client();
    let resp = call_method_inner(vn_client, data_dir, component, method_call).await?;

    // store the account component address and other substate ids for later reference
    add_substate_ids(
        world,
        outputs_name,
        resp.dry_run_result.as_ref().unwrap().finalize.result.accept().unwrap(),
    );
    Ok(resp)
}

async fn call_method_inner(
    vn_client: ValidatorNodeClient,
    vn_data_dir: PathBuf,
    component: SubstateRequirement,
    method_call: String,
) -> Result<SubmitTransactionResponse, RejectReason> {
    let instruction = CliInstruction::CallMethod {
        component_address: component.substate_id.clone(),
        // TODO: actually parse the method call for arguments
        method_name: method_call,
        args: vec![],
    };

    println!("Inputs: {}", component);
    let args = SubmitArgs {
        instruction,
        common: CommonSubmitArgs {
            wait_for_result: true,
            wait_for_result_timeout: Some(60),
            inputs: vec![component],
            version: None,
            dump_outputs_into: None,
            account_template_address: None,
            dry_run: false,
        },
    };
    let resp = handle_submit(args, vn_data_dir, &mut vn_client.clone()).await.unwrap();

    if let Some(failure) = resp.dry_run_result.as_ref().unwrap().finalize.reject() {
        return Err(failure.clone());
    }

    Ok(resp)
}

pub async fn submit_manifest(
    world: &mut TariWorld,
    vn_name: String,
    outputs_name: String,
    manifest_content: String,
    input_str: String,
    signing_key_name: String,
) {
    // HACKY: Sets the active key so that submit_transaction will use it.
    let (_, key) = world.account_keys.get(&signing_key_name).unwrap();
    let key_str = key.to_string();
    get_key_manager(world).set_active_key(&key_str).unwrap();

    let input_groups = input_str.split(',').map(|s| s.trim()).collect::<Vec<_>>();
    // generate globals for components addresses
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

    // parse the manifest
    let instructions = parse_manifest(&manifest_content, globals, HashMap::new()).unwrap();

    // submit the instructions to the vn
    let mut client = world.get_validator_node(&vn_name).get_client();
    let data_dir = get_cli_data_dir(world);

    // Supply the inputs explicitly. If this is empty, the internal component manager will attempt to supply the correct
    // inputs
    let inputs = input_str
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.starts_with("ref:"))
        .flat_map(|s| {
            world
                .outputs
                .get(s)
                .unwrap_or_else(|| panic!("No outputs named {}", s.trim()))
        })
        .filter(|(_, addr)| !addr.substate_id.is_transaction_receipt())
        .map(|(_, addr)| addr.clone())
        .collect::<Vec<_>>();

    // Remove inputs that have been downed
    let inputs = select_latest_version(inputs);

    let args = CommonSubmitArgs {
        wait_for_result: true,
        wait_for_result_timeout: Some(60),
        inputs,
        version: None,
        dump_outputs_into: None,
        account_template_address: None,
        dry_run: false,
    };
    let resp = submit_transaction(instructions.instructions, args, data_dir, &mut client)
        .await
        .unwrap();

    if let Some(ref failure) = resp.dry_run_result.as_ref().unwrap().finalize.reject() {
        panic!("Transaction failed: {:?}", failure);
    }

    add_substate_ids(
        world,
        outputs_name,
        resp.dry_run_result.unwrap().finalize.result.accept().unwrap(),
    );
}

pub(crate) fn get_cli_data_dir(world: &mut TariWorld) -> PathBuf {
    get_base_dir_for_scenario("vn_cli", world.current_scenario_name.as_ref().unwrap(), "SHARED")
}

// Remove inputs that have been downed
fn select_latest_version(mut inputs: Vec<SubstateRequirement>) -> Vec<SubstateRequirement> {
    inputs.sort_by(|a, b| b.substate_id.cmp(&a.substate_id).then(b.version.cmp(&a.version)));
    inputs.dedup_by(|a, b| a.substate_id == b.substate_id);
    inputs
}
