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
    collections::HashMap,
    convert::{TryFrom, TryInto},
    fmt,
    fs,
    path::PathBuf,
    str::FromStr,
    time::{Duration, Instant},
};

use anyhow::anyhow;
use clap::{Args, Subcommand};
use tari_bor::decode_exact;
use tari_common_types::types::PublicKey;
use tari_dan_common_types::{Epoch, SubstateAddress, SubstateRequirement};
use tari_dan_engine::abi::Type;
use tari_dan_wallet_sdk::apis::confidential_transfer::ConfidentialTransferInputSelection;
use tari_engine_types::{
    commit_result::{FinalizeResult, RejectReason, TransactionResult},
    instruction::Instruction,
    instruction_result::InstructionResult,
    parse_template_address,
    substate::{SubstateDiff, SubstateId, SubstateValue},
    TemplateAddress,
};
use tari_template_lib::{
    arg,
    args,
    args::Arg,
    constants::CONFIDENTIAL_TARI_RESOURCE_ADDRESS,
    models::{Amount, BucketId, NonFungibleAddress, NonFungibleId},
    prelude::ResourceAddress,
};
use tari_transaction::{Transaction, TransactionId, UnsignedTransaction};
use tari_transaction_manifest::{parse_manifest, ManifestValue};
use tari_utilities::ByteArray;
use tari_wallet_daemon_client::{
    types::{
        AccountGetResponse,
        AccountsTransferRequest,
        ConfidentialTransferRequest,
        SettingsGetResponse,
        TransactionGetResultRequest,
        TransactionSubmitDryRunRequest,
        TransactionSubmitRequest,
        TransactionWaitResultRequest,
        TransactionWaitResultResponse,
    },
    ComponentAddressOrName,
    WalletDaemonClient,
};

use crate::from_hex::FromHex;

#[derive(Debug, Subcommand, Clone)]
pub enum TransactionSubcommand {
    Get(GetArgs),
    Submit(SubmitArgs),
    SubmitManifest(SubmitManifestArgs),
    Send(SendArgs),
    ConfidentialTransfer(ConfidentialTransferArgs),
}

#[derive(Debug, Args, Clone)]
pub struct GetArgs {
    transaction_id: FromHex<TransactionId>,
}

#[derive(Debug, Args, Clone)]
pub struct SubmitArgs {
    #[clap(subcommand)]
    pub instruction: CliInstruction,
    #[clap(flatten)]
    pub common: CommonSubmitArgs,
}

#[derive(Debug, Args, Clone)]
pub struct CommonSubmitArgs {
    /// Timeout in seconds
    #[clap(long, short = 't', alias = "wait-timeout")]
    pub wait_for_result_timeout_secs: Option<u64>,
    #[clap(long, short = 'n')]
    pub num_outputs: Option<u8>,
    #[clap(long, short = 'i')]
    pub inputs: Vec<SubstateRequirement>,
    #[clap(long, alias = "autofill")]
    pub detect_inputs: Option<bool>,
    #[clap(long, short = 'v')]
    pub version: Option<u8>,
    #[clap(long, short = 'd')]
    pub dump_outputs_into: Option<ComponentAddressOrName>,
    #[clap(long)]
    pub dry_run: bool,
    #[clap(long)]
    pub max_fee: Option<u64>,
    #[clap(long, short = 'f', alias = "fee-account")]
    pub fee_account: Option<ComponentAddressOrName>,
    #[clap(long)]
    pub min_epoch: Option<u64>,
    #[clap(long)]
    pub max_epoch: Option<u64>,
}

#[derive(Debug, Args, Clone)]
pub struct SubmitManifestArgs {
    manifest: PathBuf,
    #[clap(long, short = 'g')]
    input_variables: Vec<String>,
    #[clap(flatten)]
    common: CommonSubmitArgs,
}

#[derive(Debug, Args, Clone)]
pub struct SendArgs {
    amount: u64,
    resource_address: ResourceAddress,
    destination_public_key: FromHex<Vec<u8>>,
    #[clap(flatten)]
    common: CommonSubmitArgs,
    source_account_name: Option<ComponentAddressOrName>,
}

#[derive(Debug, Args, Clone)]
pub struct ConfidentialTransferArgs {
    amount: u64,
    destination_public_key: FromHex<Vec<u8>>,
    #[clap(flatten)]
    common: CommonSubmitArgs,
    #[clap(long, short = 'a', alias = "account")]
    source_account: Option<ComponentAddressOrName>,
    /// The address of the resource to send. If not provided, use the default Tari confidential resource
    #[clap(long)]
    resource_address: Option<ResourceAddress>,
}

#[derive(Debug, Subcommand, Clone)]
pub enum CliInstruction {
    CallFunction {
        template_address: FromHex<TemplateAddress>,
        function_name: String,
        #[clap(long, short = 'a')]
        args: Vec<CliArg>,
    },
    CallMethod {
        component_address: SubstateId,
        method_name: String,
        #[clap(long, short = 'a')]
        args: Vec<CliArg>,
    },
}

impl TransactionSubcommand {
    pub async fn handle(self, mut client: WalletDaemonClient) -> Result<(), anyhow::Error> {
        match self {
            TransactionSubcommand::Submit(args) => {
                handle_submit(args, &mut client).await?;
            },
            TransactionSubcommand::SubmitManifest(args) => {
                handle_submit_manifest(args, &mut client).await?;
            },
            TransactionSubcommand::Get(args) => handle_get(args, &mut client).await?,
            TransactionSubcommand::Send(args) => {
                handle_send(args, &mut client).await?;
            },
            TransactionSubcommand::ConfidentialTransfer(args) => {
                handle_confidential_transfer(args, &mut client).await?;
            },
        }
        Ok(())
    }
}

async fn handle_get(args: GetArgs, client: &mut WalletDaemonClient) -> Result<(), anyhow::Error> {
    let request = TransactionGetResultRequest {
        transaction_id: args.transaction_id.into_inner(),
    };
    let resp = client.get_transaction_result(request).await?;

    if let Some(result) = resp.result {
        println!("Transaction {}", args.transaction_id);
        println!();

        summarize_finalize_result(&result);
    } else {
        println!("Transaction not finalized",);
    }

    Ok(())
}

pub async fn handle_submit(args: SubmitArgs, client: &mut WalletDaemonClient) -> Result<(), anyhow::Error> {
    let SubmitArgs { instruction, common } = args;
    let instruction = match instruction {
        CliInstruction::CallFunction {
            template_address,
            function_name,
            args,
        } => Instruction::CallFunction {
            template_address: template_address.into_inner(),
            function: function_name,
            args: args.into_iter().map(|s| s.into_arg()).collect(),
        },
        CliInstruction::CallMethod {
            component_address,
            method_name,
            args,
        } => Instruction::CallMethod {
            component_address: component_address
                .as_component_address()
                .ok_or_else(|| anyhow!("Invalid component address: {}", component_address))?,
            method: method_name,
            args: args.into_iter().map(|s| s.into_arg()).collect(),
        },
    };

    let fee_account;
    if let Some(fee_account_name) = common.fee_account.clone() {
        fee_account = client.accounts_get(fee_account_name).await?.account;
    } else {
        fee_account = client.accounts_get_default().await?.account;
    }

    let SettingsGetResponse { network, .. } = client.get_settings().await?;

    let mut builder = Transaction::builder()
        .for_network(network.byte)
        .fee_transaction_pay_from_component(
            fee_account.address.as_component_address().unwrap(),
            Amount::try_from(common.max_fee.unwrap_or(1000))?,
        )
        .add_instruction(instruction)
        .with_inputs(common.inputs)
        .with_min_epoch(common.min_epoch.map(Epoch))
        .with_max_epoch(common.max_epoch.map(Epoch));

    if let Some(dump_account) = common.dump_outputs_into {
        let AccountGetResponse { account, .. } = client.accounts_get(dump_account).await?;

        builder = builder.put_last_instruction_output_on_workspace("bucket").call_method(
            account.address.as_component_address().unwrap(),
            "deposit",
            args![Workspace("bucket")],
        );
    }

    let transaction = builder.build_unsigned_transaction();
    summarize_transaction(&transaction);

    if common.dry_run {
        println!("NOTE: Dry run is enabled. This transaction will not be processed by the network.");
        println!();
        let resp = client
            .submit_transaction_dry_run(TransactionSubmitDryRunRequest {
                transaction,
                signing_key_index: None,
                autofill_inputs: vec![],
                detect_inputs: common.detect_inputs.unwrap_or(true),
                detect_inputs_use_unversioned: true,
                proof_ids: vec![],
            })
            .await?;
        wait_transaction_result(resp.transaction_id, client).await?;
    } else {
        let request = TransactionSubmitRequest {
            transaction,
            signing_key_index: None,
            autofill_inputs: vec![],
            detect_inputs: common.detect_inputs.unwrap_or(true),
            detect_inputs_use_unversioned: true,
            proof_ids: vec![],
        };
        let resp = client.submit_transaction(&request).await?;
        wait_transaction_result(resp.transaction_id, client).await?;
    }
    Ok(())
}

async fn handle_submit_manifest(
    args: SubmitManifestArgs,
    client: &mut WalletDaemonClient,
) -> Result<(), anyhow::Error> {
    let timer = Instant::now();
    let contents = fs::read_to_string(&args.manifest).map_err(|e| anyhow!("Failed to read manifest: {}", e))?;
    let instructions = parse_manifest(&contents, parse_globals(args.input_variables)?, Default::default())?;
    let common = args.common;

    let fee_account;
    if let Some(fee_account_name) = common.fee_account.clone() {
        fee_account = client.accounts_get(fee_account_name).await?.account;
    } else {
        fee_account = client.accounts_get_default().await?.account;
    }

    let SettingsGetResponse { network, .. } = client.get_settings().await?;

    let builder = Transaction::builder()
        .for_network(network.byte)
        .with_fee_instructions(
            instructions
                .fee_instructions
                .into_iter()
                .chain(vec![Instruction::CallMethod {
                    component_address: fee_account.address.as_component_address().unwrap(),
                    method: "pay_fee".to_string(),
                    args: args![Amount::try_from(common.max_fee.unwrap_or(1000))?],
                }]),
        )
        .with_instructions(instructions.instructions)
        .with_inputs(common.inputs)
        .with_min_epoch(common.min_epoch.map(Epoch))
        .with_max_epoch(common.max_epoch.map(Epoch));

    let transaction = builder.build_unsigned_transaction();
    summarize_transaction(&transaction);

    if common.dry_run {
        println!("NOTE: Dry run is enabled. This transaction will not be processed by the network.");
        println!();

        let resp = client
            .submit_transaction_dry_run(TransactionSubmitDryRunRequest {
                transaction,
                signing_key_index: None,
                autofill_inputs: vec![],
                detect_inputs: common.detect_inputs.unwrap_or(true),
                detect_inputs_use_unversioned: true,
                proof_ids: vec![],
            })
            .await?;
        summarize(&resp.result.finalize, timer.elapsed());
    } else {
        let request = TransactionSubmitRequest {
            transaction,
            signing_key_index: None,
            autofill_inputs: vec![],
            detect_inputs: common.detect_inputs.unwrap_or(true),
            detect_inputs_use_unversioned: true,
            proof_ids: vec![],
        };

        let resp = client.submit_transaction(&request).await?;
        wait_transaction_result(resp.transaction_id, client).await?;
    }

    Ok(())
}

pub async fn handle_send(args: SendArgs, client: &mut WalletDaemonClient) -> Result<(), anyhow::Error> {
    let SendArgs {
        source_account_name,
        amount,
        resource_address,
        destination_public_key,
        common,
    } = args;

    let destination_public_key =
        PublicKey::from_canonical_bytes(&destination_public_key.into_inner()).map_err(anyhow::Error::msg)?;

    let fee = common.max_fee.map(|f| f.try_into()).transpose()?;
    let resp = client
        .accounts_transfer(AccountsTransferRequest {
            account: source_account_name,
            amount: Amount::try_from(amount)?,
            resource_address,
            destination_public_key,
            max_fee: fee,
            proof_from_badge_resource: None,
            dry_run: false,
        })
        .await?;

    println!("Transaction: {}", resp.transaction_id);
    println!("Fee: {} ({} refunded)", resp.fee, resp.fee_refunded);
    println!();
    summarize_finalize_result(&resp.result);

    Ok(())
}

pub async fn handle_confidential_transfer(
    args: ConfidentialTransferArgs,
    client: &mut WalletDaemonClient,
) -> Result<(), anyhow::Error> {
    let ConfidentialTransferArgs {
        source_account,
        resource_address,
        amount,
        destination_public_key,
        common,
    } = args;

    // let AccountByNameResponse { account, .. } = client.accounts_get_by_name(&source_account_name).await?;
    let destination_public_key =
        PublicKey::from_canonical_bytes(&destination_public_key.into_inner()).map_err(anyhow::Error::msg)?;
    let resp = client
        .accounts_confidential_transfer(ConfidentialTransferRequest {
            account: source_account,
            input_selection: ConfidentialTransferInputSelection::PreferConfidential,
            amount: Amount::try_from(amount)?,
            resource_address: resource_address.unwrap_or(CONFIDENTIAL_TARI_RESOURCE_ADDRESS),
            destination_public_key,
            max_fee: common.max_fee.map(|f| f.try_into()).transpose()?,
            output_to_revealed: false,
            proof_from_badge_resource: None,
            dry_run: false,
        })
        .await?;

    println!("Transaction: {}", resp.transaction_id);
    println!("Fee: {}", resp.fee);
    println!();
    summarize_finalize_result(&resp.result);

    Ok(())
}

pub async fn wait_transaction_result(
    transaction_id: TransactionId,
    client: &mut WalletDaemonClient,
) -> Result<TransactionWaitResultResponse, anyhow::Error> {
    let timer = Instant::now();

    println!();
    println!("✅ Transaction {} submitted.", transaction_id);
    println!();

    println!();
    println!("⏳️ Waiting for transaction result...");
    println!();
    let wait_resp = client
        .wait_transaction_result(TransactionWaitResultRequest {
            transaction_id,
            // Never timeout, you can ctrl+c to exit
            timeout_secs: None,
        })
        .await?;
    if wait_resp.timed_out {
        println!("⏳️ Transaction result timed out.",);
        println!();
    } else if let Some(ref result) = wait_resp.result {
        summarize(result, timer.elapsed());
    } else {
        println!("⚠️ Transaction not finalized");
    }

    Ok(wait_resp)
}

fn summarize_transaction(transaction: &UnsignedTransaction) {
    println!("Inputs:");
    if transaction.inputs().is_empty() {
        println!("  None");
    } else {
        for req in transaction.inputs() {
            println!("- {}", req);
        }
    }
    println!();
    println!("🌟 Submitting fee instructions:");
    for instruction in transaction.fee_instructions() {
        println!("- {}", instruction);
    }
    println!();
    println!("🌟 Submitting instructions:");
    for instruction in transaction.instructions() {
        println!("- {}", instruction);
    }
    println!();
}

fn summarize(result: &FinalizeResult, time_taken: Duration) {
    println!("✅️ Transaction complete");
    println!();
    // if let Some(qc) = resp.qcs.first() {
    //     println!("Epoch: {}", qc.epoch());
    //     println!("Payload height: {}", qc.payload_height());
    //     println!("Signed by: {} validator nodes", qc.validators_metadata().len());
    // } else {
    //     println!("No QC");
    // }
    // println!();

    summarize_finalize_result(result);

    println!();
    println!("Fee: {}", result.fee_receipt.total_fees_charged());
    println!("Time taken: {:?}", time_taken);
    println!();
    println!("OVERALL DECISION: {}", result.result);
}

pub fn print_substate_diff(diff: &SubstateDiff) {
    for (address, substate) in diff.up_iter() {
        println!("️🌲 UP substate {} (v{})", address, substate.version(),);
        println!(
            "      🧩 Substate address: {}",
            SubstateAddress::from_substate_id(address, substate.version())
        );
        match substate.substate_value() {
            SubstateValue::Component(component) => {
                println!("      ▶ component ({}): {}", component.module_name, address,);
            },
            SubstateValue::Resource(_) => {
                println!("      ▶ resource: {}", address);
            },
            SubstateValue::TransactionReceipt(_) => {
                println!("      ▶ transaction_receipt: {}", address);
            },
            SubstateValue::Vault(vault) => {
                println!("      ▶ vault: {} {}", address, vault.resource_address());
            },
            SubstateValue::NonFungible(_) => {
                println!("      ▶ NFT: {}", address);
            },
            SubstateValue::UnclaimedConfidentialOutput(_) => {
                println!("      ▶ Layer 1 commitment: {}", address);
            },
            SubstateValue::NonFungibleIndex(index) => {
                let referenced_address = SubstateId::from(index.referenced_address().clone());
                println!("      ▶ NFT index {} referencing {}", address, referenced_address);
            },
            SubstateValue::Template(_) => {
                println!("      ▶ Template: {}", address);
            },
            SubstateValue::ValidatorFeePool(pool) => {
                println!("      ▶ Validator Fee Pool: {}", address);
                println!("        ▶ Total fees: {}", pool.amount);
            },
        }
        println!();
    }
    for (address, version) in diff.down_iter() {
        println!("🗑️ DOWN substate {} v{}", address, version,);
        println!(
            "      🧩 Substate address: {}",
            SubstateAddress::from_substate_id(address, *version)
        );
        println!();
    }
}

fn print_reject_reason(reason: &RejectReason) {
    println!("❌️ Transaction rejected: {}", reason);
}

pub fn summarize_finalize_result(finalize: &FinalizeResult) {
    println!("========= Substates =========");
    match finalize.result {
        TransactionResult::Accept(ref diff) => print_substate_diff(diff),
        TransactionResult::AcceptFeeRejectRest(ref diff, ref reason) => {
            print_substate_diff(diff);
            print_reject_reason(reason);
        },
        TransactionResult::Reject(ref reason) => print_reject_reason(reason),
    }

    println!("========= Return Values =========");
    print_execution_results(&finalize.execution_results);

    println!();
    println!("========= LOGS =========");
    for log in &finalize.logs {
        println!("{}", log);
    }
}

fn display_vec<W: fmt::Write>(writer: &mut W, ty: &Type, result: &InstructionResult) -> fmt::Result {
    fn stringify_slice<T: fmt::Display>(slice: &[T]) -> String {
        slice.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(", ")
    }

    match &ty {
        Type::Unit => {},
        Type::Bool => {
            write!(writer, "{}", stringify_slice(&result.decode::<Vec<bool>>().unwrap()))?;
        },
        Type::I8 => {
            write!(writer, "{}", stringify_slice(&result.decode::<Vec<i8>>().unwrap()))?;
        },
        Type::I16 => {
            write!(writer, "{}", stringify_slice(&result.decode::<Vec<i16>>().unwrap()))?;
        },
        Type::I32 => {
            write!(writer, "{}", stringify_slice(&result.decode::<Vec<i32>>().unwrap()))?;
        },
        Type::I64 => {
            write!(writer, "{}", stringify_slice(&result.decode::<Vec<i64>>().unwrap()))?;
        },
        Type::I128 => {
            write!(writer, "{}", stringify_slice(&result.decode::<Vec<i128>>().unwrap()))?;
        },
        Type::U8 => {
            write!(writer, "{}", stringify_slice(&result.decode::<Vec<u8>>().unwrap()))?;
        },
        Type::U16 => {
            write!(writer, "{}", stringify_slice(&result.decode::<Vec<u16>>().unwrap()))?;
        },
        Type::U32 => {
            write!(writer, "{}", stringify_slice(&result.decode::<Vec<u32>>().unwrap()))?;
        },
        Type::U64 => {
            write!(writer, "{}", stringify_slice(&result.decode::<Vec<u64>>().unwrap()))?;
        },
        Type::U128 => {
            write!(writer, "{}", stringify_slice(&result.decode::<Vec<u128>>().unwrap()))?;
        },
        Type::String => {
            write!(writer, "{}", result.decode::<Vec<String>>().unwrap().join(", "))?;
        },
        Type::Vec(ty) => {
            let mut vec_ty = String::new();
            display_vec(&mut vec_ty, ty, result)?;
            match &**ty {
                Type::Other { name } => {
                    write!(writer, "Vec<{}>: {}", name, vec_ty)?;
                },
                _ => {
                    write!(writer, "Vec<{:?}>: {}", ty, vec_ty)?;
                },
            }
        },
        Type::Tuple(subtypes) => {
            let str = format_tuple(subtypes, result);
            write!(writer, "{}", str)?;
        },
        Type::Other { name } if name == "Amount" => {
            write!(writer, "{}", stringify_slice(&result.decode::<Vec<Amount>>().unwrap()))?;
        },
        Type::Other { name } if name == "NonFungibleId" => {
            write!(
                writer,
                "{}",
                stringify_slice(&result.decode::<Vec<NonFungibleId>>().unwrap())
            )?;
        },
        Type::Other { .. } => {
            write!(writer, "{}", serde_json::to_string_pretty(&result.indexed).unwrap())?;
        },
    }
    Ok(())
}

fn format_tuple(subtypes: &[Type], result: &InstructionResult) -> String {
    let tuple_type = Type::Tuple(subtypes.to_vec());
    let result_json = serde_json::to_string(&result.indexed).unwrap();
    format!("{}: {}", tuple_type, result_json)
}

pub fn print_execution_results(results: &[InstructionResult]) {
    for result in results {
        match &result.return_type {
            Type::Unit => {},
            Type::Bool => {
                println!("bool: {}", result.decode::<bool>().unwrap());
            },
            Type::I8 => {
                println!("i8: {}", result.decode::<i8>().unwrap());
            },
            Type::I16 => {
                println!("i16: {}", result.decode::<i16>().unwrap());
            },
            Type::I32 => {
                println!("i32: {}", result.decode::<i32>().unwrap());
            },
            Type::I64 => {
                println!("i64: {}", result.decode::<i64>().unwrap());
            },
            Type::I128 => {
                println!("i128: {}", result.decode::<i128>().unwrap());
            },
            Type::U8 => {
                println!("u8: {}", result.decode::<u8>().unwrap());
            },
            Type::U16 => {
                println!("u16: {}", result.decode::<u16>().unwrap());
            },
            Type::U32 => {
                println!("u32: {}", result.decode::<u32>().unwrap());
            },
            Type::U64 => {
                println!("u64: {}", result.decode::<u64>().unwrap());
            },
            Type::U128 => {
                println!("u128: {}", result.decode::<u128>().unwrap());
            },
            Type::String => {
                println!("string: {}", result.decode::<String>().unwrap());
            },
            Type::Vec(ty) => {
                let mut vec_ty = String::new();
                display_vec(&mut vec_ty, ty, result).unwrap();
                match &**ty {
                    Type::Other { name } => {
                        println!("Vec<{}>: {}", name, vec_ty);
                    },
                    _ => {
                        println!("Vec<{:?}>: {}", ty, vec_ty);
                    },
                }
            },
            Type::Tuple(subtypes) => {
                let str = format_tuple(subtypes, result);
                println!("{}", str);
            },
            Type::Other { ref name } if name == "Amount" => {
                println!("{}: {}", name, result.decode::<Amount>().unwrap());
            },
            Type::Other { ref name } if name == "Bucket" => {
                println!("{}: {}", name, result.decode::<BucketId>().unwrap());
            },
            Type::Other { ref name } => {
                println!("{}: {}", name, serde_json::to_string_pretty(&result.indexed).unwrap());
            },
        }
    }
}

#[derive(Debug, Clone)]
pub enum CliArg {
    String(String),
    U64(u64),
    U32(u32),
    U16(u16),
    U8(u8),
    I64(i64),
    I32(i32),
    I16(i16),
    I8(i8),
    Bool(bool),
    Blob(tari_bor::Value),
    NonFungibleId(NonFungibleId),
    SubstateId(SubstateId),
    TemplateAddress(TemplateAddress),
}

impl FromStr for CliArg {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(file) = s.strip_prefix('@') {
            let base64_data = fs::read_to_string(file).map_err(|e| anyhow!("Failed to read file {}: {}", file, e))?;
            return Ok(CliArg::Blob(decode_exact(&base64::decode(base64_data)?)?));
        }

        if let Ok(v) = s.parse::<u64>() {
            return Ok(CliArg::U64(v));
        }
        if let Ok(v) = s.parse::<u32>() {
            return Ok(CliArg::U32(v));
        }
        if let Ok(v) = s.parse::<u16>() {
            return Ok(CliArg::U16(v));
        }
        if let Ok(v) = s.parse::<u8>() {
            return Ok(CliArg::U8(v));
        }
        if let Ok(v) = s.parse::<i64>() {
            return Ok(CliArg::I64(v));
        }
        if let Ok(v) = s.parse::<i32>() {
            return Ok(CliArg::I32(v));
        }
        if let Ok(v) = s.parse::<i16>() {
            return Ok(CliArg::I16(v));
        }
        if let Ok(v) = s.parse::<i8>() {
            return Ok(CliArg::I8(v));
        }
        if let Ok(v) = s.parse::<bool>() {
            return Ok(CliArg::Bool(v));
        }

        if let Ok(v) = s.parse::<SubstateId>() {
            return Ok(CliArg::SubstateId(v));
        }

        if let Some(v) = parse_template_address(s) {
            return Ok(CliArg::TemplateAddress(v));
        }

        if let Some(("nft", nft_id)) = s.split_once('_') {
            match NonFungibleId::try_from_canonical_string(nft_id) {
                Ok(v) => {
                    return Ok(CliArg::NonFungibleId(v));
                },
                Err(e) => {
                    eprintln!(
                        "WARN: '{}' is not a valid NonFungibleId ({:?}) and will be interpreted as a string",
                        s, e
                    );
                },
            }
        }
        Ok(CliArg::String(s.to_string()))
    }
}

impl CliArg {
    pub fn into_arg(self) -> Arg {
        match self {
            CliArg::String(s) => arg!(s),
            CliArg::U64(v) => arg!(v),
            CliArg::U32(v) => arg!(v),
            CliArg::U16(v) => arg!(v),
            CliArg::U8(v) => arg!(v),
            CliArg::I64(v) => arg!(v),
            CliArg::I32(v) => arg!(v),
            CliArg::I16(v) => arg!(v),
            CliArg::I8(v) => arg!(v),
            CliArg::Bool(v) => arg!(v),
            CliArg::Blob(v) => Arg::literal(v).unwrap(),
            CliArg::SubstateId(v) => match v {
                SubstateId::Component(v) => arg!(v),
                SubstateId::Resource(v) => arg!(v),
                SubstateId::Vault(v) => arg!(v),
                SubstateId::UnclaimedConfidentialOutput(v) => arg!(v),
                SubstateId::NonFungible(v) => arg!(v),
                SubstateId::NonFungibleIndex(v) => arg!(v),
                SubstateId::TransactionReceipt(v) => arg!(v),
                SubstateId::Template(v) => arg!(v),
                SubstateId::ValidatorFeePool(v) => arg!(v),
            },
            CliArg::TemplateAddress(v) => arg!(v),
            CliArg::NonFungibleId(v) => arg!(v),
        }
    }
}

#[derive(Debug, Clone)]
pub struct NewResourceOutput {
    pub template_address: TemplateAddress,
    pub token_symbol: String,
}

impl FromStr for NewResourceOutput {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (template_address, token_symbol) = s
            .split_once(':')
            .ok_or_else(|| anyhow!("Expected template address and token symbol"))?;
        let template_address = TemplateAddress::from_hex(template_address)?;
        Ok(NewResourceOutput {
            template_address,
            token_symbol: token_symbol.to_string(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct SpecificNonFungibleMintOutput {
    pub resource_address: ResourceAddress,
    pub non_fungible_id: NonFungibleId,
}

impl SpecificNonFungibleMintOutput {
    pub fn to_substate_address(&self) -> SubstateId {
        SubstateId::NonFungible(NonFungibleAddress::new(
            self.resource_address,
            self.non_fungible_id.clone(),
        ))
    }
}

impl FromStr for SpecificNonFungibleMintOutput {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (resource_address, non_fungible_id) = s
            .split_once(',')
            .ok_or_else(|| anyhow!("Expected resource address and non-fungible id"))?;
        let resource_address = SubstateId::from_str(resource_address)?;
        let resource_address = resource_address
            .as_resource_address()
            .ok_or_else(|| anyhow!("Expected resource address but got {}", resource_address))?;
        let non_fungible_id = non_fungible_id
            .split_once('_')
            .map(|(_, b)| b)
            .unwrap_or(non_fungible_id);
        let non_fungible_id =
            NonFungibleId::try_from_canonical_string(non_fungible_id).map_err(|e| anyhow!("{:?}", e))?;
        Ok(SpecificNonFungibleMintOutput {
            resource_address,
            non_fungible_id,
        })
    }
}

#[derive(Debug, Clone)]
pub struct NewNonFungibleMintOutput {
    pub resource_address: ResourceAddress,
    pub count: u8,
}

impl FromStr for NewNonFungibleMintOutput {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (resource_address, count_str) = s.split_once(',').unwrap_or((s, "1"));
        let resource_address = SubstateId::from_str(resource_address)?;
        let resource_address = resource_address
            .as_resource_address()
            .ok_or_else(|| anyhow!("Expected resource address but got {}", resource_address))?;
        Ok(NewNonFungibleMintOutput {
            resource_address,
            count: count_str.parse()?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct NewNonFungibleIndexOutput {
    pub parent_address: ResourceAddress,
    pub index: u64,
}

impl FromStr for NewNonFungibleIndexOutput {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (parent_address, index_str) = s.split_once(',').unwrap_or((s, "0"));
        let parent_address = SubstateId::from_str(parent_address)?;
        let parent_address = parent_address
            .as_resource_address()
            .ok_or_else(|| anyhow!("Expected resource address but got {}", parent_address))?;
        Ok(NewNonFungibleIndexOutput {
            parent_address,
            index: index_str.parse()?,
        })
    }
}

fn parse_globals(globals: Vec<String>) -> Result<HashMap<String, ManifestValue>, anyhow::Error> {
    let mut result = HashMap::with_capacity(globals.len());
    for global in globals {
        let (name, value) = global
            .split_once('=')
            .ok_or_else(|| anyhow!("Invalid global: {}", global))?;
        if let Ok(url) = url::Url::parse(value) {
            let blob = match url.scheme() {
                "file" => {
                    let contents = fs::read_to_string(url.path())
                        .map_err(|err| anyhow!("Failed to read file '{}': {}", &url, err))?;

                    base64::decode(contents.trim())
                        .map_err(|err| anyhow!("Failed to decode base64 file '{}': {}", url, err))?
                },
                "data" => {
                    base64::decode(url.path()).map_err(|err| anyhow!("Failed to decode base64 '{}': {}", url, err))?
                },
                scheme => anyhow::bail!("Unsupported scheme '{}'", scheme),
            };
            result.insert(name.to_string(), ManifestValue::Value(decode_exact(&blob)?));
        } else {
            let value = value
                .parse()
                .map_err(|err| anyhow!("Failed to parse global '{}': {}", name, err))?;
            result.insert(name.to_string(), value);
        }
    }
    Ok(result)
}
