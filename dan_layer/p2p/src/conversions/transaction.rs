//  Copyright 2023, The Tari Project
//
//  Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
//  following conditions are met:
//
//  1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following
//  disclaimer.
//
//  2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the
//  following disclaimer in the documentation and/or other materials provided with the distribution.
//
//  3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote
//  products derived from this software without specific prior written permission.
//
//  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
//  INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
//  DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
//  SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
//  SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
//  WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
//  USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

use std::convert::{TryFrom, TryInto};

use anyhow::anyhow;
use tari_bor::{decode_exact, encode};
use tari_common_types::types::{Commitment, PrivateKey, PublicKey};
use tari_crypto::{ristretto::RistrettoComSig, tari_utilities::ByteArray};
use tari_dan_common_types::{SubstateRequirement, VersionedSubstateId};
use tari_engine_types::{confidential::ConfidentialClaim, instruction::Instruction, substate::SubstateId};
use tari_template_lib::{
    args::Arg,
    auth::OwnerRule,
    crypto::{BalanceProofSignature, PedersonCommitmentBytes, RistrettoPublicKeyBytes},
    models::{
        Amount,
        ConfidentialOutputStatement,
        ConfidentialStatement,
        ConfidentialWithdrawProof,
        EncryptedData,
        ObjectKey,
        ViewableBalanceProof,
    },
    prelude::AccessRules,
};
use tari_transaction::Transaction;

use crate::{
    proto::{
        self,
        transaction::{instruction::InstructionType, OptionalVersion},
    },
    utils::checked_copy_fixed,
    NewTransactionMessage,
};

// -------------------------------- NewTransactionMessage -------------------------------- //

impl From<NewTransactionMessage> for proto::transaction::NewTransactionMessage {
    fn from(msg: NewTransactionMessage) -> Self {
        Self {
            transaction: Some((&msg.transaction).into()),
        }
    }
}

impl TryFrom<proto::transaction::NewTransactionMessage> for NewTransactionMessage {
    type Error = anyhow::Error;

    fn try_from(value: proto::transaction::NewTransactionMessage) -> Result<Self, Self::Error> {
        Ok(NewTransactionMessage {
            transaction: value
                .transaction
                .ok_or_else(|| anyhow!("Transaction not provided"))?
                .try_into()?,
        })
    }
}

//---------------------------------- Transaction --------------------------------------------//
impl TryFrom<proto::transaction::Transaction> for Transaction {
    type Error = anyhow::Error;

    fn try_from(transaction: proto::transaction::Transaction) -> Result<Self, Self::Error> {
        decode_exact(&transaction.bor_encoded).map_err(|e| anyhow!("tari_bor::decode failed for transaction: {}", e))
    }
}

impl From<&Transaction> for proto::transaction::Transaction {
    fn from(transaction: &Transaction) -> Self {
        proto::transaction::Transaction {
            bor_encoded: encode(transaction).expect("tari_bor::encode failed for transaction"),
        }
    }
}

//---------------------------------- UnsignedTransaction --------------------------------------------//

// impl TryFrom<proto::transaction::UnsignedTransactionV1> for UnsignedTransactionV1 {
//     type Error = anyhow::Error;
//
//     fn try_from(request: proto::transaction::UnsignedTransactionV1) -> Result<Self, Self::Error> {
//         let instructions = request
//             .instructions
//             .into_iter()
//             .map(TryInto::try_into)
//             .collect::<Result<Vec<_>, _>>()?;
//
//         let fee_instructions = request
//             .fee_instructions
//             .into_iter()
//             .map(TryInto::try_into)
//             .collect::<Result<Vec<_>, _>>()?;
//
//         let inputs = request
//             .inputs
//             .into_iter()
//             .map(TryInto::try_into)
//             .collect::<Result<_, _>>()?;
//
//         let min_epoch = request.min_epoch.map(|epoch| Epoch(epoch.epoch));
//         let max_epoch = request.max_epoch.map(|epoch| Epoch(epoch.epoch));
//         Ok(Self {
//             fee_instructions,
//             instructions,
//             inputs,
//             min_epoch,
//             max_epoch,
//         })
//     }
// }
//
// impl From<&UnsignedTransactionV1> for proto::transaction::UnsignedTransaction {
//     fn from(transaction: &UnsignedTransactionV1) -> Self {
//         let inputs = transaction.inputs().iter().map(Into::into).collect();
//         let min_epoch = transaction
//             .min_epoch()
//             .map(|epoch| proto::common::Epoch { epoch: epoch.0 });
//         let max_epoch = transaction
//             .max_epoch()
//             .map(|epoch| proto::common::Epoch { epoch: epoch.0 });
//         let fee_instructions = transaction.fee_instructions().iter().cloned().map(Into::into).collect();
//         let instructions = transaction.instructions().iter().cloned().map(Into::into).collect();
//
//         proto::transaction::UnsignedTransaction {
//             fee_instructions,
//             instructions,
//             inputs,
//             min_epoch,
//             max_epoch,
//         }
//     }
// }

// -------------------------------- Instruction -------------------------------- //

impl TryFrom<proto::transaction::Instruction> for Instruction {
    type Error = anyhow::Error;

    fn try_from(request: proto::transaction::Instruction) -> Result<Self, Self::Error> {
        let args = request
            .args
            .into_iter()
            .map(|a| a.try_into())
            .collect::<Result<_, _>>()?;
        let instruction_type =
            InstructionType::try_from(request.instruction_type).map_err(|e| anyhow!("invalid instruction_type {e}"))?;

        let instruction = match instruction_type {
            InstructionType::CreateAccount => Instruction::CreateAccount {
                public_key_address: PublicKey::from_canonical_bytes(&request.create_account_public_key)
                    .map_err(|e| anyhow!("create_account_public_key: {}", e))?,
                owner_rule: request.create_account_owner_rule.map(TryInto::try_into).transpose()?,
                access_rules: request.create_account_access_rules.map(TryInto::try_into).transpose()?,
                workspace_bucket: Some(request.create_account_workspace_bucket).filter(|s| !s.is_empty()),
            },
            InstructionType::Function => {
                let function = request.function;
                Instruction::CallFunction {
                    template_address: request.template_address.try_into()?,
                    function,
                    args,
                }
            },
            InstructionType::Method => {
                let method = request.method;
                let component_address = ObjectKey::try_from(request.component_address)?.into();
                Instruction::CallMethod {
                    component_address,
                    method,
                    args,
                }
            },
            InstructionType::PutOutputInWorkspace => {
                Instruction::PutLastInstructionOutputOnWorkspace { key: request.key }
            },
            InstructionType::EmitLog => Instruction::EmitLog {
                level: request.log_level.parse()?,
                message: request.log_message,
            },
            InstructionType::ClaimBurn => Instruction::ClaimBurn {
                claim: Box::new(ConfidentialClaim {
                    public_key: PublicKey::from_canonical_bytes(&request.claim_burn_public_key)
                        .map_err(|e| anyhow!("claim_burn_public_key: {}", e))?,
                    output_address: request
                        .claim_burn_commitment_address
                        .as_slice()
                        .try_into()
                        .map_err(|e| anyhow!("claim_burn_commitment_address: {}", e))?,
                    range_proof: request.claim_burn_range_proof,
                    proof_of_knowledge: request
                        .claim_burn_proof_of_knowledge
                        .ok_or_else(|| anyhow!("claim_burn_proof_of_knowledge not provided"))?
                        .try_into()
                        .map_err(|e| anyhow!("claim_burn_proof_of_knowledge: {}", e))?,
                    withdraw_proof: request.claim_burn_withdraw_proof.map(TryInto::try_into).transpose()?,
                }),
            },
            InstructionType::ClaimValidatorFees => Instruction::ClaimValidatorFees {
                address: request
                    .claim_validator_fees_address
                    .as_slice()
                    .try_into()
                    .map_err(|e| anyhow!("claim_validator_fees_address: {e}"))?,
            },
            InstructionType::DropAllProofsInWorkspace => Instruction::DropAllProofsInWorkspace,
            InstructionType::AssertBucketContains => {
                let resource_address = ObjectKey::try_from(request.resource_address)?.into();
                Instruction::AssertBucketContains {
                    key: request.key,
                    resource_address,
                    min_amount: Amount::new(request.min_amount),
                }
            },
            InstructionType::PublishTemplate => Instruction::PublishTemplate {
                binary: request.template_binary,
            },
        };

        Ok(instruction)
    }
}

impl From<Instruction> for proto::transaction::Instruction {
    fn from(instruction: Instruction) -> Self {
        let mut result = proto::transaction::Instruction::default();

        match instruction {
            Instruction::CreateAccount {
                public_key_address,
                owner_rule,
                access_rules,
                workspace_bucket,
            } => {
                result.instruction_type = InstructionType::CreateAccount as i32;
                result.create_account_public_key = public_key_address.to_vec();
                result.create_account_owner_rule = owner_rule.map(Into::into);
                result.create_account_access_rules = access_rules.map(Into::into);
                result.create_account_workspace_bucket = workspace_bucket.unwrap_or_default();
            },
            Instruction::CallFunction {
                template_address,
                function,
                args,
            } => {
                result.instruction_type = InstructionType::Function as i32;
                result.template_address = template_address.to_vec();
                result.function = function;
                result.args = args.into_iter().map(|a| a.into()).collect();
            },
            Instruction::CallMethod {
                component_address,
                method,
                args,
            } => {
                result.instruction_type = InstructionType::Method as i32;
                result.component_address = component_address.as_bytes().to_vec();
                result.method = method;
                result.args = args.into_iter().map(|a| a.into()).collect();
            },
            Instruction::PutLastInstructionOutputOnWorkspace { key } => {
                result.instruction_type = InstructionType::PutOutputInWorkspace as i32;
                result.key = key;
            },
            Instruction::EmitLog { level, message } => {
                result.instruction_type = InstructionType::EmitLog as i32;
                result.log_level = level.to_string();
                result.log_message = message;
            },
            Instruction::ClaimBurn { claim } => {
                result.instruction_type = InstructionType::ClaimBurn as i32;
                result.claim_burn_commitment_address = claim.output_address.as_bytes().to_vec();
                result.claim_burn_range_proof = claim.range_proof.to_vec();
                result.claim_burn_proof_of_knowledge = Some(claim.proof_of_knowledge.into());
                result.claim_burn_public_key = claim.public_key.to_vec();
                result.claim_burn_withdraw_proof = claim.withdraw_proof.map(Into::into);
            },
            Instruction::ClaimValidatorFees { address } => {
                result.instruction_type = InstructionType::ClaimValidatorFees as i32;
                result.claim_validator_fees_address = address.as_slice().to_vec();
            },
            Instruction::DropAllProofsInWorkspace => {
                result.instruction_type = InstructionType::DropAllProofsInWorkspace as i32;
            },
            Instruction::AssertBucketContains {
                key,
                resource_address,
                min_amount,
            } => {
                result.instruction_type = InstructionType::AssertBucketContains as i32;
                result.key = key;
                result.resource_address = resource_address.as_bytes().to_vec();
                result.min_amount = min_amount.0
            },
            Instruction::PublishTemplate { binary } => {
                result.instruction_type = InstructionType::PublishTemplate as i32;
                result.template_binary = binary;
            },
        }
        result
    }
}

// -------------------------------- Arg -------------------------------- //

impl TryFrom<proto::transaction::Arg> for Arg {
    type Error = anyhow::Error;

    fn try_from(request: proto::transaction::Arg) -> Result<Self, Self::Error> {
        let data = request.data;
        let arg = match request.arg_type {
            0 => Arg::Literal(decode_exact(&data)?),
            1 => Arg::Workspace(data),
            _ => return Err(anyhow!("invalid arg_type")),
        };

        Ok(arg)
    }
}

impl From<Arg> for proto::transaction::Arg {
    fn from(arg: Arg) -> Self {
        let mut result = proto::transaction::Arg::default();

        match arg {
            Arg::Literal(data) => {
                result.arg_type = 0;
                result.data = tari_bor::encode(&data).unwrap();
            },
            Arg::Workspace(data) => {
                result.arg_type = 1;
                result.data = data;
            },
        }

        result
    }
}

// -------------------------------- SubstateRequirement -------------------------------- //
impl TryFrom<proto::transaction::SubstateRequirement> for SubstateRequirement {
    type Error = anyhow::Error;

    fn try_from(val: proto::transaction::SubstateRequirement) -> Result<Self, Self::Error> {
        let substate_id = SubstateId::from_bytes(&val.substate_id)?;
        let version = val.version.map(|v| v.version);
        let substate_specification = SubstateRequirement::new(substate_id, version);
        Ok(substate_specification)
    }
}

impl From<SubstateRequirement> for proto::transaction::SubstateRequirement {
    fn from(val: SubstateRequirement) -> Self {
        (&val).into()
    }
}

impl From<&SubstateRequirement> for proto::transaction::SubstateRequirement {
    fn from(val: &SubstateRequirement) -> Self {
        Self {
            substate_id: val.substate_id().to_bytes(),
            version: val.version().map(|v| OptionalVersion { version: v }),
        }
    }
}

// -------------------------------- VersionedSubstate -------------------------------- //

impl TryFrom<proto::transaction::VersionedSubstateId> for VersionedSubstateId {
    type Error = anyhow::Error;

    fn try_from(val: proto::transaction::VersionedSubstateId) -> Result<Self, Self::Error> {
        let substate_id = SubstateId::from_bytes(&val.substate_id)?;
        let substate_specification = VersionedSubstateId::new(substate_id, val.version);
        Ok(substate_specification)
    }
}

impl From<VersionedSubstateId> for proto::transaction::VersionedSubstateId {
    fn from(val: VersionedSubstateId) -> Self {
        (&val).into()
    }
}

impl From<&VersionedSubstateId> for proto::transaction::VersionedSubstateId {
    fn from(val: &VersionedSubstateId) -> Self {
        Self {
            substate_id: val.substate_id().to_bytes(),
            version: val.version(),
        }
    }
}

// -------------------------------- CommitmentSignature -------------------------------- //

impl TryFrom<proto::transaction::CommitmentSignature> for RistrettoComSig {
    type Error = anyhow::Error;

    fn try_from(val: proto::transaction::CommitmentSignature) -> Result<Self, Self::Error> {
        let u = PrivateKey::from_canonical_bytes(&val.signature_u).map_err(anyhow::Error::msg)?;
        let v = PrivateKey::from_canonical_bytes(&val.signature_v).map_err(anyhow::Error::msg)?;
        let public_nonce = PublicKey::from_canonical_bytes(&val.public_nonce_commitment).map_err(anyhow::Error::msg)?;

        Ok(RistrettoComSig::new(Commitment::from_public_key(&public_nonce), u, v))
    }
}

impl From<RistrettoComSig> for proto::transaction::CommitmentSignature {
    fn from(val: RistrettoComSig) -> Self {
        Self {
            public_nonce_commitment: val.public_nonce().to_vec(),
            signature_u: val.u().to_vec(),
            signature_v: val.v().to_vec(),
        }
    }
}
// -------------------------------- ConfidentialWithdrawProof -------------------------------- //

impl TryFrom<proto::transaction::ConfidentialWithdrawProof> for ConfidentialWithdrawProof {
    type Error = anyhow::Error;

    fn try_from(val: proto::transaction::ConfidentialWithdrawProof) -> Result<Self, Self::Error> {
        Ok(ConfidentialWithdrawProof {
            inputs: val
                .inputs
                .into_iter()
                .map(|v| {
                    PedersonCommitmentBytes::from_bytes(&v).map_err(|e| anyhow!("Invalid input commitment bytes: {e}"))
                })
                .collect::<Result<_, _>>()?,
            input_revealed_amount: val.input_revealed_amount.try_into()?,
            output_proof: val
                .output_proof
                .ok_or_else(|| anyhow!("output_proof is missing"))?
                .try_into()?,
            balance_proof: BalanceProofSignature::from_bytes(&val.balance_proof)
                .map_err(|e| anyhow!("Invalid balance proof signature: {}", e.to_error_string()))?,
        })
    }
}

impl From<ConfidentialWithdrawProof> for proto::transaction::ConfidentialWithdrawProof {
    fn from(val: ConfidentialWithdrawProof) -> Self {
        Self {
            inputs: val.inputs.iter().map(|v| v.as_bytes().to_vec()).collect(),
            input_revealed_amount: val
                .input_revealed_amount
                .as_u64_checked()
                .expect("input_revealed_amount is negative or too large"),
            output_proof: Some(val.output_proof.into()),
            balance_proof: val.balance_proof.as_bytes().to_vec(),
        }
    }
}

// -------------------------------- ConfidentialOutputStatement -------------------------------- //

impl TryFrom<proto::transaction::ConfidentialOutputStatement> for ConfidentialOutputStatement {
    type Error = anyhow::Error;

    fn try_from(val: proto::transaction::ConfidentialOutputStatement) -> Result<Self, Self::Error> {
        Ok(ConfidentialOutputStatement {
            output_statement: val.output_statement.map(TryInto::try_into).transpose()?,
            change_statement: val.change_statement.map(TryInto::try_into).transpose()?,
            range_proof: val.range_proof,
            output_revealed_amount: val.output_revealed_amount.try_into()?,
            change_revealed_amount: val.change_revealed_amount.try_into()?,
        })
    }
}

impl From<ConfidentialOutputStatement> for proto::transaction::ConfidentialOutputStatement {
    fn from(val: ConfidentialOutputStatement) -> Self {
        Self {
            output_statement: val.output_statement.map(Into::into),
            change_statement: val.change_statement.map(Into::into),
            range_proof: val.range_proof,
            output_revealed_amount: val
                .output_revealed_amount
                .as_u64_checked()
                .expect("output_revealed_amount is negative or too large"),
            change_revealed_amount: val
                .change_revealed_amount
                .as_u64_checked()
                .expect("change_revealed_amount is negative or too large"),
        }
    }
}

// -------------------------------- ConfidentialStatement -------------------------------- //

impl TryFrom<proto::transaction::ConfidentialStatement> for ConfidentialStatement {
    type Error = anyhow::Error;

    fn try_from(val: proto::transaction::ConfidentialStatement) -> Result<Self, Self::Error> {
        let sender_public_nonce = Some(val.sender_public_nonce)
            .filter(|v| !v.is_empty())
            .map(|v| {
                RistrettoPublicKeyBytes::from_bytes(&v)
                    .map_err(|e| anyhow!("Invalid sender_public_nonce: {}", e.to_error_string()))
            })
            .transpose()?
            .ok_or_else(|| anyhow!("sender_public_nonce is missing"))?;

        Ok(ConfidentialStatement {
            commitment: checked_copy_fixed(&val.commitment)
                .ok_or_else(|| anyhow!("Invalid length of commitment bytes"))?,
            sender_public_nonce,
            encrypted_data: EncryptedData::try_from(val.encrypted_value)
                .map_err(|len| anyhow!("Invalid length ({len}) of encrypted_value bytes"))?,
            minimum_value_promise: val.minimum_value_promise,
            viewable_balance_proof: val.viewable_balance_proof.map(TryInto::try_into).transpose()?,
        })
    }
}

impl From<ConfidentialStatement> for proto::transaction::ConfidentialStatement {
    fn from(val: ConfidentialStatement) -> Self {
        Self {
            commitment: val.commitment.to_vec(),
            sender_public_nonce: val.sender_public_nonce.as_bytes().to_vec(),
            encrypted_value: val.encrypted_data.as_ref().to_vec(),
            minimum_value_promise: val.minimum_value_promise,
            viewable_balance_proof: val.viewable_balance_proof.map(Into::into),
        }
    }
}

// -------------------------------- ViewableBalanceProof -------------------------------- //

impl TryFrom<proto::transaction::ViewableBalanceProof> for ViewableBalanceProof {
    type Error = anyhow::Error;

    fn try_from(val: proto::transaction::ViewableBalanceProof) -> Result<Self, Self::Error> {
        Ok(ViewableBalanceProof {
            elgamal_encrypted: val.elgamal_encrypted.as_slice().try_into()?,
            elgamal_public_nonce: val.elgamal_public_nonce.as_slice().try_into()?,
            c_prime: val.c_prime.as_slice().try_into()?,
            e_prime: val.e_prime.as_slice().try_into()?,
            r_prime: val.r_prime.as_slice().try_into()?,
            s_v: val.s_v.as_slice().try_into()?,
            s_m: val.s_m.as_slice().try_into()?,
            s_r: val.s_r.as_slice().try_into()?,
        })
    }
}

impl From<ViewableBalanceProof> for proto::transaction::ViewableBalanceProof {
    fn from(val: ViewableBalanceProof) -> Self {
        Self {
            elgamal_encrypted: val.elgamal_encrypted.as_bytes().to_vec(),
            elgamal_public_nonce: val.elgamal_public_nonce.as_bytes().to_vec(),
            c_prime: val.c_prime.as_bytes().to_vec(),
            e_prime: val.e_prime.as_bytes().to_vec(),
            r_prime: val.r_prime.as_bytes().to_vec(),
            s_v: val.s_v.as_bytes().to_vec(),
            s_m: val.s_m.as_bytes().to_vec(),
            s_r: val.s_r.as_bytes().to_vec(),
        }
    }
}

// -------------------------------- OwnerRule -------------------------------- //

impl From<OwnerRule> for proto::transaction::OwnerRule {
    fn from(value: OwnerRule) -> Self {
        Self {
            encoded_owner_rule: encode(&value).unwrap(),
        }
    }
}

impl TryFrom<proto::transaction::OwnerRule> for OwnerRule {
    type Error = anyhow::Error;

    fn try_from(value: proto::transaction::OwnerRule) -> Result<Self, Self::Error> {
        Ok(decode_exact(&value.encoded_owner_rule)?)
    }
}

// -------------------------------- AccessRules -------------------------------- //

impl From<AccessRules> for proto::transaction::AccessRules {
    fn from(value: AccessRules) -> Self {
        Self {
            encoded_access_rules: encode(&value).unwrap(),
        }
    }
}

impl TryFrom<proto::transaction::AccessRules> for AccessRules {
    type Error = anyhow::Error;

    fn try_from(value: proto::transaction::AccessRules) -> Result<Self, Self::Error> {
        Ok(decode_exact(&value.encoded_access_rules)?)
    }
}
