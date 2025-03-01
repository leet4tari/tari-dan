//   Copyright 2023 The Tari Project
//   SPDX-License-Identifier: BSD-3-Clause

use std::{collections::BTreeMap, ops::ControlFlow};

use serde::{Deserialize, Serialize};
use tari_bor::{decode, BorError, FromTagAndValue, ValueVisitor};
use tari_template_lib::{
    models::{
        BinaryTag,
        BucketId,
        NonFungibleAddressContents,
        ObjectKey,
        ProofId,
        ResourceAddress,
        UnclaimedConfidentialOutputAddress,
        VaultId,
    },
    prelude::{ComponentAddress, Metadata, NonFungibleAddress},
    Hash,
};

use crate::{
    published_template::PublishedTemplateAddress,
    serde_with,
    substate::SubstateId,
    transaction_receipt::TransactionReceiptAddress,
    vn_fee_pool::ValidatorFeePoolAddress,
};

const MAX_VISITOR_DEPTH: usize = 50;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(
    feature = "ts",
    derive(ts_rs::TS),
    ts(export, export_to = "../../bindings/src/types/")
)]
pub struct IndexedValue {
    indexed: IndexedWellKnownTypes,
    #[serde(with = "serde_with::cbor_value")]
    #[cfg_attr(feature = "ts", ts(type = "any"))]
    value: tari_bor::Value,
}

impl IndexedValue {
    pub fn from_type<T: Serialize + ?Sized>(v: &T) -> Result<Self, IndexedValueError> {
        let value = tari_bor::to_value(v)?;
        Self::from_value(value)
    }

    pub fn from_raw(bytes: &[u8]) -> Result<Self, IndexedValueError> {
        if bytes.is_empty() {
            return Ok(Self::default());
        }
        let value: tari_bor::Value = decode(bytes)?;
        Self::from_value(value)
    }

    pub fn from_value(value: tari_bor::Value) -> Result<Self, IndexedValueError> {
        let indexed = IndexedWellKnownTypes::from_value(&value)?;
        Ok(Self { indexed, value })
    }

    pub fn referenced_substates(&self) -> impl Iterator<Item = SubstateId> + '_ {
        self.indexed
            .component_addresses
            .iter()
            .map(|a| (*a).into())
            .chain(self.indexed.resource_addresses.iter().map(|a| (*a).into()))
            .chain(self.indexed.non_fungible_addresses.iter().map(|a| a.clone().into()))
            .chain(self.indexed.vault_ids.iter().map(|a| (*a).into()))
    }

    pub fn well_known_types(&self) -> &IndexedWellKnownTypes {
        &self.indexed
    }

    pub fn bucket_ids(&self) -> &[BucketId] {
        &self.indexed.bucket_ids
    }

    pub fn proof_ids(&self) -> &[ProofId] {
        &self.indexed.proof_ids
    }

    pub fn component_addresses(&self) -> &[ComponentAddress] {
        &self.indexed.component_addresses
    }

    pub fn resource_addresses(&self) -> &[ResourceAddress] {
        &self.indexed.resource_addresses
    }

    pub fn non_fungible_addresses(&self) -> &[NonFungibleAddress] {
        &self.indexed.non_fungible_addresses
    }

    pub fn vault_ids(&self) -> &[VaultId] {
        &self.indexed.vault_ids
    }

    pub fn metadata(&self) -> &[Metadata] {
        &self.indexed.metadata
    }

    pub fn value(&self) -> &tari_bor::Value {
        &self.value
    }

    pub fn into_value(self) -> tari_bor::Value {
        self.value
    }

    pub fn get_value<T>(&self, path: &str) -> Result<Option<T>, IndexedValueError>
    where for<'a> T: serde::Deserialize<'a> {
        decode_value_at_path(&self.value, path)
    }

    pub const fn empty() -> Self {
        Self {
            indexed: IndexedWellKnownTypes::new(),
            value: tari_bor::Value::Null,
        }
    }
}

impl Default for IndexedValue {
    fn default() -> Self {
        Self::empty()
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[cfg_attr(
    feature = "ts",
    derive(ts_rs::TS),
    ts(export, export_to = "../../bindings/src/types/")
)]
pub struct IndexedWellKnownTypes {
    bucket_ids: Vec<BucketId>,
    proof_ids: Vec<ProofId>,
    component_addresses: Vec<ComponentAddress>,
    resource_addresses: Vec<ResourceAddress>,
    transaction_receipt_addresses: Vec<TransactionReceiptAddress>,
    // #[serde(with = "serde_with::hex::vec")]
    non_fungible_addresses: Vec<NonFungibleAddress>,
    vault_ids: Vec<VaultId>,
    metadata: Vec<Metadata>,
    unclaimed_confidential_output_address: Vec<UnclaimedConfidentialOutputAddress>,
    published_template_addresses: Vec<PublishedTemplateAddress>,
    validator_node_fee_pools: Vec<ValidatorFeePoolAddress>,
}

impl IndexedWellKnownTypes {
    pub const fn new() -> Self {
        Self {
            bucket_ids: vec![],
            proof_ids: vec![],
            component_addresses: vec![],
            resource_addresses: vec![],
            transaction_receipt_addresses: vec![],
            non_fungible_addresses: vec![],
            vault_ids: vec![],
            metadata: vec![],
            unclaimed_confidential_output_address: vec![],
            published_template_addresses: vec![],
            validator_node_fee_pools: vec![],
        }
    }

    pub fn from_value(value: &tari_bor::Value) -> Result<Self, IndexedValueError> {
        Self::from_value_with_max_depth(value, MAX_VISITOR_DEPTH)
    }

    pub fn from_value_with_max_depth(value: &tari_bor::Value, max_depth: usize) -> Result<Self, IndexedValueError> {
        let mut visitor = IndexedValueVisitor::new();
        tari_bor::walk_all(value, &mut visitor, max_depth)?;

        Ok(Self {
            bucket_ids: visitor.buckets,
            proof_ids: visitor.proofs,
            resource_addresses: visitor.resource_addresses,
            component_addresses: visitor.component_addresses,
            transaction_receipt_addresses: visitor.transaction_receipt_addresses,
            non_fungible_addresses: visitor.non_fungible_addresses,
            vault_ids: visitor.vault_ids,
            metadata: visitor.metadata,
            unclaimed_confidential_output_address: visitor.unclaimed_confidential_output_addresses,
            published_template_addresses: visitor.published_templates,
            validator_node_fee_pools: visitor.validator_node_fee_pools,
        })
    }

    /// Checks if a value contains a substate with the given address. This function does not allocate.
    pub fn value_contains_substate(value: &tari_bor::Value, address: &SubstateId) -> Result<bool, IndexedValueError> {
        let mut found = false;
        tari_bor::walk_all(
            value,
            &mut |value: WellKnownTariValue| {
                match value {
                    WellKnownTariValue::ComponentAddress(addr) => {
                        found = *address == addr;
                    },
                    WellKnownTariValue::ResourceAddress(addr) => {
                        found = *address == addr;
                    },
                    WellKnownTariValue::TransactionReceiptAddress(addr) => {
                        found = *address == addr;
                    },
                    WellKnownTariValue::NonFungibleAddress(addr) => {
                        found = *address == addr;
                    },
                    WellKnownTariValue::VaultId(addr) => {
                        found = *address == addr;
                    },
                    WellKnownTariValue::UnclaimedConfidentialOutputAddress(addr) => {
                        found = *address == addr;
                    },
                    WellKnownTariValue::PublishedTemplateAddress(addr) => {
                        found = *address == addr;
                    },
                    WellKnownTariValue::ValidatorNodeFeePool(addr) => {
                        found = *address == addr;
                    },
                    WellKnownTariValue::BucketId(_) |
                    WellKnownTariValue::Metadata(_) |
                    WellKnownTariValue::ProofId(_) => {},
                }

                if found {
                    Ok(ControlFlow::Break(()))
                } else {
                    Ok(ControlFlow::Continue(()))
                }
            },
            MAX_VISITOR_DEPTH,
        )?;
        Ok(found)
    }

    pub fn referenced_substates(&self) -> impl Iterator<Item = SubstateId> + '_ {
        self.component_addresses
            .iter()
            .map(|a| (*a).into())
            .chain(self.resource_addresses.iter().map(|a| (*a).into()))
            .chain(self.non_fungible_addresses.iter().map(|a| a.clone().into()))
            .chain(self.vault_ids.iter().map(|a| (*a).into()))
            .chain(self.unclaimed_confidential_output_address.iter().map(|a| (*a).into()))
            .chain(self.validator_node_fee_pools.iter().map(|a| (*a).into()))
    }

    pub fn bucket_ids(&self) -> &[BucketId] {
        &self.bucket_ids
    }

    pub fn proof_ids(&self) -> &[ProofId] {
        &self.proof_ids
    }

    pub fn component_addresses(&self) -> &[ComponentAddress] {
        &self.component_addresses
    }

    pub fn resource_addresses(&self) -> &[ResourceAddress] {
        &self.resource_addresses
    }

    pub fn non_fungible_addresses(&self) -> &[NonFungibleAddress] {
        &self.non_fungible_addresses
    }

    pub fn vault_ids(&self) -> &[VaultId] {
        &self.vault_ids
    }

    pub fn metadata(&self) -> &[Metadata] {
        &self.metadata
    }

    pub fn diff(&self, other: &Self) -> Self {
        Self {
            bucket_ids: diff_vec(&self.bucket_ids, &other.bucket_ids),
            proof_ids: diff_vec(&self.proof_ids, &other.proof_ids),
            component_addresses: diff_vec(&self.component_addresses, &other.component_addresses),
            resource_addresses: diff_vec(&self.resource_addresses, &other.resource_addresses),
            transaction_receipt_addresses: diff_vec(
                &self.transaction_receipt_addresses,
                &other.transaction_receipt_addresses,
            ),
            non_fungible_addresses: diff_vec(&self.non_fungible_addresses, &other.non_fungible_addresses),
            vault_ids: diff_vec(&self.vault_ids, &other.vault_ids),
            metadata: diff_vec(&self.metadata, &other.metadata),
            unclaimed_confidential_output_address: diff_vec(
                &self.unclaimed_confidential_output_address,
                &other.unclaimed_confidential_output_address,
            ),
            published_template_addresses: diff_vec(
                &self.published_template_addresses,
                &other.published_template_addresses,
            ),
            validator_node_fee_pools: diff_vec(&self.validator_node_fee_pools, &other.validator_node_fee_pools),
        }
    }
}

fn diff_vec<T: PartialEq + Clone>(a: &[T], b: &[T]) -> Vec<T> {
    a.iter().filter(|x| !b.contains(x)).cloned().collect()
}

impl FromIterator<IndexedWellKnownTypes> for IndexedWellKnownTypes {
    fn from_iter<T: IntoIterator<Item = IndexedWellKnownTypes>>(iter: T) -> Self {
        let mut indexed = Self::default();
        for value in iter {
            indexed.bucket_ids.extend(value.bucket_ids);
            indexed.proof_ids.extend(value.proof_ids);
            indexed.component_addresses.extend(value.component_addresses);
            indexed.resource_addresses.extend(value.resource_addresses);
            indexed
                .transaction_receipt_addresses
                .extend(value.transaction_receipt_addresses);
            indexed.non_fungible_addresses.extend(value.non_fungible_addresses);
            indexed.vault_ids.extend(value.vault_ids);
            indexed.metadata.extend(value.metadata);
            indexed
                .unclaimed_confidential_output_address
                .extend(value.unclaimed_confidential_output_address);
        }
        indexed
    }
}

pub enum WellKnownTariValue {
    ComponentAddress(ComponentAddress),
    ResourceAddress(ResourceAddress),
    TransactionReceiptAddress(TransactionReceiptAddress),
    NonFungibleAddress(NonFungibleAddress),
    BucketId(BucketId),
    Metadata(Metadata),
    VaultId(VaultId),
    ProofId(ProofId),
    UnclaimedConfidentialOutputAddress(UnclaimedConfidentialOutputAddress),
    PublishedTemplateAddress(PublishedTemplateAddress),
    ValidatorNodeFeePool(ValidatorFeePoolAddress),
}

impl FromTagAndValue for WellKnownTariValue {
    type Error = IndexedValueError;

    fn try_from_tag_and_value(tag: u64, value: &tari_bor::Value) -> Result<Self, Self::Error>
    where Self: Sized {
        let tag = BinaryTag::from_u64(tag).ok_or(IndexedValueError::InvalidTag(tag))?;
        match tag {
            BinaryTag::ComponentAddress => {
                let component_address: ObjectKey = value.deserialized().map_err(BorError::from)?;
                Ok(Self::ComponentAddress(component_address.into()))
            },
            BinaryTag::BucketId => {
                let bucket_id: u32 = value.deserialized().map_err(BorError::from)?;
                Ok(Self::BucketId(bucket_id.into()))
            },
            BinaryTag::ResourceAddress => {
                let resource_address: ObjectKey = value.deserialized().map_err(BorError::from)?;
                Ok(Self::ResourceAddress(resource_address.into()))
            },
            BinaryTag::TransactionReceipt => {
                let tx_receipt_hash: Hash = value.deserialized().map_err(BorError::from)?;
                Ok(Self::TransactionReceiptAddress(tx_receipt_hash.into()))
            },
            BinaryTag::NonFungibleAddress => {
                let non_fungible_address: NonFungibleAddressContents = value.deserialized().map_err(BorError::from)?;
                Ok(Self::NonFungibleAddress(non_fungible_address.into()))
            },
            BinaryTag::Metadata => {
                let metadata: BTreeMap<String, String> = value.deserialized().map_err(BorError::from)?;
                Ok(Self::Metadata(metadata.into()))
            },
            BinaryTag::VaultId => {
                let vault_id: ObjectKey = value.deserialized().map_err(BorError::from)?;
                Ok(Self::VaultId(vault_id.into()))
            },
            BinaryTag::ProofId => {
                let value: u32 = value.deserialized().map_err(BorError::from)?;
                Ok(Self::ProofId(value.into()))
            },
            BinaryTag::UnclaimedConfidentialOutputAddress => {
                let value: ObjectKey = value.deserialized().map_err(BorError::from)?;
                Ok(Self::UnclaimedConfidentialOutputAddress(value.into()))
            },
            BinaryTag::TemplateAddress => {
                let value: Hash = value.deserialized().map_err(BorError::from)?;
                Ok(Self::PublishedTemplateAddress(value.into()))
            },
            BinaryTag::ValidatorNodeFeePool => {
                let value: [u8; 32] = value.deserialized().map_err(BorError::from)?;
                Ok(Self::ValidatorNodeFeePool(value.into()))
            },
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct IndexedValueVisitor {
    buckets: Vec<BucketId>,
    proofs: Vec<ProofId>,
    component_addresses: Vec<ComponentAddress>,
    resource_addresses: Vec<ResourceAddress>,
    transaction_receipt_addresses: Vec<TransactionReceiptAddress>,
    non_fungible_addresses: Vec<NonFungibleAddress>,
    vault_ids: Vec<VaultId>,
    metadata: Vec<Metadata>,
    unclaimed_confidential_output_addresses: Vec<UnclaimedConfidentialOutputAddress>,
    published_templates: Vec<PublishedTemplateAddress>,
    validator_node_fee_pools: Vec<ValidatorFeePoolAddress>,
}

impl IndexedValueVisitor {
    pub fn new() -> Self {
        Self {
            buckets: vec![],
            proofs: vec![],
            component_addresses: vec![],
            resource_addresses: vec![],
            transaction_receipt_addresses: vec![],
            non_fungible_addresses: vec![],
            vault_ids: vec![],
            metadata: vec![],
            unclaimed_confidential_output_addresses: vec![],
            published_templates: vec![],
            validator_node_fee_pools: vec![],
        }
    }
}

impl ValueVisitor<WellKnownTariValue> for IndexedValueVisitor {
    type Error = IndexedValueError;

    fn visit(&mut self, value: WellKnownTariValue) -> Result<ControlFlow<()>, Self::Error> {
        match value {
            WellKnownTariValue::ComponentAddress(address) => {
                self.component_addresses.push(address);
            },
            WellKnownTariValue::ResourceAddress(address) => {
                self.resource_addresses.push(address);
            },
            WellKnownTariValue::TransactionReceiptAddress(address) => {
                self.transaction_receipt_addresses.push(address);
            },
            WellKnownTariValue::BucketId(bucket_id) => {
                self.buckets.push(bucket_id);
            },
            WellKnownTariValue::NonFungibleAddress(address) => {
                self.non_fungible_addresses.push(address);
            },
            WellKnownTariValue::VaultId(vault_id) => {
                self.vault_ids.push(vault_id);
            },
            WellKnownTariValue::Metadata(metadata) => {
                self.metadata.push(metadata);
            },
            WellKnownTariValue::ProofId(proof_id) => {
                self.proofs.push(proof_id);
            },
            WellKnownTariValue::UnclaimedConfidentialOutputAddress(address) => {
                self.unclaimed_confidential_output_addresses.push(address);
            },
            WellKnownTariValue::PublishedTemplateAddress(template) => {
                self.published_templates.push(template);
            },
            WellKnownTariValue::ValidatorNodeFeePool(address) => {
                self.validator_node_fee_pools.push(address);
            },
        }
        Ok(ControlFlow::Continue(()))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum IndexedValueError {
    #[error("Bor error: {0}")]
    BorError(#[from] tari_bor::BorError),
    #[error("Invalid tag: {0}")]
    InvalidTag(u64),
    #[error("{0}")]
    Custom(String),
}

impl From<&str> for IndexedValueError {
    fn from(s: &str) -> Self {
        Self::Custom(s.to_string())
    }
}

pub fn decode_value_at_path<T>(value: &tari_bor::Value, path: &str) -> Result<Option<T>, IndexedValueError>
where for<'a> T: serde::Deserialize<'a> {
    get_value_by_path(value, path)
        .map(tari_bor::from_value)
        .transpose()
        .map_err(Into::into)
}

fn get_value_by_path<'a>(value: &'a tari_bor::Value, path: &str) -> Option<&'a tari_bor::Value> {
    let mut value = value;
    for part in path.split('.') {
        if part == "$" {
            continue;
        }
        match value {
            tari_bor::Value::Map(map) => {
                value = &map
                    .iter()
                    .find(|(k, _)| k.as_text().map(|s| s == part).unwrap_or(false))?
                    .1;
            },
            tari_bor::Value::Array(list) => {
                let index: usize = part.parse().expect("invalid index");
                value = list.get(index)?;
            },
            _ => return None,
        }
    }
    Some(value)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use rand::{rngs::OsRng, RngCore};
    use tari_bor::cbor;
    use tari_template_lib::models::NonFungibleId;

    use super::*;
    use crate::hashing::{hasher32, EngineHashDomainLabel};

    fn new_object_key() -> ObjectKey {
        hasher32(EngineHashDomainLabel::ComponentAddress)
            .chain(&OsRng.next_u32())
            .result()
            .trailing_bytes()
            .into()
    }

    #[derive(Serialize, Deserialize)]
    struct SubStruct {
        buckets: Vec<BucketId>,
    }

    #[derive(Serialize, Deserialize)]
    struct TestStruct {
        name: String,
        component: ComponentAddress,
        components: Vec<ComponentAddress>,
        resource_map: HashMap<ResourceAddress, ComponentAddress>,
        sub_struct: SubStruct,
        sub_structs: Vec<SubStruct>,
        vault_ids: Vec<VaultId>,
        non_fungible_id: Option<NonFungibleAddress>,
        metadata: Metadata,
    }

    #[test]
    fn it_returns_empty_indexed_value_for_empty_bytes() {
        let value = IndexedValue::from_raw(&[]).unwrap();
        assert_eq!(value, IndexedValue::default());
    }

    #[test]
    fn it_extracts_known_types_from_binary_data() {
        let addrs: [ComponentAddress; 3] = [
            new_object_key().into(),
            new_object_key().into(),
            new_object_key().into(),
        ];
        let resx_addr = ResourceAddress::new(new_object_key());

        let data = TestStruct {
            name: "John".to_string(),
            component: addrs[0],
            components: vec![addrs[1]],
            resource_map: {
                let mut m = HashMap::new();
                m.insert(resx_addr, addrs[2]);
                m
            },
            sub_struct: SubStruct {
                buckets: vec![1.into(), 2.into()],
            },
            sub_structs: vec![
                SubStruct {
                    buckets: vec![1.into(), 2.into()],
                },
                SubStruct {
                    buckets: vec![1.into(), 2.into()],
                },
            ],
            vault_ids: vec![VaultId::new(new_object_key())],
            non_fungible_id: Some(NonFungibleAddress::new(resx_addr, NonFungibleId::Uint64(1))),
            metadata: Metadata::new(),
        };

        let value = tari_bor::to_value(&data).unwrap();
        let indexed = IndexedValue::from_value(value).unwrap();

        assert!(indexed.component_addresses().contains(&addrs[0]));
        assert!(indexed.component_addresses().contains(&addrs[1]));
        assert!(indexed.component_addresses().contains(&addrs[2]));
        assert_eq!(indexed.component_addresses().len(), 3);
        assert_eq!(indexed.resource_addresses().len(), 1);

        assert_eq!(indexed.non_fungible_addresses().len(), 1);
        assert_eq!(indexed.vault_ids().len(), 1);
        assert_eq!(indexed.metadata().len(), 1);

        assert!(indexed.bucket_ids().contains(&1.into()));
        assert!(indexed.bucket_ids().contains(&2.into()));
        assert_eq!(indexed.bucket_ids().len(), 6);

        let buckets: Vec<BucketId> = indexed.get_value("$.sub_structs.1.buckets").unwrap().unwrap();
        assert_eq!(buckets, vec![1.into(), 2.into()]);
    }

    #[test]
    fn it_diffs_two_indexed_values() {
        let v1 = IndexedWellKnownTypes::from_value(
            &cbor!({
                "bucket" => BucketId::from(1),
                "proof1" => ProofId::from(1),
                "proof2" => ProofId::from(2),
            })
            .unwrap(),
        )
        .unwrap();
        let v2 = IndexedWellKnownTypes::from_value(
            &cbor!({
                "buckets" => [BucketId::from(1), BucketId::from(2)],
                "proofs" => [ProofId::from(2), ProofId::from(3), ProofId::from(4)],
            })
            .unwrap(),
        )
        .unwrap();

        let diff = v2.diff(&v1);

        assert_eq!(diff.bucket_ids, [BucketId::from(2)]);
        assert_eq!(diff.proof_ids, [ProofId::from(3), ProofId::from(4)]);
    }
}
