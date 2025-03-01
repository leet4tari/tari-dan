//   Copyright 2023 The Tari Project
//   SPDX-License-Identifier: BSD-3-Clause

use std::str::FromStr;

use chrono::NaiveDateTime;
use tari_bor::json_encoding::CborValueJsonDeserializeWrapper;
use tari_dan_wallet_sdk::storage::WalletStorageError;
use tari_template_lib::models::{NonFungibleId, ResourceAddress, VaultId};

use crate::schema::non_fungible_tokens;

#[derive(Debug, Clone, Identifiable, Queryable)]
#[diesel(table_name = non_fungible_tokens)]
pub struct NonFungibleToken {
    pub id: i32,
    pub vault_id: i32,
    pub nft_id: String,
    pub resource_address: String,
    pub data: String,
    pub mutable_data: String,
    pub is_burned: bool,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
}

impl NonFungibleToken {
    pub fn try_into_non_fungible_token(
        self,
        vault_id: VaultId,
    ) -> Result<tari_dan_wallet_sdk::models::NonFungibleToken, WalletStorageError> {
        let data: CborValueJsonDeserializeWrapper =
            serde_json::from_str(&self.data).map_err(|e| WalletStorageError::DecodingError {
                operation: "try_from",
                item: "non_fungible_tokens.data",
                details: e.to_string(),
            })?;

        let mutable_data: CborValueJsonDeserializeWrapper =
            serde_json::from_str(&self.mutable_data).map_err(|e| WalletStorageError::DecodingError {
                operation: "try_from",
                item: "non_fungible_tokens.data",
                details: e.to_string(),
            })?;
        Ok(tari_dan_wallet_sdk::models::NonFungibleToken {
            data: data.into_inner(),
            mutable_data: mutable_data.into_inner(),
            resource_address: ResourceAddress::from_str(&self.resource_address).map_err(|e| {
                WalletStorageError::DecodingError {
                    operation: "try_from",
                    item: "non_fungible_tokens.resource_address",
                    details: e.to_string(),
                }
            })?,
            nft_id: NonFungibleId::try_from_canonical_string(&self.nft_id).map_err(|e| {
                WalletStorageError::DecodingError {
                    operation: "try_from",
                    item: "non_fungible_tokens.nft_id",
                    details: format!("{:?}", e),
                }
            })?,
            vault_id,
            is_burned: self.is_burned,
        })
    }
}
