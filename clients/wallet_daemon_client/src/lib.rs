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
mod component_address;
pub use component_address::*;
pub mod error;
pub mod serialize;
pub mod types;

use std::borrow::Borrow;

use json::Value;
use reqwest::{
    header::{self, HeaderMap, AUTHORIZATION},
    IntoUrl,
    Url,
};
use serde::{de::DeserializeOwned, Serialize};
use serde_json as json;
use serde_json::json;
use types::{
    AccountsCreateFreeTestCoinsRequest,
    AccountsCreateFreeTestCoinsResponse,
    AccountsTransferRequest,
    AccountsTransferResponse,
    AuthLoginAcceptRequest,
    AuthLoginAcceptResponse,
    AuthLoginDenyRequest,
    AuthLoginDenyResponse,
    AuthLoginRequest,
    AuthLoginResponse,
    ClaimBurnRequest,
    ClaimBurnResponse,
    GetAccountNftRequest,
    GetAccountNftResponse,
    ListAccountNftRequest,
    ListAccountNftResponse,
    MintAccountNftRequest,
    MintAccountNftResponse,
    ProofsCancelRequest,
    ProofsCancelResponse,
    ProofsFinalizeRequest,
    ProofsFinalizeResponse,
    ProofsGenerateRequest,
    ProofsGenerateResponse,
    WebRtcStartRequest,
    WebRtcStartResponse,
};

use crate::{
    error::WalletDaemonClientError,
    types::{
        AccountGetDefaultRequest,
        AccountGetRequest,
        AccountGetResponse,
        AccountSetDefaultRequest,
        AccountSetDefaultResponse,
        AccountsCreateRequest,
        AccountsCreateResponse,
        AccountsGetBalancesRequest,
        AccountsGetBalancesResponse,
        AccountsInvokeRequest,
        AccountsInvokeResponse,
        AccountsListRequest,
        AccountsListResponse,
        AuthGetAllJwtRequest,
        AuthGetAllJwtResponse,
        AuthRevokeTokenRequest,
        AuthRevokeTokenResponse,
        ClaimValidatorFeesRequest,
        ClaimValidatorFeesResponse,
        ConfidentialCreateOutputProofRequest,
        ConfidentialCreateOutputProofResponse,
        ConfidentialTransferRequest,
        ConfidentialTransferResponse,
        ConfidentialViewVaultBalanceRequest,
        ConfidentialViewVaultBalanceResponse,
        GetValidatorFeesRequest,
        GetValidatorFeesResponse,
        KeyBranch,
        KeysCreateRequest,
        KeysCreateResponse,
        KeysListRequest,
        KeysListResponse,
        KeysSetActiveRequest,
        KeysSetActiveResponse,
        PublishTemplateRequest,
        PublishTemplateResponse,
        RevealFundsRequest,
        RevealFundsResponse,
        SettingsGetResponse,
        TransactionGetAllRequest,
        TransactionGetAllResponse,
        TransactionGetRequest,
        TransactionGetResponse,
        TransactionGetResultRequest,
        TransactionGetResultResponse,
        TransactionSubmitDryRunRequest,
        TransactionSubmitDryRunResponse,
        TransactionSubmitRequest,
        TransactionSubmitResponse,
        TransactionWaitResultRequest,
        TransactionWaitResultResponse,
    },
};

#[derive(Debug, Clone)]
pub struct WalletDaemonClient {
    client: reqwest::Client,
    endpoint: Url,
    request_id: i64,
    token: Option<String>,
}

impl WalletDaemonClient {
    pub fn connect<T: IntoUrl>(endpoint: T, token: Option<String>) -> Result<Self, WalletDaemonClientError> {
        let client = reqwest::Client::builder()
            .default_headers({
                let mut headers = HeaderMap::with_capacity(1);
                headers.insert(header::CONTENT_TYPE, "application/json".parse().unwrap());
                headers
            })
            .build()?;

        Ok(Self {
            client,
            endpoint: endpoint.into_url()?,
            request_id: 0,
            token,
        })
    }

    pub fn set_auth_token(&mut self, token: String) -> &mut Self {
        self.token = Some(token);
        self
    }

    // pub async fn get_identity(&mut self) -> Result<GetIdentityResponse, WalletDaemonClientError> {
    //     self.send_request("identities.get", json!({})).await
    // }

    pub async fn create_key(&mut self, branch: KeyBranch) -> Result<KeysCreateResponse, WalletDaemonClientError> {
        self.send_request("keys.create", &KeysCreateRequest {
            branch,
            specific_index: None,
        })
        .await
    }

    pub async fn create_specific_key(
        &mut self,
        branch: KeyBranch,
        index: u64,
    ) -> Result<KeysCreateResponse, WalletDaemonClientError> {
        self.send_request("keys.create", &KeysCreateRequest {
            branch,
            specific_index: Some(index),
        })
        .await
    }

    pub async fn set_active_key(&mut self, index: u64) -> Result<KeysSetActiveResponse, WalletDaemonClientError> {
        self.send_request("keys.set_active", &KeysSetActiveRequest { index })
            .await
    }

    pub async fn list_keys(&mut self, branch: KeyBranch) -> Result<KeysListResponse, WalletDaemonClientError> {
        self.send_request("keys.list", &KeysListRequest { branch }).await
    }

    pub async fn get_transaction<T: Borrow<TransactionGetRequest>>(
        &mut self,
        request: T,
    ) -> Result<TransactionGetResponse, WalletDaemonClientError> {
        self.send_request("transactions.get", request.borrow()).await
    }

    pub async fn get_transactions_all<T: Borrow<TransactionGetAllRequest>>(
        &mut self,
        request: T,
    ) -> Result<TransactionGetAllResponse, WalletDaemonClientError> {
        self.send_request("transactions.get_all", request.borrow()).await
    }

    pub async fn get_transaction_result<T: Borrow<TransactionGetResultRequest>>(
        &mut self,
        request: T,
    ) -> Result<TransactionGetResultResponse, WalletDaemonClientError> {
        self.send_request("transactions.get_result", request.borrow()).await
    }

    pub async fn wait_transaction_result<T: Borrow<TransactionWaitResultRequest>>(
        &mut self,
        request: T,
    ) -> Result<TransactionWaitResultResponse, WalletDaemonClientError> {
        self.send_request("transactions.wait_result", request.borrow()).await
    }

    pub async fn submit_transaction<T: Borrow<TransactionSubmitRequest>>(
        &mut self,
        request: T,
    ) -> Result<TransactionSubmitResponse, WalletDaemonClientError> {
        self.send_request("transactions.submit", request.borrow()).await
    }

    pub async fn submit_transaction_dry_run<T: Borrow<TransactionSubmitDryRunRequest>>(
        &mut self,
        request: T,
    ) -> Result<TransactionSubmitDryRunResponse, WalletDaemonClientError> {
        self.send_request("transactions.submit_dry_run", request.borrow()).await
    }

    pub async fn create_account<T: Borrow<AccountsCreateRequest>>(
        &mut self,
        request: T,
    ) -> Result<AccountsCreateResponse, WalletDaemonClientError> {
        self.send_request("accounts.create", request.borrow()).await
    }

    pub async fn invoke_account_method<T: Borrow<AccountsInvokeRequest>>(
        &mut self,
        req: T,
    ) -> Result<AccountsInvokeResponse, WalletDaemonClientError> {
        self.send_request("accounts.invoke", req.borrow()).await
    }

    pub async fn get_account_balances<T: Borrow<AccountsGetBalancesRequest>>(
        &mut self,
        request: T,
    ) -> Result<AccountsGetBalancesResponse, WalletDaemonClientError> {
        self.send_request("accounts.get_balances", request.borrow()).await
    }

    pub async fn get_validator_fees<T: Borrow<GetValidatorFeesRequest>>(
        &mut self,
        request: T,
    ) -> Result<GetValidatorFeesResponse, WalletDaemonClientError> {
        self.send_request("validators.get_fees", request.borrow()).await
    }

    pub async fn claim_validator_fees<T: Borrow<ClaimValidatorFeesRequest>>(
        &mut self,
        request: T,
    ) -> Result<ClaimValidatorFeesResponse, WalletDaemonClientError> {
        self.send_request("validators.claim_fees", request.borrow()).await
    }

    pub async fn list_accounts(
        &mut self,
        offset: u64,
        limit: u64,
    ) -> Result<AccountsListResponse, WalletDaemonClientError> {
        self.send_request("accounts.list", &AccountsListRequest { offset, limit })
            .await
    }

    pub async fn accounts_get(
        &mut self,
        name_or_address: ComponentAddressOrName,
    ) -> Result<AccountGetResponse, WalletDaemonClientError> {
        self.send_request("accounts.get", &AccountGetRequest { name_or_address })
            .await
    }

    pub async fn accounts_get_default(&mut self) -> Result<AccountGetResponse, WalletDaemonClientError> {
        self.send_request("accounts.get_default", &AccountGetDefaultRequest {})
            .await
    }

    pub async fn accounts_set_default(
        &mut self,
        account: ComponentAddressOrName,
    ) -> Result<AccountSetDefaultResponse, WalletDaemonClientError> {
        self.send_request("accounts.set_default", &AccountSetDefaultRequest { account })
            .await
    }

    pub async fn accounts_transfer<T: Borrow<AccountsTransferRequest>>(
        &mut self,
        req: T,
    ) -> Result<AccountsTransferResponse, WalletDaemonClientError> {
        self.send_request("accounts.transfer", req.borrow()).await
    }

    pub async fn accounts_confidential_transfer<T: Borrow<ConfidentialTransferRequest>>(
        &mut self,
        req: T,
    ) -> Result<ConfidentialTransferResponse, WalletDaemonClientError> {
        self.send_request("accounts.confidential_transfer", req.borrow()).await
    }

    pub async fn claim_burn<T: Borrow<ClaimBurnRequest>>(
        &mut self,
        req: T,
    ) -> Result<ClaimBurnResponse, WalletDaemonClientError> {
        self.send_request("accounts.claim_burn", req.borrow()).await
    }

    pub async fn accounts_reveal_funds<T: Borrow<RevealFundsRequest>>(
        &mut self,
        req: T,
    ) -> Result<RevealFundsResponse, WalletDaemonClientError> {
        self.send_request("accounts.reveal_funds", req.borrow()).await
    }

    pub async fn create_transfer_proof<T: Borrow<ProofsGenerateRequest>>(
        &mut self,
        req: T,
    ) -> Result<ProofsGenerateResponse, WalletDaemonClientError> {
        self.send_request("confidential.create_transfer_proof", req.borrow())
            .await
    }

    pub async fn cancel_transfer_proof<T: Borrow<ProofsCancelRequest>>(
        &mut self,
        req: T,
    ) -> Result<ProofsCancelResponse, WalletDaemonClientError> {
        self.send_request("confidential.cancel", req.borrow()).await
    }

    pub async fn finalize_transfer_proof<T: Borrow<ProofsFinalizeRequest>>(
        &mut self,
        req: T,
    ) -> Result<ProofsFinalizeResponse, WalletDaemonClientError> {
        self.send_request("confidential.finalize", req.borrow()).await
    }

    pub async fn create_confidential_output_proof<T: Borrow<ConfidentialCreateOutputProofRequest>>(
        &mut self,
        req: T,
    ) -> Result<ConfidentialCreateOutputProofResponse, WalletDaemonClientError> {
        self.send_request("confidential.create_output_proof", req.borrow())
            .await
    }

    pub async fn create_free_test_coins<T: Borrow<AccountsCreateFreeTestCoinsRequest>>(
        &mut self,
        req: T,
    ) -> Result<AccountsCreateFreeTestCoinsResponse, WalletDaemonClientError> {
        self.send_request("accounts.create_free_test_coins", req.borrow()).await
    }

    pub async fn mint_account_nft<T: Borrow<MintAccountNftRequest>>(
        &mut self,
        req: T,
    ) -> Result<MintAccountNftResponse, WalletDaemonClientError> {
        self.send_request("nfts.mint_account_nft", req.borrow()).await
    }

    pub async fn get_account_nft<T: Borrow<GetAccountNftRequest>>(
        &mut self,
        req: T,
    ) -> Result<GetAccountNftResponse, WalletDaemonClientError> {
        self.send_request("nfts.get", req.borrow()).await
    }

    pub async fn list_account_nfts<T: Borrow<ListAccountNftRequest>>(
        &mut self,
        req: T,
    ) -> Result<ListAccountNftResponse, WalletDaemonClientError> {
        self.send_request("nfts.list", req.borrow()).await
    }

    pub async fn view_vault_balance<T: Borrow<ConfidentialViewVaultBalanceRequest>>(
        &mut self,
        req: T,
    ) -> Result<ConfidentialViewVaultBalanceResponse, WalletDaemonClientError> {
        self.send_request("confidential.view_vault_balance", req.borrow()).await
    }

    pub async fn auth_request<T: Borrow<AuthLoginRequest>>(
        &mut self,
        req: T,
    ) -> Result<AuthLoginResponse, WalletDaemonClientError> {
        self.send_request("auth.request", req.borrow()).await
    }

    pub async fn auth_accept<T: Borrow<AuthLoginAcceptRequest>>(
        &mut self,
        req: T,
    ) -> Result<AuthLoginAcceptResponse, WalletDaemonClientError> {
        self.send_request("auth.accept", req.borrow()).await
    }

    pub async fn auth_deny<T: Borrow<AuthLoginDenyRequest>>(
        &mut self,
        req: T,
    ) -> Result<AuthLoginDenyResponse, WalletDaemonClientError> {
        self.send_request("auth.deny", req.borrow()).await
    }

    pub async fn auth_revoke<T: Borrow<AuthRevokeTokenRequest>>(
        &mut self,
        req: T,
    ) -> Result<AuthRevokeTokenResponse, WalletDaemonClientError> {
        self.send_request("auth.revoke", req.borrow()).await
    }

    pub async fn auth_get_all_jwt<T: Borrow<AuthGetAllJwtRequest>>(
        &mut self,
        req: T,
    ) -> Result<AuthGetAllJwtResponse, WalletDaemonClientError> {
        self.send_request("auth.get_all_jwt", req.borrow()).await
    }

    pub async fn webrtc_start<T: Borrow<WebRtcStartRequest>>(
        &mut self,
        req: T,
    ) -> Result<WebRtcStartResponse, WalletDaemonClientError> {
        self.send_request("webrtc.start", req.borrow()).await
    }

    pub async fn publish_template<T: Borrow<PublishTemplateRequest>>(
        &mut self,
        request: T,
    ) -> Result<PublishTemplateResponse, WalletDaemonClientError> {
        self.send_request("transactions.publish_template", request.borrow())
            .await
    }

    pub async fn get_settings(&mut self) -> Result<SettingsGetResponse, WalletDaemonClientError> {
        self.send_request("settings.get", &json!({})).await
    }

    fn next_request_id(&mut self) -> i64 {
        self.request_id += 1;
        self.request_id
    }

    async fn jrpc_call<T: Serialize>(&mut self, method: &str, params: &T) -> Result<Value, WalletDaemonClientError> {
        let request_json = json!(
            {
                "jsonrpc": "2.0",
                "id": self.next_request_id(),
                "method": method,
                "params": params,
            }
        );
        let mut builder = self.client.post(self.endpoint.clone());
        if let Some(token) = &self.token {
            // If we don't have the token and the method is anything else than "auth.login" it will fail.
            builder = builder.header(AUTHORIZATION, format!("Bearer {}", token));
        }
        let resp = builder.body(request_json.to_string()).send().await?;
        let val = resp.json().await?;
        jsonrpc_result(val)
    }

    async fn send_request<T: Serialize, R: DeserializeOwned>(
        &mut self,
        method: &str,
        params: &T,
    ) -> Result<R, WalletDaemonClientError> {
        let params = json::to_value(params).map_err(|e| WalletDaemonClientError::SerializeRequest {
            source: e,
            method: method.to_string(),
        })?;
        let resp = self.jrpc_call(method, &params).await?;
        match serde_json::from_value(resp) {
            Ok(r) => Ok(r),
            Err(e) => Err(WalletDaemonClientError::DeserializeResponse {
                source: e,
                method: method.to_string(),
            }),
        }
    }
}

fn jsonrpc_result(val: json::Value) -> Result<json::Value, WalletDaemonClientError> {
    if let Some(err) = val.get("error") {
        let code = err.get("code").and_then(|c| c.as_i64()).unwrap_or(-1);
        let message = err.get("message").and_then(|m| m.as_str()).unwrap_or("Unknown error");
        return Err(WalletDaemonClientError::RequestFailedWithStatus {
            code,
            message: message.to_string(),
        });
    }

    let result = val
        .get("result")
        .ok_or_else(|| WalletDaemonClientError::InvalidResponse {
            message: "Missing result field".to_string(),
        })?;
    Ok(result.clone())
}
