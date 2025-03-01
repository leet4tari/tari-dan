//  Copyright 2022. The Tari Project
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

import { FormEvent, useState } from "react";
import { Form } from "react-router-dom";
import TextField from "@mui/material/TextField/TextField";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import Grid from "@mui/material/Grid";
import Typography from "@mui/material/Typography";
import { useTheme } from "@mui/material/styles";
import Loading from "../../Components/Loading";
import { useAccountsCreateFreeTestCoins } from "../../api/hooks/useAccounts";
import useAccountStore from "../../store/accountStore";
import { substateIdToString } from "@tari-project/typescript-bindings";

function Onboarding() {
  const { mutate, status } = useAccountsCreateFreeTestCoins();
  const { setAccount, setPublicKey } = useAccountStore();
  const theme = useTheme();

  const [accountFormState, setAccountFormState] = useState({
    accountName: "",
  });

  const handleCreateAccount = (e: FormEvent) => {
    e.preventDefault();
    mutate(
      {
        account: { Name: accountFormState.accountName },
        amount: 1_000_000_000,
        fee: 1000,
      },
      {
        onSuccess: (resp) => {
          setAccount(resp.account);
          setPublicKey(resp.public_key);
        },
      },
    );
  };

  const onAccountChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setAccountFormState({
      ...accountFormState,
      [e.target.name]: e.target.value,
    });
  };

  if (status === "loading") {
    return <Loading />;
  }

  return (
    <>
      <Grid item xs={12} md={12} lg={12}>
        <Box
          style={{
            display: "flex",
            justifyContent: "center",
            alignItems: "center",
            flexDirection: "column",
            width: "100%",
            height: "calc(100vh - 200px)",
            minHeight: 400,
            gap: theme.spacing(3),
          }}
        >
          <Box
            style={{
              display: "flex",
              justifyContent: "center",
              alignItems: "center",
              flexDirection: "column",
              gap: 0,
              maxWidth: 600,
            }}
          >
            <Typography
              variant="h3"
              style={{
                textAlign: "center",
              }}
            >
              Welcome to the Tari Asset Vault
            </Typography>
            <Typography
              variant="h5"
              style={{
                textAlign: "center",
              }}
            >
              Create your test account below to get started
            </Typography>
            <Form
              onSubmit={handleCreateAccount}
              className="flex-container"
              style={{
                flexDirection: "column",
                marginTop: theme.spacing(3),
              }}
            >
              <TextField
                name="accountName"
                label="Account Name"
                value={accountFormState.accountName}
                onChange={onAccountChange}
                style={{ flexGrow: 1 }}
              />
              <Button variant="contained" type="submit">
                Create Account
              </Button>
            </Form>
          </Box>
        </Box>
      </Grid>
    </>
  );
}

export default Onboarding;
