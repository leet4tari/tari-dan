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

import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import { useTheme } from "@mui/material/styles";
import { useAccountsCreateFreeTestCoins } from "../../../api/hooks/useAccounts";
import ClaimBurn from "./ClaimBurn";
import useAccountStore from "../../../store/accountStore";
import SendMoney from "./SendMoney";
import ClaimFees from "./ClaimFees";
import PublishTemplate from "./PublishTemplate";
import { substateIdToString } from "@tari-project/typescript-bindings";

function ActionMenu() {
  const { mutate } = useAccountsCreateFreeTestCoins();
  const { account, setAccount, setPublicKey } = useAccountStore();
  const theme = useTheme();
  if (!account) {
    return null;
  }

  const onClaimFreeCoins = () => {
    mutate(
      {
        account: { ComponentAddress: substateIdToString(account.address) },
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

  return (
    <Box
      style={{
        display: "flex",
        gap: theme.spacing(1),
        marginBottom: theme.spacing(2),
      }}
    >
      <SendMoney />
      <ClaimFees />
      <Button variant="outlined" onClick={onClaimFreeCoins}>
        Claim Free Testnet Coins
      </Button>
      <ClaimBurn />
      <PublishTemplate />
    </Box>
  );
}

export default ActionMenu;
