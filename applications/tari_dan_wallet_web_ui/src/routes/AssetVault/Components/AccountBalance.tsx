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
import Fade from "@mui/material/Fade";
import IconButton from "@mui/material/IconButton";
import Typography from "@mui/material/Typography";
import { useTheme } from "@mui/material/styles";
import { IoEyeOffOutline, IoEyeOutline } from "react-icons/io5";
import FetchStatusCheck from "../../../Components/FetchStatusCheck";
import { useAccountsGetBalances } from "../../../api/hooks/useAccounts";
import TariGem from "../../../assets/TariGem";
import useAccountStore from "../../../store/accountStore";
import { substateIdToString } from "@tari-project/typescript-bindings";
import { useEffect } from "react";

export default function AccountBalance() {
  const theme = useTheme();
  const { showBalance, setShowBalance, account } = useAccountStore();
  if (!account) return <></>;

  const {
    data: balancesData,
    isError: balancesIsError,
    error: balancesError,
    isFetching: balancesIsFetching,
    isLoading: balancesIsLoading,
    isRefetching: balancesIsRefetching,
    refetch,
  } = useAccountsGetBalances({ ComponentAddress: substateIdToString(account.address) });

  useEffect(() => {
    refetch();
  }, [account]);

  const balanceObj = balancesData?.balances.find((b) => b.token_symbol === "XTR") || balancesData?.balances[0];
  const balance = balanceObj?.balance || 0 + (balanceObj?.confidential_balance || 0);

  let formattedBalance = "";
  if (balancesIsLoading || balancesIsRefetching) {
    formattedBalance = "...";
  } else {
    if (showBalance) {
      formattedBalance = balance.toLocaleString("en-US", {
        minimumFractionDigits: 0,
        maximumFractionDigits: 0,
      });
    } else {
      formattedBalance = "************";
    }
  }

  return (
    <FetchStatusCheck
      isError={balancesIsError}
      errorMessage={balancesError?.message || "Error fetching data"}
      isLoading={false}
    >
      <Fade in={!balancesIsFetching && !balancesIsError} timeout={100}>
        <Box>
          <Box>
            <Typography variant="body2" gutterBottom={false}>
              Balance
            </Typography>
          </Box>
          <Box
            style={{
              display: "flex",
              alignItems: "flex-start",
              justifyContent: "space-between",
              gap: theme.spacing(1),
              minWidth: "230px",
            }}
          >
            <Typography variant="h2">
              <>
                <TariGem fill={theme.palette.text.primary} /> {formattedBalance}
              </>
            </Typography>
            <IconButton onClick={() => setShowBalance(!showBalance)}>
              {showBalance ? (
                <IoEyeOffOutline color={theme.palette.primary.main} />
              ) : (
                <IoEyeOutline color={theme.palette.primary.main} />
              )}
            </IconButton>
          </Box>
        </Box>
      </Fade>
    </FetchStatusCheck>
  );
}
