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
import Tab from "@mui/material/Tab";
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell from "@mui/material/TableCell";
import TableContainer from "@mui/material/TableContainer";
import TableHead from "@mui/material/TableHead";
import TableRow from "@mui/material/TableRow";
import Tabs from "@mui/material/Tabs";
import Typography from "@mui/material/Typography";
import { useState } from "react";
import CopyToClipboard from "../../../Components/CopyToClipboard";
import FetchStatusCheck from "../../../Components/FetchStatusCheck";
import { DataTableCell } from "../../../Components/StyledComponents";
import { useAccountNFTsList, useAccountsGetBalances } from "../../../api/hooks/useAccounts";
import useAccountStore from "../../../store/accountStore";
import { shortenString, shortenSubstateId, substateIdToString } from "../../../utils/helpers";
import NFTList from "../../../Components/NFTList";
import { Button } from "@mui/material";
import { SendMoneyDialog } from "./SendMoney";
import { ResourceAddress, ResourceType, VaultId, BalanceEntry, Account } from "@tari-project/typescript-bindings";

interface TabPanelProps {
  children?: React.ReactNode;
  index: number;
  value: number;
}

interface BalanceRowProps {
  token_symbol: string;
  resource_address: ResourceAddress;
  resource_type: ResourceType;
  vault_address: VaultId;
  balance: number;
  confidential_balance: number;
  onSendClicked?: (resource_address: ResourceAddress, resource_type: ResourceType) => void;
}

function BalanceRow(props: BalanceRowProps) {
  const { token_symbol, resource_address, resource_type, balance, confidential_balance, vault_address, onSendClicked } =
    props;
  const { showBalance } = useAccountStore();
  return (
    <TableRow key={token_symbol || resource_address}>
      <DataTableCell>
        <span title={vault_address}>{shortenSubstateId(vault_address)}</span>
        <CopyToClipboard copy={substateIdToString(vault_address)} title="Copy vault address" />
      </DataTableCell>
      <DataTableCell>
        <span title={resource_address}>
          {token_symbol || shortenSubstateId(resource_address)} {resource_type}
        </span>
        <CopyToClipboard copy={resource_address} title="Copy resource address" />
      </DataTableCell>
      <DataTableCell>{showBalance ? balance : "*************"}</DataTableCell>
      <DataTableCell>
        <ConfidentialBalance show={showBalance} resourceType={resource_type} balance={confidential_balance} />
      </DataTableCell>
      <DataTableCell>
        <Button variant="outlined" onClick={() => onSendClicked?.(resource_address, resource_type)}>
          Send
        </Button>
      </DataTableCell>
    </TableRow>
  );
}

function ConfidentialBalance(props: { show: boolean; balance: number; resourceType: string }) {
  switch (props.resourceType) {
    case "Confidential":
      return <>{props.show ? props.balance : "**************"}</>;
    default:
      return <>--</>;
  }
}

function TabPanel(props: TabPanelProps) {
  const { children, value, index, ...other } = props;

  return (
    <div
      role="tabpanel"
      hidden={value !== index}
      id={`simple-tabpanel-${index}`}
      aria-labelledby={`simple-tab-${index}`}
      {...other}
    >
      {value === index && (
        <Box sx={{ p: 3 }}>
          <Typography component="div">{children}</Typography>
        </Box>
      )}
    </div>
  );
}

function tabProps(index: number) {
  return {
    "id": `asset-tab-${index}`,
    "aria-controls": `asset-tabpanel-${index}`,
  };
}

function Assets({ account }: { account: Account }) {
  const [resourceToSend, setResourceToSend] = useState<{
    address: ResourceAddress;
    resource_type: ResourceType;
  } | null>(null);
  const [value, setValue] = useState(0);

  const {
    data: balancesData,
    isError: balancesIsError,
    error: balancesError,
    isFetching: balancesIsFetching,
  } = useAccountsGetBalances({ ComponentAddress: substateIdToString(account.address) });

  const {
    data: nftsListData,
    isError: nftsListIsError,
    error: nftsListError,
    isFetching: nftsListIsFetching,
  } = useAccountNFTsList({ ComponentAddress: substateIdToString(account.address) }, 0, 10);

  const handleChange = (_event: React.SyntheticEvent, newValue: number) => {
    setValue(newValue);
  };

  const handleSendResourceClicked = (address: ResourceAddress, resource_type: ResourceType) => {
    setResourceToSend({ address, resource_type });
  };

  return (
    <Box sx={{ width: "100%" }}>
      <SendMoneyDialog
        open={resourceToSend !== null}
        handleClose={() => setResourceToSend(null)}
        onSendComplete={() => setResourceToSend(null)}
        resource_address={resourceToSend?.address}
        resource_type={resourceToSend?.resource_type}
      />
      <Box sx={{ borderBottom: 1, borderColor: "divider" }}>
        <Tabs value={value} onChange={handleChange} aria-label="account assets" variant="standard">
          <Tab label="Tokens" {...tabProps(0)} style={{ width: 150 }} />
          <Tab label="NFTs" {...tabProps(1)} style={{ width: 150 }} />
        </Tabs>
      </Box>
      <TabPanel value={value} index={0}>
        <FetchStatusCheck
          isError={balancesIsError}
          errorMessage={balancesError?.message || "Error fetching data"}
          isLoading={balancesIsFetching}
        >
          <TableContainer>
            <Table>
              <TableHead>
                <TableRow>
                  <TableCell>Vault</TableCell>
                  <TableCell>Resource</TableCell>
                  <TableCell>Revealed Balance</TableCell>
                  <TableCell>Confidential Balance</TableCell>
                  <TableCell></TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {balancesData?.balances.map(
                  (
                    {
                      resource_address,
                      balance,
                      resource_type,
                      confidential_balance,
                      token_symbol,
                      vault_address,
                    }: BalanceEntry,
                    i: number,
                  ) => (
                    <BalanceRow
                      key={i}
                      token_symbol={token_symbol || ""}
                      resource_address={resource_address}
                      resource_type={resource_type}
                      balance={balance}
                      confidential_balance={confidential_balance}
                      vault_address={vault_address as unknown as string}
                      onSendClicked={handleSendResourceClicked}
                    />
                  ),
                )}
              </TableBody>
            </Table>
          </TableContainer>
        </FetchStatusCheck>
      </TabPanel>
      <TabPanel value={value} index={1}>
        <NFTList
          nftsListIsError={nftsListIsError}
          nftsListIsFetching={nftsListIsFetching}
          nftsListError={nftsListError}
          nftsListData={nftsListData}
        />
      </TabPanel>
    </Box>
  );
}

export default Assets;
