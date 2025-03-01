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

import { useMutation, useQuery } from "@tanstack/react-query";
import { transactionsGet, transactionsGetAll, transactionsPublishTemplate } from "../../utils/json_rpc";
import { ApiError } from "../helpers/types";
import queryClient from "../queryClient";

import type { TransactionStatus } from "@tari-project/typescript-bindings";

export const useTransactionDetails = (hash: string) => {
  return useQuery({
    queryKey: ["transaction_details"],
    queryFn: () => {
      return transactionsGet({ transaction_id: hash });
    },
    onError: (error: ApiError) => {
      error;
    },
  });
};

export const useGetAllTransactions = (status: TransactionStatus | null, component: string | null) => {
  return useQuery({
    queryKey: ["transactions"],
    queryFn: () => transactionsGetAll({ status: status, component: component }),
    onError: (error: ApiError) => {
      error;
    },
    refetchInterval: 5000,
    keepPreviousData: true,
  });
};

export const usePublishTemplate = () => {
  return useMutation(transactionsPublishTemplate, {
    onError: (error: ApiError) => {
      console.error(error);
    },
    onSettled: () => {
      queryClient.invalidateQueries(["transactions"]);
      queryClient.invalidateQueries(["accounts_balances"]);
    },
  });
};
