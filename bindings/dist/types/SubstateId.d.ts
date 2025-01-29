import type { ComponentAddress } from "./ComponentAddress";
import type { NonFungibleAddress } from "./NonFungibleAddress";
import type { NonFungibleIndexAddress } from "./NonFungibleIndexAddress";
import type { PublishedTemplateAddress } from "./PublishedTemplateAddress";
import type { ResourceAddress } from "./ResourceAddress";
import type { TransactionReceiptAddress } from "./TransactionReceiptAddress";
import type { UnclaimedConfidentialOutputAddress } from "./UnclaimedConfidentialOutputAddress";
import type { ValidatorFeePoolAddress } from "./ValidatorFeePoolAddress";
import type { VaultId } from "./VaultId";
export type SubstateId = {
    Component: ComponentAddress;
} | {
    Resource: ResourceAddress;
} | {
    Vault: VaultId;
} | {
    UnclaimedConfidentialOutput: UnclaimedConfidentialOutputAddress;
} | {
    NonFungible: NonFungibleAddress;
} | {
    NonFungibleIndex: NonFungibleIndexAddress;
} | {
    TransactionReceipt: TransactionReceiptAddress;
} | {
    Template: PublishedTemplateAddress;
} | {
    ValidatorFeePool: ValidatorFeePoolAddress;
};
