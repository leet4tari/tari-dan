export type RejectReason = {
    InvalidTransaction: string;
} | {
    ExecutionFailure: string;
} | {
    OneOrMoreInputsNotFound: string;
} | {
    FailedToLockInputs: string;
} | {
    FailedToLockOutputs: string;
} | "ForeignPledgeInputConflict" | {
    ForeignShardGroupDecidedToAbort: {
        start_shard: number;
        end_shard: number;
        abort_reason: string;
    };
} | {
    FeesNotPaid: string;
} | "Unknown";
