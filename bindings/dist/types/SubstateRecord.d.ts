import type { Epoch } from "./Epoch";
import type { NodeHeight } from "./NodeHeight";
import type { Shard } from "./Shard";
import type { SubstateDestroyed } from "./SubstateDestroyed";
import type { SubstateId } from "./SubstateId";
import type { SubstateValue } from "./SubstateValue";
export interface SubstateRecord {
    substate_id: SubstateId;
    version: number;
    substate_value: SubstateValue | null;
    state_hash: string;
    created_by_transaction: string;
    created_justify: string;
    created_block: string;
    created_height: NodeHeight;
    created_by_shard: Shard;
    created_at_epoch: Epoch;
    destroyed: SubstateDestroyed | null;
}
