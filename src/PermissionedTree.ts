import {ControlledOpSet, OpList} from "./ControlledOpSet";
import {Timestamp} from "./helper/Timestamp";
import {TypedValue} from "./helper/TypedValue";
import {
  PersistentAppliedOp,
  persistentDoOpFactory,
  persistentUndoOp,
} from "./PersistentUndoHelper";
import {HashMap} from "prelude-ts";
import {ObjectValue} from "./helper/ObjectValue";

export type PermissionedTree = ControlledOpSet<Tree, AppliedOp, DeviceId>;

// Creates a new PermissionedTreeValue with shareId.creator as the initial
// writer.
export function createPermissionedTree(
  ownerDeviceId: DeviceId,
): PermissionedTree {
  return ControlledOpSet.create<Tree, AppliedOp, DeviceId>(
    persistentDoOpFactory((value, op, deviceId) => value.doOp(op)),
    persistentUndoOp,
    (value) => value.desiredHeads(),
    new Tree({
      ownerDeviceId,
      writers: HashMap.of([
        ownerDeviceId,
        new PriorityStatus({priority: 0, status: "open"}),
      ]),
      nodes: HashMap.empty(),
    }),
  );
}

export class DeviceId extends TypedValue<"DeviceId", string> {}
export class NodeId extends TypedValue<"NodeId", string> {}

// Operations
export type AppliedOp = PersistentAppliedOp<
  Tree,
  SetWriter | CreateNode | SetParent
>;
type SetWriter = {
  timestamp: Timestamp;
  device: DeviceId;
  type: "set writer";

  targetWriter: DeviceId;
  priority: number;
  status: "open" | OpList<AppliedOp>;
};
type CreateNode = {
  timestamp: Timestamp;
  device: DeviceId;
  type: "create node";

  node: NodeId;
  parent: NodeId;
  position: number;
};
type SetParent = {
  timestamp: Timestamp;
  device: DeviceId;
  type: "set parent";

  node: NodeId;
  parent: NodeId;
  position: number;
};

// Internal types
export class PriorityStatus extends ObjectValue<{
  priority: number;
  status: "open" | OpList<AppliedOp>;
}>() {}
export class NodeInfo extends ObjectValue<{
  parent: NodeId;
  position: number;
}>() {}

class Tree extends ObjectValue<{
  ownerDeviceId: DeviceId;
  writers: HashMap<DeviceId, PriorityStatus>;
  nodes: HashMap<NodeId, NodeInfo>;
}>() {
  doOp(op: AppliedOp["op"]): this {
    switch (op.type) {
      case "set writer":
        const devicePriority = this.writers
          .get(op.device)
          .getOrThrow("Cannot find writer entry for op author").priority;
        const writerPriority = this.writers
          .get(op.targetWriter)
          .getOrUndefined()?.priority;
        if (writerPriority !== undefined && writerPriority >= devicePriority)
          return this;
        if (op.priority >= devicePriority) return this;

        return this.copy({
          writers: this.writers.put(
            op.targetWriter,
            new PriorityStatus({
              priority: op.priority,
              status: op.status,
            }),
          ),
        });
      case "create node":
        if (this.nodes.containsKey(op.node)) return this;
        return this.copy({
          nodes: this.nodes.put(
            op.node,
            new NodeInfo({
              parent: op.parent,
              position: op.position,
            }),
          ),
        });
      case "set parent":
        const nodeInfo = this.nodes.get(op.node);
        if (this.ancestor(op.node, op.parent) || nodeInfo.isNone()) return this;
        return this.copy({
          nodes: this.nodes.put(
            op.node,
            nodeInfo.get().copy({parent: op.parent, position: op.position}),
          ),
        });
    }
  }

  desiredHeads(): HashMap<DeviceId, "open" | OpList<AppliedOp>> {
    return this.writers.map((deviceId, info) => [deviceId, info.status]);
  }

  private ancestor(parent: NodeId, child: NodeId): boolean {
    if (child === parent) return true;
    const childInfo = this.nodes.get(child);
    return childInfo
      .map((info) => this.ancestor(parent, info.parent))
      .getOrElse(false);
  }
}
