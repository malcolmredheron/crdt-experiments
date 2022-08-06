import {ControlledOpSet, OpList} from "./ControlledOpSet";
import {Timestamp} from "./helper/Timestamp";
import {TypedValue} from "./helper/TypedValue";
import {
  PersistentAppliedOp,
  persistentDoOpFactory,
  persistentUndoOp,
} from "./PersistentUndoHelper";
import {HashMap, Option, Vector} from "prelude-ts";
import {ObjectValue} from "./helper/ObjectValue";

export type PermissionedTree = ControlledOpSet<
  PermissionedTreeValue,
  AppliedOp,
  StreamId
>;

export function createPermissionedTree(
  ownerStreamId: StreamId,
): PermissionedTree {
  return ControlledOpSet<PermissionedTreeValue, AppliedOp, StreamId>.create(
    persistentDoOpFactory((value, op, deviceId) => {
      return value.doOp(op, deviceId);
    }),
    persistentUndoOp,
    (value) => value.desiredHeads(),
    createPermissionedTreeValue(ownerStreamId),
  );
}

export class DeviceId extends TypedValue<"DeviceId", string> {}
export class ShareId extends TypedValue<"ShareId", string> {}
export class StreamId extends ObjectValue<{
  deviceId: DeviceId;
  shareId: ShareId;
}>() {}
export class NodeId extends TypedValue<"NodeId", string> {}

// Operations
export type AppliedOp = PersistentAppliedOp<
  PermissionedTreeValue,
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
  ownerStreamId: undefined | StreamId;
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
  subtree: undefined | PermissionedTreeValue;
}>() {}

export class PermissionedTreeValue extends ObjectValue<{
  readonly shareId: ShareId;
  writers: HashMap<DeviceId, PriorityStatus>;
  nodes: HashMap<NodeId, NodeInfo>;
}>() {
  doOp(op: AppliedOp["op"], streamId: StreamId): this {
    if (this.shareId !== streamId.shareId) {
      const nodes1 = this.nodes.mapValues((nodeInfo) =>
        nodeInfo.subtree
          ? nodeInfo.copy({subtree: nodeInfo.subtree.doOp(op, streamId)})
          : nodeInfo,
      );
      if (nodes1.equals(this.nodes)) return this;
      return this.copy({nodes: nodes1});
    }
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
              subtree: op.ownerStreamId
                ? createPermissionedTreeValue(op.ownerStreamId)
                : undefined,
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

  desiredHeads(): HashMap<StreamId, "open" | OpList<AppliedOp>> {
    const directHeads = this.writers.map((device, info) => [
      new StreamId({deviceId: device, shareId: this.shareId}),
      info.status,
    ]);
    const childDesiredHeads: Vector<
      HashMap<StreamId, "open" | OpList<AppliedOp>>
    > = Vector.ofIterable(this.nodes.valueIterable()).mapOption(({subtree}) =>
      subtree === undefined ? Option.none() : Option.of(subtree.desiredHeads()),
    );
    return childDesiredHeads.fold(directHeads, (heads0, heads1) =>
      HashMap.of(...heads0, ...heads1),
    );
  }

  private ancestor(parent: NodeId, child: NodeId): boolean {
    if (child === parent) return true;
    const childInfo = this.nodes.get(child);
    return childInfo
      .map((info) => this.ancestor(parent, info.parent))
      .getOrElse(false);
  }
}

// Creates a new PermissionedTreeValue with ownerStreamId as a initial writer.
function createPermissionedTreeValue(
  ownerStreamId: StreamId,
): PermissionedTreeValue {
  return new PermissionedTreeValue({
    shareId: ownerStreamId.shareId,
    writers: HashMap.of([
      ownerStreamId.deviceId,
      new PriorityStatus({priority: 0, status: "open"}),
    ]),
    nodes: HashMap.empty(),
  });
}
