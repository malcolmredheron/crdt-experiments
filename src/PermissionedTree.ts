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

export class DeviceId extends TypedValue<"DeviceId", string> {}
export class ShareId extends TypedValue<"ShareId", string> {}
export class StreamId extends ObjectValue<{
  deviceId: DeviceId;
  shareId: ShareId;
}>() {}

export type PermissionedTree = ControlledOpSet<
  PermissionedTreeValue,
  AppliedOp,
  StreamId
>;

class PermissionedTreeValue extends ObjectValue<{
  readonly shareId: ShareId;
  writers: HashMap<DeviceId, PriorityStatus>;
  nodes: HashMap<NodeId, NodeInfo>;
}>() {}
export class NodeId extends TypedValue<"NodeId", string> {}
export class PriorityStatus extends ObjectValue<{
  priority: number;
  status: "open" | OpList<AppliedOp>;
}>() {}
export class NodeInfo extends ObjectValue<{
  parent: NodeId;
  position: number;
  subtree: undefined | PermissionedTreeValue;
}>() {}

export type AppliedOp = PersistentAppliedOp<
  PermissionedTreeValue,
  SetWriter | CreateNode | SetParent
>;
export type SetWriter = {
  timestamp: Timestamp;
  device: DeviceId;
  type: "set writer";
  targetWriter: DeviceId;
  priority: number;
  status: "open" | OpList<AppliedOp>;
};
export type CreateNode = {
  timestamp: Timestamp;
  device: DeviceId;
  type: "create node";
  node: NodeId;
  parent: NodeId;
  position: number;
  shareId: undefined | ShareId;
};
export type SetParent = {
  timestamp: Timestamp;
  device: DeviceId;
  type: "set parent";
  node: NodeId;
  parent: NodeId;
  position: number;
};

export function createPermissionedTree(
  ownerStreamId: StreamId,
): PermissionedTree {
  return ControlledOpSet<PermissionedTreeValue, AppliedOp, StreamId>.create(
    persistentDoOpFactory((value, op) => {
      switch (op.type) {
        case "set writer":
          const devicePriority = value.writers
            .get(op.device)
            .getOrThrow("Cannot find writer entry for op author").priority;
          const writerPriority = value.writers
            .get(op.targetWriter)
            .getOrUndefined()?.priority;
          if (writerPriority !== undefined && writerPriority >= devicePriority)
            return value;
          if (op.priority >= devicePriority) return value;

          return value.copy({
            writers: value.writers.put(
              op.targetWriter,
              new PriorityStatus({
                priority: op.priority,
                status: op.status,
              }),
            ),
          });
        case "create node":
          if (value.nodes.containsKey(op.node)) return value;
          return value.copy({
            nodes: value.nodes.put(
              op.node,
              new NodeInfo({
                parent: op.parent,
                position: op.position,
                subtree: op.shareId
                  ? newPermissionedTreeValue(
                      new StreamId({deviceId: op.device, shareId: op.shareId}),
                    )
                  : undefined,
              }),
            ),
          });
        case "set parent":
          const nodeInfo = value.nodes.get(op.node);
          if (ancestor(value.nodes, op.node, op.parent) || nodeInfo.isNone())
            return value;
          return value.copy({
            nodes: value.nodes.put(
              op.node,
              nodeInfo.get().copy({parent: op.parent, position: op.position}),
            ),
          });
      }
    }),
    persistentUndoOp,
    (value) =>
      value.writers.map((device, info) => [
        new StreamId({deviceId: device, shareId: value.shareId}),
        info.status,
      ]),
    newPermissionedTreeValue(ownerStreamId),
  );
}

// Creates a new PermissionedTreeValue with ownerStreamId as a initial writer.
function newPermissionedTreeValue(
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

// Is `parent` an ancestor of (or identical to) `child`?
function ancestor(
  tree: HashMap<NodeId, NodeInfo>,
  parent: NodeId,
  child: NodeId,
): boolean {
  if (child === parent) return true;
  const childInfo = tree.get(child);
  return childInfo
    .map((info) => ancestor(tree, parent, info.parent))
    .getOrElse(false);
}
