import {ControlledOpSet, OpList} from "./ControlledOpSet";
import {Timestamp} from "./helper/Timestamp";
import {TypedValue} from "./helper/TypedValue";
import {
  PersistentAppliedOp,
  persistentDoOpFactory,
  persistentUndoOp,
} from "./PersistentUndoHelper";
import {HashMap} from "prelude-ts";
import {CaseClass} from "./helper/CaseClass";

export class DeviceId extends TypedValue<"DeviceId", string> {}

export type PermissionedTree = ControlledOpSet<
  PermissionedTreeValue,
  AppliedOp,
  DeviceId
>;

export class PriorityStatus extends CaseClass<{
  priority: number;
  status: "open" | OpList<AppliedOp>;
}> {}
export class ParentPos extends CaseClass<{parent: NodeId; position: number}> {}

type PermissionedTreeValue = {
  writers: HashMap<DeviceId, PriorityStatus>;
  nodes: HashMap<NodeId, ParentPos>;
};
export class NodeId extends TypedValue<"NodeId", string> {}

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
  type: "create node";
  node: NodeId;
  parent: NodeId;
  position: number;
};
export type SetParent = {
  timestamp: Timestamp;
  type: "set parent";
  node: NodeId;
  parent: NodeId;
  position: number;
};

export function createPermissionedTree(owner: DeviceId): PermissionedTree {
  return ControlledOpSet<PermissionedTreeValue, AppliedOp, DeviceId>.create(
    persistentDoOpFactory((value, op) => {
      switch (op.type) {
        case "set writer":
          const devicePriority = value.writers
            .get(op.device)
            .getOrThrow("Cannot find writer entry for op author").p.priority;
          const writerPriority = value.writers
            .get(op.targetWriter)
            .getOrUndefined()?.p.priority;
          if (writerPriority !== undefined && writerPriority >= devicePriority)
            return value;
          if (op.priority >= devicePriority) return value;

          return {
            ...value,
            writers: value.writers.put(
              op.targetWriter,
              new PriorityStatus({
                priority: op.priority,
                status: op.status,
              }),
            ),
          };
        case "create node":
          if (value.nodes.containsKey(op.node)) return value;
          return {
            ...value,
            nodes: value.nodes.put(
              op.node,
              new ParentPos({parent: op.parent, position: op.position}),
            ),
          };
        case "set parent":
          if (
            ancestor(value.nodes, op.node, op.parent) ||
            !value.nodes.containsKey(op.node)
          )
            return value;
          return {
            ...value,
            nodes: value.nodes.put(
              op.node,
              new ParentPos({parent: op.parent, position: op.position}),
            ),
          };
      }
    }),
    persistentUndoOp,
    (value) => value.writers.map((device, info) => [device, info.p.status]),
    {
      writers: HashMap.of([
        owner,
        new PriorityStatus({priority: 0, status: "open"}),
      ]),
      nodes: HashMap.empty(),
    },
  );
}

// Is `parent` an ancestor of (or identical to) `child`?
function ancestor(
  tree: HashMap<NodeId, ParentPos>,
  parent: NodeId,
  child: NodeId,
): boolean {
  if (child === parent) return true;
  const childInfo = tree.get(child);
  return childInfo
    .map((info) => ancestor(tree, parent, info.p.parent))
    .getOrElse(false);
}
