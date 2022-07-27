import {
  asType,
  definedOrThrow,
  mapMapToMap,
  mapWith,
  RoMap,
} from "./helper/Collection";
import {ControlledOpSet, DeviceId, OpList} from "./ControlledOpSet";
import {Timestamp} from "./helper/Timestamp";
import {TypedValue} from "./helper/TypedValue";
import {
  PersistentAppliedOp,
  persistentDoOpFactory,
  persistentUndoOp,
} from "./PersistentUndoHelper";
import {HashMap} from "prelude-ts";
import {CaseClass} from "./helper/CaseClass";

export type PermissionedTree = ControlledOpSet<
  PermissionedTreeValue,
  AppliedOp
>;

export class ParentPos extends CaseClass<{parent: NodeId; position: number}> {}

type PermissionedTreeValue = {
  writers: RoMap<
    DeviceId,
    {priority: number; status: "open" | OpList<AppliedOp>}
  >;
  nodes: HashMap<NodeId, ParentPos>;
};
export class NodeId extends TypedValue<"NodeId", string> {}

export type AppliedOp = PersistentAppliedOp<
  PermissionedTreeValue,
  SetWriter | SetParent
>;
export type SetWriter = {
  timestamp: Timestamp;
  device: DeviceId;
  type: "set writer";
  targetWriter: DeviceId;
  priority: number;
  status: "open" | OpList<AppliedOp>;
};
export type SetParent = {
  timestamp: Timestamp;
  type: "set parent";
  node: NodeId;
  parent: NodeId;
  position: number;
};

export function createPermissionedTree(owner: DeviceId): PermissionedTree {
  return ControlledOpSet<PermissionedTreeValue, AppliedOp>.create(
    persistentDoOpFactory((value, op) => {
      if (op.type === "set writer") {
        const devicePriority = definedOrThrow(
          value.writers.get(op.device),
          "Cannot find writer entry for op author",
        ).priority;
        const writerPriority = value.writers.get(op.targetWriter)?.priority;
        if (writerPriority !== undefined && writerPriority >= devicePriority)
          return value;
        if (op.priority >= devicePriority) return value;

        return {
          ...value,
          writers: mapWith(value.writers, op.targetWriter, {
            priority: op.priority,
            status: op.status,
          }),
        };
      } else {
        if (ancestor(value.nodes, op.node, op.parent)) return value;
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
    (value) =>
      mapMapToMap(value.writers, (device, info) => [device, info.status]),
    asType<PermissionedTreeValue>({
      writers: RoMap([[owner, {priority: 0, status: "open"}]]),
      nodes: HashMap.empty(),
    }),
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
