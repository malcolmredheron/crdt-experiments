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

export type PermissionedTree = ControlledOpSet<
  PermissionedTreeValue,
  AppliedOp
>;

type PermissionedTreeValue = {
  writers: RoMap<
    DeviceId,
    {priority: number; status: "open" | OpList<AppliedOp>}
  >;
  nodes: RoMap<NodeId, {parent: NodeId; position: number}>;
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
          nodes: mapWith(value.nodes, op.node, {
            parent: op.parent,
            position: op.position,
          }),
        };
      }
    }),
    persistentUndoOp,
    (value) =>
      mapMapToMap(value.writers, (device, info) => [device, info.status]),
    asType<PermissionedTreeValue>({
      writers: RoMap([[owner, {priority: 0, status: "open"}]]),
      nodes: RoMap([]),
    }),
  );
}

// Is `parent` an ancestor of (or identical to) `child`?
function ancestor(
  tree: RoMap<NodeId, {parent: NodeId; position: number}>,
  parent: NodeId,
  child: NodeId,
): boolean {
  if (child === parent) return true;
  const childInfo = tree.get(child);
  if (!childInfo) return false;
  return ancestor(tree, parent, childInfo.parent);
}
