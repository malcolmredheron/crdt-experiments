import {asType} from "./helper/Collection";
import {ControlledOpSet, DeviceId, OpList} from "./ControlledOpSet";
import {Timestamp} from "./helper/Timestamp";
import {TypedValue} from "./helper/TypedValue";
import {
  PersistentAppliedOp,
  persistentDoOpFactory,
  persistentUndoOp,
} from "./PersistentUndoHelper";
import {areEqual, fieldsHashCode, HashMap} from "prelude-ts";

export type PermissionedTree = ControlledOpSet<
  PermissionedTreeValue,
  AppliedOp
>;

class ParentPos {
  public parent: NodeId;
  public position: number;
  constructor(parent: NodeId, position: number) {
    this.parent = parent;
    this.position = position;
  }
  equals(other: ParentPos | undefined): boolean {
    if (other) {
      return (
        areEqual(this.parent, other.parent) &&
        areEqual(this.position, other.position)
      );
    } else {
      return false;
    }
  }
  hashCode(): number {
    return fieldsHashCode(this.parent, this.position);
  }
  toString(): string {
    return `{parent:${this.parent}, position:${this.position}}`;
  }
}

class PriorityStatus {
  public priority: number;
  public status: "open" | OpList<AppliedOp>;
  constructor(priority: number, status: "open" | OpList<AppliedOp>) {
    this.priority = priority;
    this.status = status;
  }
  equals(other: PriorityStatus | undefined): boolean {
    if (!other) return false;
    return (
      areEqual(this.priority, other.priority) &&
      areEqual(this.status, other.status)
    );
  }
  hashCode(): number {
    return fieldsHashCode(this.priority, this.status);
  }
}

type PermissionedTreeValue = {
  writers: HashMap<DeviceId, PriorityStatus>;
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
        const devicePriority = value.writers
          .get(op.device)
          .getOrThrow("Cannot find writer entry for op author").priority;
        const writerPriority = value.writers
          .get(op.targetWriter)
          .getOrUndefined()?.priority;
        if (writerPriority !== undefined && writerPriority >= devicePriority)
          return value;
        if (op.priority >= devicePriority) return value;

        return {
          ...value,
          writers: value.writers.put(
            op.targetWriter,
            new PriorityStatus(op.priority, op.status),
          ),
        };
      } else {
        if (ancestor(value.nodes, op.node, op.parent)) return value;
        return {
          ...value,
          nodes: value.nodes.put(
            op.node,
            new ParentPos(op.parent, op.position),
          ),
        };
      }
    }),
    persistentUndoOp,
    (value) => value.writers.mapValues((info) => info.status),
    asType<PermissionedTreeValue>({
      writers: HashMap.of([owner, new PriorityStatus(0, "open")]),
      nodes: HashMap.empty(),
    }),
  );
}

// Is `parent` an ancestor of (or identical to) `child`?
function ancestor(
  tree: HashMap<NodeId, {parent: NodeId; position: number}>,
  parent: NodeId,
  child: NodeId,
): boolean {
  if (child === parent) return true;
  const childInfo = tree.get(child);
  return childInfo
    .map((info) => ancestor(tree, parent, info.parent))
    .getOrElse(false);
}
