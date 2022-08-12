import {ControlledOpSet, OpList} from "./ControlledOpSet";
import {Timestamp} from "./helper/Timestamp";
import {TypedValue} from "./helper/TypedValue";
import {
  PersistentAppliedOp,
  persistentDoOpFactory,
  persistentUndoOp,
} from "./PersistentUndoHelper";
import {HashMap, HashSet} from "prelude-ts";
import {ObjectValue} from "./helper/ObjectValue";

export type NestedPermissionedTree = ControlledOpSet<Tree, AppliedOp, StreamId>;

// Creates a new PermissionedTreeValue with shareId.creator as the initial
// writer.
export function createPermissionedTree(
  shareId: ShareId,
): NestedPermissionedTree {
  return ControlledOpSet<Tree, AppliedOp, StreamId>.create(
    persistentDoOpFactory((value, op, deviceId) => {
      return value.doOp(op, deviceId);
    }),
    persistentUndoOp,
    (value) => value.desiredHeads(),
    new Tree({
      root: shareId,
      // Because this is empty, desiredWriters will fill in a default writer for
      // the root shared node.
      sharedNodes: HashMap.of(),
    }),
  );
}

export class DeviceId extends TypedValue<"DeviceId", string> {}
export class ShareId extends ObjectValue<{creator: DeviceId; id: string}>() {}
export class StreamId extends ObjectValue<{
  deviceId: DeviceId;
  shareId: ShareId;
}>() {}
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
  shareId: undefined | ShareId;
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
  readonly shareId: undefined | ShareId;
}>() {}

class Tree extends ObjectValue<{
  readonly root: ShareId;
  sharedNodes: HashMap<ShareId, SharedNode>;
}>() {
  doOp(op: AppliedOp["op"], streamId: StreamId): this {
    const sharedNode = this.sharedNodeForId(streamId.shareId);
    const sharedNode1 = sharedNode.doOp(op);
    return this.copy({
      sharedNodes: this.sharedNodes.put(streamId.shareId, sharedNode1),
    });
  }

  desiredHeads(): HashMap<StreamId, "open" | OpList<AppliedOp>> {
    // This is not idiomatic functional code, but it's much faster than what we
    // had before.
    let shareIdsProcessed = HashSet.of<ShareId>();
    const entries = new Array<[StreamId, "open" | OpList<AppliedOp>]>();
    const processShareId = (shareId: ShareId): void => {
      if (shareIdsProcessed.contains(shareId)) return;
      shareIdsProcessed = shareIdsProcessed.add(shareId);

      const sharedNode = this.sharedNodeForId(shareId);
      sharedNode.writers.forEach(([deviceId, info]) =>
        entries.push([
          new StreamId({deviceId: deviceId, shareId: shareId}),
          info.status,
        ]),
      );
      sharedNode.nodes.forEach(([, {shareId}]) => {
        if (shareId !== undefined) processShareId(shareId);
      });
    };
    processShareId(this.root);
    return HashMap.ofIterable(entries);
  }

  // Gets the shared node for a share id, or makes the initial one if we don't
  // have it.
  sharedNodeForId(shareId: ShareId): SharedNode {
    return this.sharedNodes.get(shareId).getOrCall(
      () =>
        new SharedNode({
          nodes: HashMap.of(),
          writers: HashMap.of([
            shareId.creator,
            new PriorityStatus({priority: 0, status: "open"}),
          ]),
        }),
    );
  }
}

export class SharedNode extends ObjectValue<{
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
              shareId: op.shareId,
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
