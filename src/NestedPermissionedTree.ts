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
export class ShareId extends ObjectValue<{creator: DeviceId; id: NodeId}>() {}
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
  position: number;
  node: SharedNode;
}>() {}

class Tree extends ObjectValue<{
  readonly root: ShareId;
  sharedNodes: HashMap<ShareId, SharedNode>;
}>() {
  doOp(op: AppliedOp["op"], streamId: StreamId): this {
    const sharedNode = this.sharedNodeForId(streamId.shareId);
    const sharedNode1 = sharedNode.doOp(op, streamId);
    return this.copy({
      sharedNodes: this.sharedNodes.put(streamId.shareId, sharedNode1),
    });
  }

  desiredHeads(): HashMap<StreamId, "open" | OpList<AppliedOp>> {
    return this.sharedNodeForId(this.root).desiredHeads();
  }

  // Gets the shared node for a share id, or makes the initial one if we don't
  // have it.
  sharedNodeForId(shareId: ShareId): SharedNode {
    return this.sharedNodes.get(shareId).getOrCall(
      () =>
        new SharedNode({
          id: shareId.id,
          shareData: new ShareData({
            shareId,
            writers: HashMap.of([
              shareId.creator,
              new PriorityStatus({priority: 0, status: "open"}),
            ]),
          }),
          children: HashMap.of(),
        }),
    );
  }
}

class ShareData extends ObjectValue<{
  readonly shareId: ShareId;
  writers: HashMap<DeviceId, PriorityStatus>;
}>() {}

export class NodeChild extends ObjectValue<{
  node: SharedNode;
  position: number;
}>() {}

export class SharedNode extends ObjectValue<{
  readonly id: NodeId;
  shareData: undefined | ShareData;
  children: HashMap<NodeId, NodeChild>;
}>() {
  doOp<T extends AppliedOp["op"]>(
    op: T,
    streamId: StreamId,
    original: T extends SetParent ? SharedNode : never,
  ): this {
    if (
      op.type === "set writer" &&
      this.shareData &&
      this.shareData.shareId === streamId.shareId
    ) {
      const devicePriority = this.shareData.writers
        .get(op.device)
        .getOrThrow("Cannot find writer entry for op author").priority;
      const writerPriority = this.shareData.writers
        .get(op.targetWriter)
        .getOrUndefined()?.priority;
      if (writerPriority !== undefined && writerPriority >= devicePriority)
        return this;
      if (op.priority >= devicePriority) return this;

      return this.copy({
        shareData: this.shareData.copy({
          writers: this.shareData.writers.put(
            op.targetWriter,
            new PriorityStatus({
              priority: op.priority,
              status: op.status,
            }),
          ),
        }),
      });
    } else if (op.type === "create node" && op.parent === this.id) {
      // xcxc if (this.nodes.containsKey(op.node)) return this;
      return this.copy({
        children: this.children.put(
          op.node,
          new NodeChild({
            node: new SharedNode({
              id: op.node,
              children: HashMap.of(),
              shareData: undefined,
            }),
            position: op.position,
          }),
        ),
      });
    } else if (op.type === "set parent" && op.parent === this.id) {
      const node: SharedNode = original;
      // xcxc if (node.contains(this.id)) return this;
      return this.copy({
        children: this.children.put(
          node.id,
          new NodeChild({node, position: op.position}),
        ),
      });
    } else if (
      op.type === "set parent" &&
      op.parent !== this.id &&
      this.children.containsKey(op.node)
    ) {
      // xcxc if (node.contains(this.id)) return this;
      return this.copy({
        children: this.children.remove(op.node),
      });
    } else {
      const children1 = mapValuesStable(this.children, (child) => {
        const childNode1 = child.node.doOp(op, streamId, original);
        if (childNode1 === child.node) return child;
        return child.copy({node: child.node});
      });
      if (children1 === this.children) return this;
      return this.copy({children: children1});
    }
  }

  desiredHeads(): HashMap<StreamId, "open" | OpList<AppliedOp>> {
    const shareData = this.shareData;
    const ourDesiredHeads = shareData
      ? HashMap.ofIterable(
          shareData.writers.map((deviceId, {status}) => [
            new StreamId({deviceId, shareId: shareData.shareId}),
            "open" as const,
          ]),
        )
      : HashMap.of<StreamId, "open" | OpList<AppliedOp>>();
    return this.children.foldLeft(ourDesiredHeads, (result, [, {node}]) =>
      HashMap.ofIterable([...result, ...node.desiredHeads()]),
    );
  }

  allNodes(): HashMap<NodeId, SharedNode> {
    return this.children.foldLeft(
      HashMap.of([this.id, this as SharedNode]),
      (all, [, child]) =>
        child.node
          .allNodes()
          .foldLeft(all, (all, [id, child]) => all.put(id, child)),
    );
  }
}

export function mapValuesStable<K, V>(
  map: HashMap<K, V>,
  func: (v: V) => V,
): HashMap<K, V> {
  let changed = false;
  const map1 = map.mapValues((v) => {
    const v1 = func(v);
    if (v1 !== v) changed = true;
    return v1;
  });
  return changed ? map1 : map;
}
