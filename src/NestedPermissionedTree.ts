import {ControlledOpSet, OpList} from "./ControlledOpSet";
import {Timestamp} from "./helper/Timestamp";
import {TypedValue} from "./helper/TypedValue";
import {
  PersistentAppliedOp,
  persistentDoOpFactory,
  persistentUndoOp,
} from "./PersistentUndoHelper";
import {HashMap, Option} from "prelude-ts";
import {ObjectValue} from "./helper/ObjectValue";
import {match} from "ts-pattern";
export type NestedPermissionedTree = ControlledOpSet<Tree, AppliedOp, StreamId>;

// Creates a new PermissionedTreeValue with shareId.creator as the initial
// writer.
export function createPermissionedTree(
  shareId: ShareId,
): NestedPermissionedTree {
  const rootKey = new NodeKey({shareId: shareId, nodeId: shareId.id});
  return ControlledOpSet<Tree, AppliedOp, StreamId>.create(
    persistentDoOpFactory((value, op, deviceId) => {
      return value.doOp(op, deviceId);
    }),
    persistentUndoOp,
    (value) => value.desiredHeads(),
    new Tree({
      rootKey: rootKey,
      // Because this is empty, desiredWriters will fill in a default writer for
      // the root shared node.
      roots: HashMap.of([
        rootKey,
        new SharedNode({
          id: rootKey.nodeId,
          shareId,
          shareData: new ShareData({
            writers: HashMap.of([
              shareId.creator,
              new PriorityStatus({priority: 0, status: "open"}),
            ]),
          }),
          children: HashMap.of(),
        }),
      ]),
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
  shareId: undefined | ShareId;
};
type SetParent = {
  timestamp: Timestamp;
  device: DeviceId;
  type: "set parent";

  node: NodeId;
  shareId: Option<ShareId>; // Only if the moved node is a share root.
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

export class NodeKey extends ObjectValue<{
  readonly nodeId: NodeId;
  readonly shareId: ShareId;
}>() {}

class Tree extends ObjectValue<{
  readonly rootKey: NodeKey;
  roots: HashMap<NodeKey, SharedNode>;
}>() {
  doOp(op: AppliedOp["op"], streamId: StreamId): this {
    if (op.type === "create node") {
      if (
        Tree.nodeForNodeKey(
          this.roots,
          new NodeKey({shareId: streamId.shareId, nodeId: op.node}),
        ).isSome()
      )
        // The node already exists. Do nothing.
        return this;
      const node = SharedNode.createNode(streamId, op);
      return this.copy({
        roots: this.roots.put(
          new NodeKey({shareId: node.shareId, nodeId: op.node}),
          node,
        ),
      });
    } else if (op.type === "set parent") {
      const node = Tree.nodeForNodeKey(
        this.roots,
        new NodeKey({
          shareId: op.shareId.getOrElse(streamId.shareId),
          nodeId: op.node,
        }),
      );
      if (node.isNone()) return this;

      const roots1 = mapValuesStable(this.roots, (root) =>
        root.doOp(op, streamId, node.get()),
      );
      if (roots1 === this.roots) return this;

      const nodeKey = new NodeKey({shareId: streamId.shareId, nodeId: op.node});
      if (Tree.nodeForNodeKey(roots1, nodeKey).isNone()) {
        // The node is nowhere in the tree. Make it a root.
        return this.copy({roots: roots1.put(nodeKey, node.get())});
      }
      const rootsWithoutNode = roots1.remove(nodeKey);
      if (Tree.nodeForNodeKey(rootsWithoutNode, nodeKey).isSome())
        // The node is somewhere else in the tree. Remove it as a root.
        return this.copy({roots: rootsWithoutNode});
      return this.copy({roots: roots1});
    } else {
      const roots1 = mapValuesStable(this.roots, (root) =>
        root.doOp(op, streamId, undefined),
      );
      if (roots1 === this.roots) return this;
      return this.copy({roots: roots1});
    }
  }

  desiredHeads(): HashMap<StreamId, "open" | OpList<AppliedOp>> {
    return this.roots.get(this.rootKey).getOrThrow().desiredHeads();
  }

  static nodeForNodeKey(
    roots: HashMap<NodeKey, SharedNode>,
    nodeKey: NodeKey,
  ): Option<SharedNode> {
    return roots.foldLeft(Option.none<SharedNode>(), (soFar, [key, root]) =>
      soFar.orCall(() => root.nodeForNodeKey(nodeKey)),
    );
  }
}

export class ShareData extends ObjectValue<{
  writers: HashMap<DeviceId, PriorityStatus>;
}>() {}

export class SharedNode extends ObjectValue<{
  readonly shareId: ShareId;
  readonly id: NodeId;
  shareData: undefined | ShareData;
  children: HashMap<NodeId, NodeInfo>;
}>() {
  doOp(
    op: Exclude<AppliedOp["op"], CreateNode>,
    streamId: StreamId,
    // Defined if op is a "set parent".
    original: SharedNode | undefined,
  ): this {
    if (
      op.type === "set writer" &&
      this.shareData &&
      this.shareId === streamId.shareId
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
    }

    const children1 = match({op, children: this.children, id: this.id})
      .with(
        {op: {type: "set parent"}},
        ({op, id, children}) => op.parent === id,
        ({op, children}) => {
          const node: SharedNode = original!;
          // xcxc if (node.contains(this.id)) return this;
          return children.put(
            node.id,
            new NodeInfo({node, position: op.position}),
          );
        },
      )
      .with(
        {op: {type: "set parent"}},
        ({op, id, children}) =>
          op.parent !== id && children.containsKey(op.node),
        ({op, children}) => children.remove(op.node),
      )
      .otherwise(({children}) => children);

    const children2 = mapValuesStable(children1, (child) => {
      const childNode1 = child.node.doOp(op, streamId, original);
      if (childNode1 === child.node) return child;
      return child.copy({node: childNode1});
    });
    if (children2 === this.children) return this;
    return this.copy({children: children2});
  }

  static createNode(streamId: StreamId, op: CreateNode): SharedNode {
    return new SharedNode({
      shareId: op.shareId ? op.shareId : streamId.shareId,
      id: op.node,
      children: HashMap.of(),
      shareData: op.shareId
        ? new ShareData({
            writers: HashMap.of([
              op.shareId.creator,
              new PriorityStatus({priority: 0, status: "open"}),
            ]),
          })
        : undefined,
    });
  }

  desiredHeads(): HashMap<StreamId, "open" | OpList<AppliedOp>> {
    const shareData = this.shareData;
    const ourDesiredHeads = shareData
      ? HashMap.ofIterable(
          shareData.writers.map((deviceId, {status}) => [
            new StreamId({deviceId, shareId: this.shareId}),
            "open" as const,
          ]),
        )
      : HashMap.of<StreamId, "open" | OpList<AppliedOp>>();
    return this.children.foldLeft(ourDesiredHeads, (result, [, {node}]) =>
      HashMap.ofIterable([...result, ...node.desiredHeads()]),
    );
  }

  nodeForNodeKey(nodeKey: NodeKey): Option<SharedNode> {
    if (nodeKey.shareId === this.shareId && nodeKey.nodeId === this.id)
      return Option.of(this);
    return this.children.foldLeft(
      Option.none<SharedNode>(),
      (soFar, [key, child]) =>
        soFar.orCall(() => child.node.nodeForNodeKey(nodeKey)),
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
