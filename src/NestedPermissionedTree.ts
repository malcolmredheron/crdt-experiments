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
import {AssertFailed} from "./helper/Assert";

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
            shareId,
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
export type AppliedOp = PersistentAppliedOp<Tree, SetWriter | SetParent>;
type SetWriter = {
  timestamp: Timestamp;
  device: DeviceId;
  type: "set writer";

  targetWriter: DeviceId;
  priority: number;
  status: "open" | OpList<AppliedOp>;
};
type SetParent = {
  timestamp: Timestamp;
  device: DeviceId;
  type: "set parent";

  nodeId: NodeId;
  nodeShareId: Option<ShareId>; // Only if the moved node is a share root.
  parentNodeId: NodeId;
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
    if (op.type === "set parent") {
      const childKey = new NodeKey({
        shareId: op.nodeShareId.getOrElse(streamId.shareId),
        nodeId: op.nodeId,
      });
      const child = Tree.nodeForNodeKey(this.roots, childKey).getOrCall(() =>
        SharedNode.createNode(streamId, op.nodeId, op.nodeShareId),
      );

      const parentKey = new NodeKey({
        shareId: streamId.shareId,
        nodeId: op.parentNodeId,
      });

      if (child.nodeForNodeKey(parentKey).isSome()) {
        // Avoid creating a cycle.
        return this;
      }

      const parentOpt = Tree.nodeForNodeKey(this.roots, parentKey);
      const parentInTree = parentOpt.isSome();
      const parent = parentOpt.getOrCall(() =>
        SharedNode.createNode(
          streamId,
          op.parentNodeId,
          streamId.shareId.id === op.parentNodeId
            ? Option.some(streamId.shareId)
            : Option.none(),
        ),
      );

      const roots1 = mapValuesStable(this.roots, (root) =>
        root.doOp(op, streamId, child),
      );
      const roots2 = !parentInTree
        ? roots1.put(parentKey, parent.doOp(op, streamId, child))
        : roots1;
      if (roots2 === this.roots) return this;

      const rootsWithoutNode = roots2.remove(childKey);
      if (Tree.nodeForNodeKey(rootsWithoutNode, childKey).isSome())
        // The node is somewhere else in the tree. Remove it as a root.
        return this.copy({roots: rootsWithoutNode});

      return this.copy({roots: roots2});
    } else if (op.type === "set writer") {
      const shareKey = new NodeKey({
        shareId: streamId.shareId,
        nodeId: streamId.shareId.id,
      });
      const roots1 = match({
        roots: this.roots,
        sharePresent: Tree.nodeForNodeKey(this.roots, shareKey).isSome(),
      })
        .with({sharePresent: true}, ({roots}) =>
          mapValuesStable(roots, (root) => root.doOp(op, streamId, undefined)),
        )
        .with({sharePresent: false}, ({roots}) =>
          roots.put(
            shareKey,
            SharedNode.createNode(
              streamId,
              streamId.shareId.id,
              Option.some(streamId.shareId),
            ).doOp(op, streamId, undefined),
          ),
        )
        .exhaustive();
      if (roots1 === this.roots) return this;
      return this.copy({roots: roots1});
    } else {
      throw new AssertFailed("unknown op type");
    }
  }

  desiredHeads(): HashMap<StreamId, "open" | OpList<AppliedOp>> {
    // We need to report desired heads based on all of the roots, otherwise we
    // won't have the ops to reconstruct subtrees that used to be in the main
    // tree but aren't now.
    return this.roots.foldLeft(
      HashMap.of<StreamId, "open" | OpList<AppliedOp>>(),
      (result, [, node]) =>
        HashMap.ofIterable([...result, ...node.desiredHeads()]),
    );
  }

  nodeForNodeKey(nodeKey: NodeKey): Option<SharedNode> {
    return this.roots.foldLeft(
      Option.none<SharedNode>(),
      (soFar, [key, root]) => soFar.orCall(() => root.nodeForNodeKey(nodeKey)),
    );
  }

  static nodeForNodeKey(
    roots: HashMap<NodeKey, SharedNode>,
    nodeKey: NodeKey,
  ): Option<SharedNode> {
    return roots.foldLeft(Option.none<SharedNode>(), (soFar, [key, root]) =>
      soFar.orCall(() => root.nodeForNodeKey(nodeKey)),
    );
  }

  root(): SharedNode {
    return Tree.nodeForNodeKey(this.roots, this.rootKey).getOrThrow();
  }
}

export class SharedNode extends ObjectValue<{
  readonly shareId: ShareId;
  readonly id: NodeId;
  shareData: undefined | ShareData;
  children: HashMap<NodeId, NodeInfo>;
}>() {
  doOp(
    op: AppliedOp["op"],
    streamId: StreamId,
    // Defined if op is a "set parent".
    child: SharedNode | undefined,
  ): this {
    const shareData1 =
      op.type === "set writer" && this.shareData
        ? this.shareData.doOp(op, streamId)
        : this.shareData;

    const children1 = match({op, children: this.children, id: this.id})
      .with(
        {op: {type: "set parent"}},
        ({op, id, children}) =>
          streamId.shareId.equals(this.shareId) && op.parentNodeId === id,
        ({op, children}) => {
          const node: SharedNode = child!;
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
          streamId.shareId.equals(this.shareId) &&
          op.parentNodeId !== id &&
          children.containsKey(op.nodeId),
        ({op, children}) => children.remove(op.nodeId),
      )
      .otherwise(({children}) => children);
    const children2 = mapValuesStable(children1, (info) => {
      const childNode1 = info.node.doOp(op, streamId, child);
      if (childNode1 === info.node) return info;
      return info.copy({node: childNode1});
    });

    if (shareData1 == this.shareData && children2 === this.children)
      return this;
    return this.copy({shareData: shareData1, children: children2});
  }

  static createNode(
    streamId: StreamId,
    nodeId: NodeId,
    shareId: Option<ShareId>,
  ): SharedNode {
    const realShareId = shareId.getOrElse(streamId.shareId);
    return new SharedNode({
      shareId: realShareId,
      id: nodeId,
      children: HashMap.of(),
      shareData: shareId.isSome()
        ? new ShareData({
            shareId: realShareId,
            writers: HashMap.of([
              shareId.get().creator,
              new PriorityStatus({priority: 0, status: "open"}),
            ]),
          })
        : undefined,
    });
  }

  desiredHeads(): HashMap<StreamId, "open" | OpList<AppliedOp>> {
    const shareData = this.shareData;
    const ourDesiredHeads = shareData
      ? shareData.desiredHeads()
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

export class ShareData extends ObjectValue<{
  readonly shareId: ShareId;
  writers: HashMap<DeviceId, PriorityStatus>;
}>() {
  doOp(op: SetWriter, streamId: StreamId): this {
    if (this.shareId === streamId.shareId) {
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
    }

    return this;
  }

  desiredHeads(): HashMap<StreamId, "open" | OpList<AppliedOp>> {
    return HashMap.ofIterable(
      this.writers.map((deviceId, {status}) => [
        new StreamId({deviceId, shareId: this.shareId}),
        "open" as const,
      ]),
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
