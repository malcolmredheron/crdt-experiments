import {ControlledOpSet, OpList} from "./ControlledOpSet";
import {Timestamp} from "./helper/Timestamp";
import {TypedValue} from "./helper/TypedValue";
import {
  PersistentAppliedOp,
  persistentDoOpFactory,
  persistentUndoOp,
} from "./PersistentUndoHelper";
import {HashMap, HashSet, Option} from "prelude-ts";
import {ObjectValue} from "./helper/ObjectValue";
import {match} from "ts-pattern";
import {AssertFailed} from "./helper/Assert";
import {MemoizeInstance} from "./helper/Memoize";

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
      roots: HashMap.of([
        rootKey,
        new SharedNode({
          id: rootKey.nodeId,
          shareId,
          shareData: ShareData.create(shareId),
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
  type: "shared node" | "share data";
}>() {}
export class NodeId extends TypedValue<"NodeId", string> {}

// Operations
export type AppliedOp = PersistentAppliedOp<Tree, SetWriter | SetParent>;
type SetWriter = {
  timestamp: Timestamp;
  type: "set writer";

  targetWriter: ShareId;
  priority: number;
  status: "open" | OpList<AppliedOp> | undefined;
};
type SetParent = {
  timestamp: Timestamp;
  type: "set parent";

  nodeId: NodeId;
  nodeShareId: Option<ShareId>; // Only if the moved node is a share root.
  parentNodeId: NodeId;
  position: number;
};

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

  @MemoizeInstance
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
    return Tree.nodeForNodeKey(this.roots, nodeKey);
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

export class ChildInfo extends ObjectValue<{
  position: number;
  child: SharedNode;
}>() {}
export class SharedNode extends ObjectValue<{
  readonly shareId: ShareId;
  readonly id: NodeId;
  shareData: undefined | ShareData;
  children: HashMap<NodeId, ChildInfo>;
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
          streamId.type === "shared node" &&
          streamId.shareId.equals(this.shareId) &&
          op.parentNodeId === id,
        ({op, children}) => {
          if (child === undefined)
            throw new AssertFailed("child must be defined");
          return children.put(
            child.id,
            new ChildInfo({child, position: op.position}),
          );
        },
      )
      .with(
        {op: {type: "set parent"}},
        ({op, id, children}) =>
          streamId.type === "shared node" &&
          streamId.shareId.equals(this.shareId) &&
          op.parentNodeId !== id &&
          children.containsKey(op.nodeId),
        ({op, children}) => children.remove(op.nodeId),
      )
      .otherwise(({children}) => children);
    const children2 = mapValuesStable(children1, (info) => {
      const childNode1 = info.child.doOp(op, streamId, child);
      if (childNode1 === info.child) return info;
      return info.copy({child: childNode1});
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
      shareData: shareId.isSome() ? ShareData.create(realShareId) : undefined,
    });
  }

  @MemoizeInstance
  desiredHeads(): HashMap<StreamId, "open" | OpList<AppliedOp>> {
    const shareData = this.shareData;
    const ourDesiredHeads = shareData
      ? shareData.desiredHeads().flatMap((streamId, status) => [
          [streamId, status],
          [streamId.copy({type: "shared node"}), status],
        ])
      : HashMap.of<StreamId, "open" | OpList<AppliedOp>>();
    return this.children.foldLeft(ourDesiredHeads, (result, [, {child}]) =>
      HashMap.ofIterable([...result, ...child.desiredHeads()]),
    );
  }

  nodeForNodeKey(nodeKey: NodeKey): Option<SharedNode> {
    if (nodeKey.shareId.equals(this.shareId) && nodeKey.nodeId === this.id)
      return Option.of(this);
    return this.children.foldLeft(
      Option.none<SharedNode>(),
      (soFar, [key, info]) =>
        soFar.orCall(() => info.child.nodeForNodeKey(nodeKey)),
    );
  }
}

export class WriterInfo extends ObjectValue<{
  // The creator of a shared node is always a writer, so these are actually the
  // extra writers.
  writer: ShareData;

  // ##WriterPriority: For now this is removed, but the idea is that we store a
  // priority for every writer node and from that can find a priority path for
  // any writer device (remove dups in favor of higher priority). Then we
  // disallow changes to higher priority shares by lower priority devices.
  // priority: number;

  // Open: active
  // OpList: closed after the listed operation
  // undefined: removed completely
  status: "open" | OpList<AppliedOp> | undefined;
}>() {}

export class ShareData extends ObjectValue<{
  readonly shareId: ShareId;
  writers: HashMap<ShareId, WriterInfo>;
}>() {
  static create(shareId: ShareId): ShareData {
    return new ShareData({
      shareId: shareId,
      writers: HashMap.of(),
    });
  }

  doOp(op: SetWriter, streamId: StreamId): this {
    if (streamId.type === "share data" && this.shareId === streamId.shareId) {
      const writerInfoOpt = this.writers.get(op.targetWriter);
      // #WriterPriority
      // const devicePriority = this.writers
      //   .get(op.device)
      //   .getOrThrow("Cannot find writer entry for op author").priority;
      // const writerPriority = writerInfoOpt.getOrUndefined()?.priority;
      // if (writerPriority !== undefined && writerPriority >= devicePriority)
      //   return this;
      // if (op.priority >= devicePriority) return this;

      return this.copy({
        writers: this.writers.put(
          op.targetWriter,
          new WriterInfo({
            writer: writerInfoOpt
              .map((wi) => wi.writer)
              .getOrCall(() => ShareData.create(op.targetWriter)),
            // #WriterPriority
            // priority: op.priority,
            status: op.status,
          }),
        ),
      });
    }

    return this;
  }

  @MemoizeInstance
  desiredHeads(): HashMap<StreamId, "open" | OpList<AppliedOp>> {
    const ourDesiredHeads = HashMap.ofIterable(
      this.writerDevices()
        .toArray()
        .map((deviceId) => [
          new StreamId({
            deviceId,
            shareId: this.shareId,
            type: "share data",
          }),
          // TODO: need to report the correct status
          "open" as "open" | OpList<AppliedOp>,
        ]),
    );
    return this.writers.foldLeft(ourDesiredHeads, (heads, [, {writer}]) =>
      HashMap.ofIterable([...heads, ...writer.desiredHeads()]),
    );
  }

  writerDevices(): HashSet<DeviceId> {
    return this.writers.foldLeft(
      HashSet.of(this.shareId.creator),
      (devices, [, writerInfo]) =>
        HashSet.ofIterable([...devices, ...writerInfo.writer.writerDevices()]),
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
