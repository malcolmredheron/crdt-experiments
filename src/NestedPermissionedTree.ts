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
import {definedOrThrow} from "./helper/Collection";

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
      shareDataRoots: HashMap.of(),
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
export type AppliedOp = PersistentAppliedOp<
  Tree,
  SetWriter | RemoveWriter | SetChild
>;

// These ops belong in the `share data` stream for the node whose writer is
// being set.
type SetWriter = {
  timestamp: Timestamp;
  type: "set writer";

  writer: ShareId;
  priority: number;
};

type RemoveWriter = {
  timestamp: Timestamp;
  type: "remove writer";

  writer: ShareId;
  // This indicates the final op that we'll accept for any streams that get
  // removed by this op.
  statuses: HashMap<StreamId, OpList<AppliedOp>>;
};

// These ops belong in the `shared node` stream for the parent node.
type SetChild = {
  timestamp: Timestamp;
  type: "set child";

  nodeId: NodeId; // The new parent
  childNodeId: NodeId; // The moved node
  childShareId: Option<ShareId>; // Only if the moved node is a share root.
  position: number;
};

export class NodeKey extends ObjectValue<{
  readonly nodeId: NodeId;
  readonly shareId: ShareId;
}>() {}

class Tree extends ObjectValue<{
  readonly rootKey: NodeKey;
  roots: HashMap<NodeKey, SharedNode>;
  shareDataRoots: HashMap<ShareId, ShareData>;
}>() {
  doOp(op: AppliedOp["op"], streamId: StreamId): this {
    if (op.type === "set child") {
      const childKey = new NodeKey({
        shareId: op.childShareId.getOrElse(streamId.shareId),
        nodeId: op.childNodeId,
      });
      const child = Tree.nodeForNodeKey(this.roots, childKey).getOrCall(() =>
        Tree.createNode(
          this.roots,
          this.shareDataRoots,
          streamId,
          op.childNodeId,
          op.childShareId,
        ),
      );

      const parentKey = new NodeKey({
        shareId: streamId.shareId,
        nodeId: op.nodeId,
      });

      if (child.nodeForNodeKey(parentKey).isSome()) {
        // Avoid creating a cycle.
        return this;
      }

      const parentOpt = Tree.nodeForNodeKey(this.roots, parentKey);
      const parentInTree = parentOpt.isSome();
      const parent = parentOpt.getOrCall(() =>
        Tree.createNode(
          this.roots,
          this.shareDataRoots,
          streamId,
          op.nodeId,
          streamId.shareId.id === op.nodeId
            ? Option.some(streamId.shareId)
            : Option.none(),
        ),
      );

      const roots1 = mapValuesStable(this.roots, (root) =>
        root.doOp(op, streamId, child, undefined),
      );
      const roots2 = !parentInTree
        ? roots1.put(parentKey, parent.doOp(op, streamId, child, undefined))
        : roots1;
      if (roots2 === this.roots) return this;

      const rootsWithoutNode = roots2.remove(childKey);
      if (Tree.nodeForNodeKey(rootsWithoutNode, childKey).isSome())
        // The node is somewhere else in the tree. Remove it as a root.
        return this.copy({roots: rootsWithoutNode});

      return this.copy({roots: roots2});
    } else if (op.type === "set writer" || op.type === "remove writer") {
      const writer =
        op.type === "remove writer"
          ? undefined
          : Tree.shareDataForShareId(
              this.roots,
              this.shareDataRoots,
              op.writer,
            ).getOrCall(() => ShareData.create(op.writer));
      const {roots: roots1, shareDataRoots: shareDataRoots1} = match({
        roots: this.roots,
        shareDataRoots: this.shareDataRoots,
        sharePresent: Tree.shareDataForShareId(
          this.roots,
          this.shareDataRoots,
          streamId.shareId,
        ).isSome(),
      })
        .with({sharePresent: true}, ({roots, shareDataRoots}) => ({
          roots: mapValuesStable(roots, (root) =>
            root.doOp(op, streamId, undefined, writer),
          ),
          shareDataRoots: mapValuesStable(shareDataRoots, (root) =>
            root.doOp(op, streamId, writer),
          ),
        }))
        .with({sharePresent: false}, ({roots, shareDataRoots}) => ({
          roots: roots,
          shareDataRoots: shareDataRoots.put(
            streamId.shareId,
            ShareData.create(streamId.shareId).doOp(op, streamId, writer),
          ),
        }))
        .exhaustive();
      if (roots1 === this.roots && shareDataRoots1 === this.shareDataRoots)
        return this;
      return this.copy({roots: roots1, shareDataRoots: shareDataRoots1});
    } else {
      throw new AssertFailed("unknown op type");
    }
  }

  @MemoizeInstance
  desiredHeads(): HashMap<StreamId, "open" | OpList<AppliedOp>> {
    // We need to report desired heads based on all of the roots, otherwise we
    // won't have the ops to reconstruct subtrees that used to be in the main
    // tree but aren't now.
    const nodeHeads = this.roots.foldLeft(
      HashMap.of<StreamId, "open" | OpList<AppliedOp>>(),
      (result, [, node]) =>
        HashMap.ofIterable([...result, ...node.desiredHeads()]),
    );
    return this.shareDataRoots.foldLeft(nodeHeads, (result, [, shareData]) =>
      HashMap.ofIterable([...result, ...shareData.desiredHeadsForShareData()]),
    );
  }

  nodeForNodeKey(nodeKey: NodeKey): Option<SharedNode> {
    return Tree.nodeForNodeKey(this.roots, nodeKey);
  }

  shareDataForShareId(shareId: ShareId): Option<ShareData> {
    return Tree.shareDataForShareId(this.roots, this.shareDataRoots, shareId);
  }

  static nodeForNodeKey(
    roots: HashMap<NodeKey, SharedNode>,
    nodeKey: NodeKey,
  ): Option<SharedNode> {
    return roots.foldLeft(Option.none<SharedNode>(), (soFar, [key, root]) =>
      soFar.orCall(() => root.nodeForNodeKey(nodeKey)),
    );
  }

  static shareDataForShareId(
    roots: HashMap<NodeKey, SharedNode>,
    shareDataRoots: HashMap<ShareId, ShareData>,
    shareId: ShareId,
  ): Option<ShareData> {
    const nodesResult = roots.foldLeft(
      Option.none<ShareData>(),
      (soFar, [key, root]) =>
        soFar.orCall(() => root.shareDataForShareId(shareId)),
    );
    if (nodesResult.isSome()) return nodesResult;
    return shareDataRoots.foldLeft(
      Option.none<ShareData>(),
      (soFar, [key, root]) =>
        soFar.orCall(() => root.shareDataForShareId(shareId)),
    );
  }

  root(): SharedNode {
    return Tree.nodeForNodeKey(this.roots, this.rootKey).getOrThrow();
  }

  static createNode(
    roots: HashMap<NodeKey, SharedNode>,
    shareDataRoots: HashMap<ShareId, ShareData>,
    streamId: StreamId,
    nodeId: NodeId,
    shareId: Option<ShareId>,
  ): SharedNode {
    const realShareId = shareId.getOrElse(streamId.shareId);
    return new SharedNode({
      shareId: realShareId,
      id: nodeId,
      children: HashMap.of(),
      shareData: shareId
        .flatMap((shareId) =>
          Tree.shareDataForShareId(roots, shareDataRoots, shareId),
        )
        .orCall(() => Option.of(ShareData.create(realShareId)))
        .getOrUndefined(),
    });
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
    // Defined if op is a "set child".
    child: undefined | SharedNode,
    // Defined if op is a "set writer".
    writer: undefined | ShareData,
  ): this {
    const shareData1 =
      (op.type === "set writer" || op.type === "remove writer") &&
      this.shareData
        ? this.shareData.doOp(op, streamId, writer)
        : this.shareData;

    const children1 = match({op, children: this.children, id: this.id})
      .with(
        {op: {type: "set child"}},
        ({op, id, children}) =>
          streamId.type === "shared node" &&
          streamId.shareId.equals(this.shareId) &&
          op.nodeId === id,
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
        {op: {type: "set child"}},
        ({op, id, children}) =>
          streamId.type === "shared node" &&
          streamId.shareId.equals(this.shareId) &&
          op.nodeId !== id &&
          children.containsKey(op.childNodeId),
        ({op, children}) => children.remove(op.childNodeId),
      )
      .otherwise(({children}) => children);
    const children2 = mapValuesStable(children1, (info) => {
      const childNode1 = info.child.doOp(op, streamId, child, writer);
      if (childNode1 === info.child) return info;
      return info.copy({child: childNode1});
    });

    if (shareData1 == this.shareData && children2 === this.children)
      return this;
    return this.copy({shareData: shareData1, children: children2});
  }

  @MemoizeInstance
  desiredHeads(): HashMap<StreamId, "open" | OpList<AppliedOp>> {
    const shareData = this.shareData;
    const ourDesiredHeads = shareData
      ? shareData
          .desiredHeadsForShareData()
          .mergeWith(shareData.desiredHeadsForSharedNode(), (left, right) => {
            throw new AssertFailed("Nothing should be in both maps");
          })
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

  shareDataForShareId(shareId: ShareId): Option<ShareData> {
    if (this.shareData) {
      if (shareId.equals(this.shareId)) return Option.of(this.shareData);
      const writersResult = this.shareData.writers.foldLeft(
        Option.none<ShareData>(),
        (soFar, [key, info]) =>
          soFar.orCall(() => info.writer.shareDataForShareId(shareId)),
      );
      if (writersResult.isSome()) return writersResult;
    }
    return this.children.foldLeft(
      Option.none<ShareData>(),
      (soFar, [key, info]) =>
        soFar.orCall(() => info.child.shareDataForShareId(shareId)),
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
}>() {}

export class ShareData extends ObjectValue<{
  readonly shareId: ShareId;
  writers: HashMap<ShareId, WriterInfo>;
  closedWriterDevicesForShareData: HashMap<DeviceId, OpList<AppliedOp>>;
  closedWriterDevicesForSharedNode: HashMap<DeviceId, OpList<AppliedOp>>;
}>() {
  static create(shareId: ShareId): ShareData {
    return new ShareData({
      shareId: shareId,
      writers: HashMap.of(),
      closedWriterDevicesForShareData: HashMap.of(),
      closedWriterDevicesForSharedNode: HashMap.of(),
    });
  }

  doOp(
    op: SetWriter | RemoveWriter,
    streamId: StreamId,
    writer: undefined | ShareData,
  ): this {
    if (streamId.type === "share data" && this.shareId === streamId.shareId) {
      if (op.type === "set writer") {
        const this1 = this.copy({
          writers: this.writers.put(
            op.writer,
            new WriterInfo({
              writer: definedOrThrow(
                writer,
                "`writer` must be defined for `set writer` op",
              ),
            }),
          ),
        });
        const newlyAddedWriterDevices = this1
          .openWriterDevices()
          .removeAll(this.openWriterDevices());
        // Anything that's newly added should no longer be listed as a closed
        // writer.
        const this2 = this1.copy({
          closedWriterDevicesForShareData:
            this1.closedWriterDevicesForShareData.filterKeys(
              (deviceId) => !newlyAddedWriterDevices.contains(deviceId),
            ),
          closedWriterDevicesForSharedNode:
            this1.closedWriterDevicesForSharedNode.filterKeys(
              (deviceId) => !newlyAddedWriterDevices.contains(deviceId),
            ),
        });
        return this2;
      } else {
        const this1 = this.copy({
          writers: this.writers.remove(op.writer),
        });
        const newlyClosedWriterDevices = this.openWriterDevices()
          .removeAll(this1.openWriterDevices())
          .toVector();
        const closedWriterDevices = (
          current: HashMap<DeviceId, OpList<AppliedOp>>,
          type: "share data" | "shared node",
        ): HashMap<DeviceId, OpList<AppliedOp>> => {
          return current.mergeWith(
            HashMap.ofIterable(
              newlyClosedWriterDevices.mapOption((removedDeviceId) =>
                op.statuses
                  .get(
                    new StreamId({
                      deviceId: removedDeviceId,
                      shareId: this.shareId,
                      type,
                    }),
                  )
                  .map((finalOp) => [removedDeviceId, finalOp]),
              ),
            ),
            (oldOp, newOp) => newOp,
          );
        };
        const this2 = this1.copy({
          closedWriterDevicesForShareData: closedWriterDevices(
            this1.closedWriterDevicesForShareData,
            "share data",
          ),
          closedWriterDevicesForSharedNode: closedWriterDevices(
            this1.closedWriterDevicesForSharedNode,
            "shared node",
          ),
        });
        return this2;
      }
    }

    return this;
  }

  @MemoizeInstance
  desiredHeadsForShareData(): HashMap<StreamId, "open" | OpList<AppliedOp>> {
    const ourHeads = this.desiredHeadsHelper(
      this.closedWriterDevicesForShareData,
      "share data",
    );
    // We also need the heads for the writers, since they help to select the ops
    // that define this object
    return this.writers.foldLeft(ourHeads, (heads, [, {writer}]) =>
      HashMap.ofIterable([...heads, ...writer.desiredHeadsForShareData()]),
    );
  }

  @MemoizeInstance
  desiredHeadsForSharedNode(): HashMap<StreamId, "open" | OpList<AppliedOp>> {
    // We don't need the shared-node heads for the writers, since they don't
    // influece this shared node. In fact, this is why we split the streams for
    // share data and shared node.
    return this.desiredHeadsHelper(
      this.closedWriterDevicesForSharedNode,
      "shared node",
    );
  }

  private desiredHeadsHelper(
    closedWriterDevices: HashMap<DeviceId, OpList<AppliedOp>>,
    type: "share data" | "shared node",
  ): HashMap<StreamId, "open" | OpList<AppliedOp>> {
    const ourOpenHeads = HashMap.ofIterable(
      this.openWriterDevices()
        .toArray()
        .map((deviceId) => [
          new StreamId({
            deviceId,
            shareId: this.shareId,
            type,
          }),
          "open" as "open" | OpList<AppliedOp>,
        ]),
    );
    const ourClosedHeads = closedWriterDevices.map((deviceId, finalOp) => [
      new StreamId({deviceId, shareId: this.shareId, type}),
      finalOp,
    ]);
    const ourHeads = ourOpenHeads.mergeWith(ourClosedHeads, (open, closed) => {
      throw new AssertFailed("One device should not be open and closed");
    });
    return ourHeads;
  }

  private openWriterDevices(): HashSet<DeviceId> {
    return this.writers.foldLeft(
      HashSet.of(this.shareId.creator),
      (devices, [, writerInfo]) =>
        HashSet.ofIterable([
          ...devices,
          ...writerInfo.writer.openWriterDevices(),
        ]),
    );
  }

  shareDataForShareId(shareId: ShareId): Option<ShareData> {
    if (shareId.equals(this.shareId)) return Option.of(this);
    return this.writers.foldLeft(
      Option.none<ShareData>(),
      (soFar, [key, info]) =>
        soFar.orCall(() => info.writer.shareDataForShareId(shareId)),
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
