import {ControlledOpSet, OpList} from "./ControlledOpSet";
import {Timestamp} from "./helper/Timestamp";
import {TypedValue} from "./helper/TypedValue";
import {
  PersistentAppliedOp,
  persistentDoOpFactory,
  persistentUndoOp,
} from "./PersistentUndoHelper";
import {HashMap, HashSet, Option, Vector} from "prelude-ts";
import {ObjectValue} from "./helper/ObjectValue";
import {AssertFailed} from "./helper/Assert";
import {MemoizeInstance} from "./helper/Memoize";
import {asType, mapValuesStable} from "./helper/Collection";

// This really shouldn't be here, but ...
import {expectPreludeEqual} from "./helper/Shared.testing";
import {match, P} from "ts-pattern";

export type NestedPermissionedTree = ControlledOpSet<Tree, AppliedOp, StreamId>;

// Creates a new PermissionedTreeValue with shareId.creator as the initial
// writer.
export function createPermissionedTree(
  deviceId: DeviceId,
): NestedPermissionedTree {
  const rootNodeId = new NodeId({creator: deviceId, rest: undefined});
  return ControlledOpSet.create<Tree, AppliedOp, StreamId>(
    persistentDoOpFactory((value, op, streamIds) => {
      return value.doOp(op, streamIds);
    }),
    persistentUndoOp,
    (value) => value.desiredHeads(),
    new Tree({
      rootNodeId: rootNodeId,
      downRoots: HashMap.of([
        rootNodeId,
        new DownNode({
          upNode: new UpNode({
            nodeId: rootNodeId,
            parents: HashMap.of(),
            closedWriterDevicesForUpNode: HashMap.of(),
            closedWriterDevicesForDownNode: HashMap.of(),
          }),
          children: HashMap.of(),
        }),
      ]),
      upRoots: HashMap.of(),
    }),
  );
}

export class DeviceId extends TypedValue<"DeviceId", string> {}
export class StreamId extends ObjectValue<{
  deviceId: DeviceId;
  nodeId: NodeId;
  type: "up" | "down";
}>() {}
export class NodeId extends ObjectValue<{
  creator: DeviceId;
  rest: string | undefined;
}>() {}
export class EdgeId extends TypedValue<"EdgeId", string> {}
export class Rank extends TypedValue<"EdgeId", number> {}

// Operations
export type AppliedOp = PersistentAppliedOp<Tree, SetEdge>;

type SetEdge = {
  timestamp: Timestamp;
  type: "set edge";

  edgeId: EdgeId;
  childId: NodeId;

  parentId: NodeId;
  rank: Rank;

  // This indicates the final op that we'll accept for any streams that get
  // removed by this op.
  streams: HashMap<StreamId, OpList<AppliedOp>>;
};

type InternalOp = SetEdgeInternal;

type SetEdgeInternal = SetEdge & {
  parent: Option<UpNode>;
  child: Option<DownNode>;
};

type TreeProps = {
  readonly rootNodeId: NodeId;
  upRoots: HashMap<NodeId, UpNode>;
  downRoots: HashMap<NodeId, DownNode>;
};

class Tree extends ObjectValue<TreeProps>() {
  constructor(props: TreeProps) {
    super(props);
    this.assertRootsConsistent();
  }

  doOp(op: AppliedOp["op"], streamIds: HashSet<StreamId>): this {
    const up = streamIds.anyMatch(
      (streamId) =>
        streamId.type === "up" && streamId.nodeId.equals(op.childId),
    );
    const down = streamIds.anyMatch(
      (streamId) =>
        streamId.type === "down" && streamId.nodeId.equals(op.parentId),
    );

    const {upRoots: upRoots1, upParent} = match({up})
      .with({up: true}, () => {
        const {upRoots: upRoots1, node: upParent} =
          Tree.getOrCreateUpNodeForNodeId(
            this.upRoots,
            this.downRoots,
            op.parentId,
          );
        const {upRoots: upRoots2} = Tree.getOrCreateUpNodeForNodeId(
          upRoots1,
          this.downRoots,
          op.childId,
        );
        return {upRoots: upRoots2, upParent: Option.some(upParent)};
      })
      .with({up: false}, () => ({
        upRoots: this.upRoots,
        upParent: Option.none<UpNode>(),
      }))
      .exhaustive();

    if (
      upParent.isSome() &&
      upParent.get().upNodeForNodeId(op.childId).isSome()
    ) {
      // Avoid creating a cycle.
      return this;
    }

    const {downRoots: downRoots1, downChild} = match({down})
      .with({down: true}, () => {
        const {downRoots: downRoots1} = Tree.getOrCreateDownNodeForNodeId(
          upRoots1,
          this.downRoots,
          op.parentId,
        );
        const {downRoots: downRoots2, node: downChild} =
          Tree.getOrCreateDownNodeForNodeId(upRoots1, downRoots1, op.childId);
        return {downRoots: downRoots2, downChild: Option.some(downChild)};
      })
      .with({down: false}, () => ({
        downRoots: this.downRoots,
        downChild: Option.none<DownNode>(),
      }))
      .exhaustive();

    const internalOp = asType<InternalOp>({
      parent: upParent,
      child: downChild,
      ...op,
    });
    const upRoots2 = mapValuesStable(upRoots1, (root) => root.doOp(internalOp));
    const downRoots2 = mapValuesStable(downRoots1, (root) =>
      root.doOp(internalOp),
    );
    if (upRoots2 === this.upRoots && downRoots2 === this.downRoots) return this;

    // If the down child was removed, add it back as a root so that we don't
    // lose the streams that went into it.
    const upRoots3 = upRoots2; // TODO: do the same for the up parent?
    const downRoots3 = match({
      originalDownChild: Tree.downNodeForNodeId(this.downRoots, op.childId),
      finalDownChild: Tree.downNodeForNodeId(downRoots2, op.childId),
    })
      .with(
        {},
        ({originalDownChild, finalDownChild}) =>
          originalDownChild.isSome() && finalDownChild.isNone(),
        ({originalDownChild}) =>
          downRoots2.put(
            op.childId,
            originalDownChild.getOrThrow().doOp(internalOp),
          ),
      )
      .with(P._, () => downRoots2)
      .exhaustive();

    // Remove anything that used to be a root but is now included elsewhere in
    // the tree.
    const upRoots4 = Vector.of(op.parentId, op.childId).foldLeft(
      upRoots3,
      (upRoots, nodeId) => Tree.cleanedUpRoots(upRoots, downRoots3, nodeId),
    );
    const downRoots4 = Vector.of(op.parentId, op.childId).foldLeft(
      downRoots3,
      (downRoots, nodeId) => Tree.cleanedDownRoots(downRoots, nodeId),
    );

    return this.copy({
      upRoots: upRoots4,
      downRoots: downRoots4,
    });
  }

  assertRootsConsistent(): void {
    // All copies of the same object should be equal
    // All roots should be necessary, ie nowhere else in the tree

    let roots = HashSet.of<DownNode | UpNode>();
    let upNodes = HashMap.of<NodeId, UpNode>();
    let downNodes = HashMap.of<NodeId, DownNode>();
    const traverseUpNode = (node: UpNode, root: boolean): void => {
      const existingNode = upNodes.get(node.nodeId);
      if (roots.contains(node) || (existingNode.isSome() && root))
        throw new AssertFailed("Found our way to a root again");
      if (root) roots = roots.add(node);
      if (existingNode.isSome()) {
        expectPreludeEqual(node, existingNode.get());
      } else {
        upNodes = upNodes.put(node.nodeId, node);
        for (const edge of node.parents.valueIterable()) {
          traverseUpNode(edge.parent, false);
        }
      }
    };
    const traverseDownNode = (node: DownNode, root: boolean): void => {
      const existingNode = downNodes.get(node.upNode.nodeId);
      if (roots.contains(node) || (existingNode.isSome() && root))
        throw new AssertFailed("Found our way to a root again");
      if (root) roots = roots.add(node);
      if (existingNode.isSome()) {
        expectPreludeEqual(node, existingNode.get());
      } else {
        downNodes = downNodes.put(node.upNode.nodeId, node);
        traverseUpNode(node.upNode, false);
        for (const child of node.children.valueIterable()) {
          traverseDownNode(child, false);
        }
      }
    };
    for (const node of this.downRoots.valueIterable()) {
      traverseDownNode(node, true);
    }
    for (const node of this.upRoots.valueIterable()) {
      traverseUpNode(node, true);
    }
  }

  @MemoizeInstance
  desiredHeads(): HashMap<StreamId, "open" | OpList<AppliedOp>> {
    // We need to report desired heads based on all of the roots, otherwise we
    // won't have the ops to reconstruct subtrees that used to be in the main
    // tree but aren't now.
    const nodeHeads = this.downRoots.foldLeft(
      HashMap.of<StreamId, "open" | OpList<AppliedOp>>(),
      (result, [, node]) =>
        HashMap.ofIterable([...result, ...node.desiredHeads()]),
    );
    return this.upRoots.foldLeft(nodeHeads, (result, [, upRoot]) =>
      HashMap.ofIterable([...result, ...upRoot.desiredHeadsForUpNode()]),
    );
  }

  root(): DownNode {
    return Tree.downNodeForNodeId(this.downRoots, this.rootNodeId).getOrThrow();
  }

  upNodeForNodeId(nodeId: NodeId): Option<UpNode> {
    return Tree.upNodeForNodeId(this.upRoots, this.downRoots, nodeId);
  }

  static upNodeForNodeId(
    upRoots: HashMap<NodeId, UpNode>,
    downRoots: HashMap<NodeId, DownNode>,
    nodeId: NodeId,
  ): Option<UpNode> {
    const nodesResult = downRoots.foldLeft(
      Option.none<UpNode>(),
      (soFar, [key, root]) => soFar.orCall(() => root.upNodeForNodeId(nodeId)),
    );
    if (nodesResult.isSome()) return nodesResult;
    return upRoots.foldLeft(Option.none<UpNode>(), (soFar, [key, root]) =>
      soFar.orCall(() => root.upNodeForNodeId(nodeId)),
    );
  }

  static getOrCreateUpNodeForNodeId(
    upRoots: HashMap<NodeId, UpNode>,
    downRoots: HashMap<NodeId, DownNode>,
    nodeId: NodeId,
  ): {node: UpNode; upRoots: HashMap<NodeId, UpNode>} {
    const existing = this.upNodeForNodeId(upRoots, downRoots, nodeId);
    if (existing.isSome()) return {node: existing.get(), upRoots};
    const node = new UpNode({
      nodeId,
      parents: HashMap.of(),
      closedWriterDevicesForUpNode: HashMap.of(),
      closedWriterDevicesForDownNode: HashMap.of(),
    });
    return {node, upRoots: upRoots.put(nodeId, node)};
  }

  static cleanedUpRoots(
    upRoots: HashMap<NodeId, UpNode>,
    downRoots: HashMap<NodeId, DownNode>,
    nodeId: NodeId,
  ): HashMap<NodeId, UpNode> {
    const upRootsWithoutNode = upRoots.remove(nodeId);
    if (this.upNodeForNodeId(upRootsWithoutNode, downRoots, nodeId).isSome())
      return upRootsWithoutNode;
    return upRoots;
  }

  downNodeForNodeId(nodeId: NodeId): Option<DownNode> {
    return Tree.downNodeForNodeId(this.downRoots, nodeId);
  }

  static downNodeForNodeId(
    downRoots: HashMap<NodeId, DownNode>,
    nodeId: NodeId,
  ): Option<DownNode> {
    return downRoots.foldLeft(Option.none<DownNode>(), (soFar, [key, root]) =>
      soFar.orCall(() => root.downNodeForNodeId(nodeId)),
    );
  }

  static getOrCreateDownNodeForNodeId(
    upRoots: HashMap<NodeId, UpNode>,
    downRoots: HashMap<NodeId, DownNode>,
    nodeId: NodeId,
  ): {node: DownNode; downRoots: HashMap<NodeId, DownNode>} {
    const existing = this.downNodeForNodeId(downRoots, nodeId);
    if (existing.isSome()) return {node: existing.get(), downRoots};
    const node = this.createDownNode(upRoots, downRoots, nodeId);
    return {node, downRoots: downRoots.put(nodeId, node)};
  }

  static createDownNode(
    upRoots: HashMap<NodeId, UpNode>,
    downRoots: HashMap<NodeId, DownNode>,
    nodeId: NodeId,
  ): DownNode {
    return new DownNode({
      children: HashMap.of(),
      upNode: this.upNodeForNodeId(upRoots, downRoots, nodeId).getOrCall(
        () =>
          new UpNode({
            nodeId,
            parents: HashMap.of(),
            closedWriterDevicesForUpNode: HashMap.of(),
            closedWriterDevicesForDownNode: HashMap.of(),
          }),
      ),
    });
  }

  static cleanedDownRoots(
    downRoots: HashMap<NodeId, DownNode>,
    nodeId: NodeId,
  ): HashMap<NodeId, DownNode> {
    const downRootsWithoutNode = downRoots.remove(nodeId);
    if (this.downNodeForNodeId(downRootsWithoutNode, nodeId).isSome())
      return downRootsWithoutNode;
    return downRoots;
  }
}

export class Edge extends ObjectValue<{
  parent: UpNode;
  rank: Rank;
}>() {}

export class UpNode extends ObjectValue<{
  readonly nodeId: NodeId;
  parents: HashMap<EdgeId, Edge>;
  closedWriterDevicesForUpNode: HashMap<DeviceId, OpList<AppliedOp>>;
  closedWriterDevicesForDownNode: HashMap<DeviceId, OpList<AppliedOp>>;
}>() {
  static create(nodeId: NodeId): UpNode {
    return new UpNode({
      nodeId,
      parents: HashMap.of(),
      closedWriterDevicesForUpNode: HashMap.of(),
      closedWriterDevicesForDownNode: HashMap.of(),
    });
  }

  doOp(op: SetEdgeInternal): this {
    if (op.parent.isNone()) return this;
    const parents1 = mapValuesStable(this.parents, (edge) => {
      const parent1 = edge.parent.doOp(op);
      if (parent1 === edge.parent) return edge;
      return edge.copy({parent: parent1});
    });
    if (this.nodeId.equals(op.childId)) {
      const this1 = this.copy({
        parents: parents1.put(
          op.edgeId,
          new Edge({parent: op.parent.get(), rank: op.rank}),
        ),
      });

      const newlyAddedWriterDevices = this1
        .openWriterDevices()
        .removeAll(this.openWriterDevices());
      // Anything that's newly added should no longer be listed as a closed
      // writer.
      const this2 = this1.copy({
        closedWriterDevicesForUpNode:
          this1.closedWriterDevicesForUpNode.filterKeys(
            (deviceId) => !newlyAddedWriterDevices.contains(deviceId),
          ),
        closedWriterDevicesForDownNode:
          this1.closedWriterDevicesForDownNode.filterKeys(
            (deviceId) => !newlyAddedWriterDevices.contains(deviceId),
          ),
      });

      const newlyClosedWriterDevices = this.openWriterDevices()
        .removeAll(this1.openWriterDevices())
        .toVector();
      const closedWriterDevices = (
        current: HashMap<DeviceId, OpList<AppliedOp>>,
        type: "up" | "down",
      ): HashMap<DeviceId, OpList<AppliedOp>> => {
        return current.mergeWith(
          HashMap.ofIterable(
            newlyClosedWriterDevices.mapOption((removedDeviceId) =>
              op.streams
                .get(
                  new StreamId({
                    deviceId: removedDeviceId,
                    nodeId: this.nodeId,
                    type,
                  }),
                )
                .map((finalOp) => [removedDeviceId, finalOp]),
            ),
          ),
          (oldOp, newOp) => newOp,
        );
      };
      const this3 = this2.copy({
        closedWriterDevicesForUpNode: closedWriterDevices(
          this1.closedWriterDevicesForUpNode,
          "up",
        ),
        closedWriterDevicesForDownNode: closedWriterDevices(
          this1.closedWriterDevicesForDownNode,
          "down",
        ),
      });

      return this3;
    }

    return this.copy({parents: parents1});
  }

  @MemoizeInstance
  desiredHeadsForUpNode(): HashMap<StreamId, "open" | OpList<AppliedOp>> {
    const ourHeads = this.desiredHeadsHelper(
      this.closedWriterDevicesForUpNode,
      "up",
    );
    // We also need the heads for the parents, since they help to select the ops
    // that define this object
    return this.parents.foldLeft(ourHeads, (heads, [, {parent}]) =>
      HashMap.ofIterable([...heads, ...parent.desiredHeadsForUpNode()]),
    );
  }

  @MemoizeInstance
  desiredHeadsForDownNode(): HashMap<StreamId, "open" | OpList<AppliedOp>> {
    // We don't need the down streams for the parents, since they don't influece
    // this up node. In fact, this is why we split the streams.
    return this.desiredHeadsHelper(this.closedWriterDevicesForDownNode, "down");
  }

  private desiredHeadsHelper(
    closedWriterDevices: HashMap<DeviceId, OpList<AppliedOp>>,
    type: "up" | "down",
  ): HashMap<StreamId, "open" | OpList<AppliedOp>> {
    const ourOpenHeads = HashMap.ofIterable(
      this.openWriterDevices()
        .toArray()
        .map((deviceId) => [
          new StreamId({
            deviceId,
            nodeId: this.nodeId,
            type,
          }),
          "open" as "open" | OpList<AppliedOp>,
        ]),
    );
    const ourClosedHeads = closedWriterDevices.map((deviceId, finalOp) => [
      new StreamId({deviceId, nodeId: this.nodeId, type}),
      finalOp,
    ]);
    const ourHeads = ourOpenHeads.mergeWith(ourClosedHeads, (open, closed) => {
      throw new AssertFailed("One device should not be open and closed");
    });
    return ourHeads;
  }

  private openWriterDevices(): HashSet<DeviceId> {
    return this.parents.foldLeft(
      HashSet.of(this.nodeId.creator),
      (devices, [, edge]) =>
        HashSet.ofIterable([...devices, ...edge.parent.openWriterDevices()]),
    );
  }

  upNodeForNodeId(nodeId: NodeId): Option<UpNode> {
    if (nodeId.equals(this.nodeId)) return Option.of(this);
    return this.parents.foldLeft(Option.none<UpNode>(), (soFar, [key, edge]) =>
      soFar.orCall(() => edge.parent.upNodeForNodeId(nodeId)),
    );
  }
}

export class DownNode extends ObjectValue<{
  upNode: UpNode;
  // TODO: the key should be node id + edge id
  children: HashMap<NodeId, DownNode>;
}>() {
  doOp(op: InternalOp): this {
    const upNode1 = this.upNode.doOp(op);

    const childShouldBeOurs =
      op.child.isSome() &&
      op.parentId.equals(this.upNode.nodeId) &&
      op.parent.isSome();
    const childIsOurs = this.children.get(op.childId).isSome();
    const children1 = match({
      op,
      children: this.children,
      childIsOurs,
      childShouldBeOurs,
      id: this.upNode.nodeId,
    })
      .with(
        {childIsOurs: false, childShouldBeOurs: true},
        ({op, id, children}) => {
          return children.put(op.childId, op.child.getOrThrow());
        },
      )
      .with({childIsOurs: true, childShouldBeOurs: false}, ({op, children}) =>
        children.remove(op.childId),
      )
      .otherwise(({children}) => children);
    const children2 = mapValuesStable(children1, (child) => child.doOp(op));

    if (upNode1 == this.upNode && children1 === this.children) return this;
    return this.copy({upNode: upNode1, children: children2});
  }

  @MemoizeInstance
  desiredHeads(): HashMap<StreamId, "open" | OpList<AppliedOp>> {
    const ourDesiredHeads = this.upNode
      .desiredHeadsForUpNode()
      .mergeWith(this.upNode.desiredHeadsForDownNode(), (left, right) => {
        throw new AssertFailed("Nothing should be in both maps");
      });
    return this.children.foldLeft(ourDesiredHeads, (result, [, child]) =>
      HashMap.ofIterable([...result, ...child.desiredHeads()]),
    );
  }

  downNodeForNodeId(nodeId: NodeId): Option<DownNode> {
    if (nodeId.equals(this.upNode.nodeId)) return Option.of(this);
    return this.children.foldLeft(
      Option.none<DownNode>(),
      (soFar, [key, child]) =>
        soFar.orCall(() => child.downNodeForNodeId(nodeId)),
    );
  }

  upNodeForNodeId(nodeId: NodeId): Option<UpNode> {
    const upNodeResult = this.upNode.upNodeForNodeId(nodeId);
    if (upNodeResult.isSome()) return upNodeResult;
    return this.children.foldLeft(
      Option.none<UpNode>(),
      (soFar, [key, child]) =>
        soFar.orCall(() => child.upNodeForNodeId(nodeId)),
    );
  }
}
