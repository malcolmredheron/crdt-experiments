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
  const rootNodeKey = new NodeKey({nodeId: rootNodeId, type: "down"});
  return ControlledOpSet.create<Tree, AppliedOp, StreamId>(
    persistentDoOpFactory((value, op, streamIds) => {
      return value.doOp(op, streamIds);
    }),
    persistentUndoOp,
    (value) => value.desiredHeads(),
    new Tree({
      rootNodeKey,
      roots: HashMap.of([
        rootNodeKey,
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
    }),
  );
}

export class DeviceId extends TypedValue<"DeviceId", string> {}
type NodeType = "up" | "down";
export class StreamId extends ObjectValue<{
  deviceId: DeviceId;
  nodeId: NodeId;
  type: NodeType;
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
  readonly rootNodeKey: NodeKey;
  roots: HashMap<NodeKey, Node>;
};

class Tree extends ObjectValue<TreeProps>() {
  constructor(props: TreeProps) {
    super(props);
    this.assertRootsConsistent();
  }

  doOp(op: AppliedOp["op"], streamIds: HashSet<StreamId>): this {
    const upParentKey = new NodeKey({nodeId: op.parentId, type: "up"});
    const upChildKey = new NodeKey({nodeId: op.childId, type: "up"});
    const downParentKey = new NodeKey({nodeId: op.parentId, type: "down"});
    const downChildKey = new NodeKey({nodeId: op.childId, type: "down"});

    const up = streamIds.anyMatch(
      (streamId) =>
        streamId.type === "up" && streamId.nodeId.equals(op.childId),
    );
    const down = streamIds.anyMatch(
      (streamId) =>
        streamId.type === "down" && streamId.nodeId.equals(op.parentId),
    );

    const roots1 = Vector.of(
      ...(up ? [upParentKey, upChildKey] : []),
      ...(down ? [downParentKey, downChildKey] : []),
    ).foldLeft(
      this.roots,
      (roots, key) => Tree.getOrCreateNodeForNodeKey(roots, key).roots,
    );

    // Avoid creating a cycle.
    const upParent = up
      ? Tree.nodeForNodeKey(roots1, upParentKey)
      : Option.none<Node>();
    const downChild = down
      ? Tree.nodeForNodeKey(roots1, downChildKey)
      : Option.none<Node>();
    if (
      upParent.isSome() &&
      upParent.get().nodeForNodeKey(upChildKey).isSome()
    ) {
      return this;
    }

    // Apply the op.
    const internalOp = asType<InternalOp>({
      parent: upParent as Option<UpNode>,
      child: downChild as Option<DownNode>,
      ...op,
    });
    const roots2 = mapValuesStable(roots1, (root) => root.doOp(internalOp));
    if (roots2 === this.roots) return this;

    // If the down child or the original up parent was removed, add it back as
    // a root so that we don't lose the streams that went into it.
    const roots3 = Vector.of(
      ...[downChildKey],
      ...Tree.nodeForNodeKey(this.roots, upChildKey)
        .flatMap((node) => (node as UpNode).parents.get(op.edgeId))
        .map((edge) => [edge.parent.nodeKey()])
        .getOrElse([]),
    ).foldLeft(roots2, (roots, key) =>
      match({
        original: Tree.nodeForNodeKey(this.roots, key),
        final: Tree.nodeForNodeKey(roots, key),
      })
        .with(
          {},
          ({original, final}) => original.isSome() && final.isNone(),
          ({original}) =>
            roots.put(key, original.getOrThrow().doOp(internalOp)),
        )
        .with(P._, () => roots)
        .exhaustive(),
    );

    // Remove anything that used to be a root but is now included elsewhere in
    // the tree.
    const roots4 = Vector.of(
      upParentKey,
      upChildKey,
      downParentKey,
      downChildKey,
    ).foldLeft(roots3, (roots, nodeKey) => Tree.cleanedRoots(roots, nodeKey));

    return this.copy({roots: roots4});
  }

  assertRootsConsistent(): void {
    // All copies of the same object should be equal
    // All roots should be necessary, ie nowhere else in the tree

    let roots = HashSet.of<Node>();
    let nodes = HashMap.of<NodeKey, Node>();
    const traverseNode = (node: Node, root: boolean): void => {
      const existingNode = nodes.get(node.nodeKey());
      if (roots.contains(node) || (existingNode.isSome() && root))
        throw new AssertFailed("Found our way to a root again");
      if (root) roots = roots.add(node);
      if (existingNode.isSome()) {
        expectPreludeEqual(node, existingNode.get());
      } else {
        nodes = nodes.put(node.nodeKey(), node);
        if (node instanceof UpNode) {
          for (const edge of node.parents.valueIterable()) {
            traverseNode(edge.parent, false);
          }
        } else {
          traverseNode(node.upNode, false);
          for (const child of node.children.valueIterable()) {
            traverseNode(child, false);
          }
        }
      }
    };
    for (const node of this.roots.valueIterable()) {
      traverseNode(node, true);
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

  root(): DownNode {
    return (
      Tree.nodeForNodeKey(this.roots, this.rootNodeKey) as Option<DownNode>
    ).getOrThrow();
  }

  nodeForNodeKey(nodeKey: NodeKey): Option<Node> {
    return Tree.nodeForNodeKey(this.roots, nodeKey);
  }

  static nodeForNodeKey(
    roots: HashMap<NodeKey, Node>,
    nodeKey: NodeKey,
  ): Option<Node> {
    return roots.foldLeft(Option.none<Node>(), (soFar, [key, root]) =>
      soFar.orCall(() => root.nodeForNodeKey(nodeKey)),
    );
  }

  static getOrCreateNodeForNodeKey(
    roots: HashMap<NodeKey, Node>,
    nodeKey: NodeKey,
  ): {node: Node; roots: HashMap<NodeKey, Node>} {
    const existing = this.nodeForNodeKey(roots, nodeKey);
    if (existing.isSome()) return {node: existing.get(), roots};
    if (nodeKey.type === "up") {
      const node = new UpNode({
        nodeId: nodeKey.nodeId,
        parents: HashMap.of(),
        closedWriterDevicesForUpNode: HashMap.of(),
        closedWriterDevicesForDownNode: HashMap.of(),
      });
      return {node, roots: roots.put(nodeKey, node)};
    } else {
      // We can ignore the updated roots from this because we are linking the
      // up node into the down node.
      const {node: upNode} = this.getOrCreateNodeForNodeKey(
        roots,
        nodeKey.copy({type: "up"}),
      );
      const node = new DownNode({
        upNode: upNode as UpNode,
        children: HashMap.of(),
      });
      return {node, roots: roots.put(nodeKey, node)};
    }
  }

  static cleanedRoots(
    roots: HashMap<NodeKey, Node>,
    nodeKey: NodeKey,
  ): HashMap<NodeKey, Node> {
    const rootsWithoutNode = roots.remove(nodeKey);
    if (this.nodeForNodeKey(rootsWithoutNode, nodeKey).isSome())
      return rootsWithoutNode;
    return roots;
  }
}

type Node = UpNode | DownNode;

export class NodeKey extends ObjectValue<{
  nodeId: NodeId;
  type: NodeType;
}>() {}

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

  nodeKey(): NodeKey {
    return new NodeKey({nodeId: this.nodeId, type: "up"});
  }

  @MemoizeInstance
  desiredHeads(): HashMap<StreamId, "open" | OpList<AppliedOp>> {
    const ourHeads = this.desiredHeadsHelper(
      this.closedWriterDevicesForUpNode,
      "up",
    );
    // We also need the heads for the parents, since they help to select the ops
    // that define this object
    return this.parents.foldLeft(ourHeads, (heads, [, {parent}]) =>
      HashMap.ofIterable([...heads, ...parent.desiredHeads()]),
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
    // A node with no parents is writeable by the creator.
    if (this.parents.isEmpty()) return HashSet.of(this.nodeId.creator);
    return this.parents.foldLeft(HashSet.of(), (devices, [, edge]) =>
      HashSet.ofIterable([...devices, ...edge.parent.openWriterDevices()]),
    );
  }

  nodeForNodeKey(nodeKey: NodeKey): Option<Node> {
    if (nodeKey.nodeId.equals(this.nodeId) && nodeKey.type === "up")
      return Option.of(this);
    return this.parents.foldLeft(Option.none(), (soFar, [key, edge]) =>
      soFar.orCall(() => edge.parent.nodeForNodeKey(nodeKey)),
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

  nodeKey(): NodeKey {
    return new NodeKey({nodeId: this.upNode.nodeId, type: "down"});
  }

  @MemoizeInstance
  desiredHeads(): HashMap<StreamId, "open" | OpList<AppliedOp>> {
    const ourDesiredHeads = this.upNode
      .desiredHeads()
      .mergeWith(this.upNode.desiredHeadsForDownNode(), (left, right) => {
        throw new AssertFailed("Nothing should be in both maps");
      });
    return this.children.foldLeft(ourDesiredHeads, (result, [, child]) =>
      HashMap.ofIterable([...result, ...child.desiredHeads()]),
    );
  }

  nodeForNodeKey(nodeKey: NodeKey): Option<Node> {
    if (nodeKey.nodeId.equals(this.upNode.nodeId) && nodeKey.type === "down")
      return Option.of(this);
    const upNodeResult = this.upNode.nodeForNodeKey(nodeKey);
    if (upNodeResult.isSome()) return upNodeResult;
    return this.children.foldLeft(Option.none(), (soFar, [key, child]) =>
      soFar.orCall(() => child.nodeForNodeKey(nodeKey)),
    );
  }
}
