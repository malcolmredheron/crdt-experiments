import {ObjectValue} from "./helper/ObjectValue";
import {TypedValue} from "./helper/TypedValue";
import {Timestamp} from "./helper/Timestamp";
import {ConsLinkedList, HashMap, HashSet, LinkedList, Option} from "prelude-ts";
import {
  concreteHeadsForAbstractHeads,
  headsEqual,
  undoHeadsOnce,
} from "./StreamHeads";
import {throwError} from "./helper/Collection";

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
export type Op = SetEdge;
export type OpList = ConsLinkedList<Op>;
export type AbstractHeads = HashMap<StreamId, "open" | ConsLinkedList<Op>>;
export type ConcreteHeads = HashMap<StreamId, ConsLinkedList<Op>>;

export type SetEdge = {
  timestamp: Timestamp;
  type: "set edge";

  edgeId: EdgeId;
  childId: NodeId;

  parentId: NodeId;
  rank: Rank;

  // This indicates the final op that we'll accept for any streams that get
  // removed by this op.
  contributingHeads: ConcreteHeads;
};

export function buildUpTree(
  universe: ConcreteHeads,
  until: Timestamp,
  nodeId: NodeId,
): UpTree {
  const tree = new UpTree({
    nodeId,
    heads: HashMap.of(),
    closedStreams: HashMap.of(),
    parents: HashMap.of(),
  });
  const concreteHeads = concreteHeadsForAbstractHeads(
    universe,
    tree.desiredHeads(),
  );
  return buildUpTreeHelper(universe, concreteHeads, until, nodeId);
}

function buildUpTreeHelper(
  universe: ConcreteHeads,
  concreteHeads: ConcreteHeads,
  until: Timestamp,
  nodeId: NodeId,
): UpTree {
  const tree = new UpTree({
    nodeId,
    heads: HashMap.of(),
    closedStreams: HashMap.of(),
    parents: HashMap.of(),
  });
  let ops = LinkedList.of<{op: Op; opHeads: ConcreteHeads}>();
  let remainingHeads = concreteHeads;
  while (true) {
    const result = undoHeadsOnce(remainingHeads);
    if (result.isSome()) {
      const {op, opHeads} = result.get();
      if (op.timestamp <= until) ops = ops.prepend({op, opHeads});
      remainingHeads = result.get().remainingHeads;
    } else break;
  }

  const build = (nodeId: NodeId, until: Timestamp): UpTree =>
    buildUpTree(universe, until, nodeId);
  const tree1 = ops.foldLeft(tree, (tree, {op, opHeads}) =>
    tree.update(build, op.timestamp, Option.of({op, opHeads})),
  );
  // Give the tree a chance to update its children with anything that comes
  // after the last op that applies to the root node.
  const tree2 = tree1.update(build, until, Option.none());

  const concreteHeads1 = concreteHeadsForAbstractHeads(
    universe,
    tree2.desiredHeads(),
  );
  if (headsEqual(concreteHeads, concreteHeads1)) return tree2;
  else return buildUpTreeHelper(universe, concreteHeads1, until, nodeId);
}

export class Edge extends ObjectValue<{
  parent: UpTree;
  rank: Rank;
}>() {}

export class UpTree extends ObjectValue<{
  readonly nodeId: NodeId;
  heads: ConcreteHeads;
  closedStreams: ConcreteHeads;

  parents: HashMap<EdgeId, Edge>;
}>() {
  update(
    build: (nodeId: NodeId, until: Timestamp) => UpTree,
    until: Timestamp,
    opInfo: Option<{op: SetEdge; opHeads: ConcreteHeads}>,
  ): UpTree {
    const this1 = opInfo
      .map(({opHeads, op}) =>
        this.copy({
          heads: opHeads.foldLeft(this.heads, (heads, [streamId, opHead]) => {
            if (!streamId.nodeId.equals(this.nodeId) || streamId.type !== "up")
              return heads;
            return heads.put(streamId, opHead);
          }),
          parents: this.parents.put(
            op.edgeId,
            new Edge({parent: build(op.parentId, until), rank: op.rank}),
          ),
        }),
      )
      .getOrElse(this);
    const this2 = this1.copy({
      parents: this1.parents.mapValues((edge) =>
        edge.copy({parent: build(edge.parent.nodeId, until)}),
      ),
    });
    const this3 = opInfo
      .map(({opHeads, op}) => {
        const addedWriterDeviceIds = this2
          .openWriterDevices()
          .removeAll(this.openWriterDevices());
        const closedStreams1 = this2.closedStreams.filterKeys((streamId) =>
          addedWriterDeviceIds.contains(streamId.deviceId),
        );

        const removedWriterDeviceIds = this.openWriterDevices().removeAll(
          this2.openWriterDevices(),
        );
        const closedStreams2 = closedStreams1.mergeWith(
          removedWriterDeviceIds.toVector().mapOption((removedWriterId) => {
            const streamId = new StreamId({
              deviceId: removedWriterId,
              nodeId: this.nodeId,
              type: "up",
            });
            return op.contributingHeads
              .get(streamId)
              .map((ops) => [streamId, ops]);
          }),
          (ops0, ops1) => throwError("Should not have stream-id collision"),
        );
        return this2.copy({closedStreams: closedStreams2});
      })
      .getOrElse(this2);
    return this3;
  }

  desiredHeads(): AbstractHeads {
    const openStreams = HashMap.ofIterable<StreamId, "open" | OpList>(
      this.openWriterDevices()
        .toVector()
        .map((deviceId) => [
          new StreamId({
            nodeId: this.nodeId,
            deviceId: deviceId,
            type: "up",
          }),
          "open",
        ]),
    );
    const ourStreams = HashMap.ofIterable([
      ...openStreams,
      ...this.closedStreams,
    ]);
    // We also need the heads for the parents, since they help to select the ops
    // that define this object
    // return this.parents.foldLeft(openStreams, (heads, [, {parent}]) =>
    //   HashMap.ofIterable([...heads, ...parent.desiredHeads()]),
    // );
    return ourStreams;
  }

  private openWriterDevices(): HashSet<DeviceId> {
    // A node with no parents is writeable by the creator.
    if (this.parents.isEmpty()) return HashSet.of(this.nodeId.creator);
    return this.parents.foldLeft(HashSet.of(), (devices, [, edge]) =>
      HashSet.ofIterable([...devices, ...edge.parent.openWriterDevices()]),
    );
  }
}
