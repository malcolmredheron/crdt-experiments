import {ObjectValue} from "./helper/ObjectValue";
import {TypedValue} from "./helper/TypedValue";
import {Timestamp} from "./helper/Timestamp";
import {ConsLinkedList, HashMap, HashSet, LinkedList, Option} from "prelude-ts";
import {
  concreteHeadsForAbstractHeads,
  headsEqual,
  undoHeadsOnce,
} from "./StreamHeads";

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
  streams: ConcreteHeads;
};

export function buildUpTree(
  universe: ConcreteHeads,
  until: Timestamp,
  nodeId: NodeId,
): UpTree {
  const tree = new UpTree({nodeId, heads: HashMap.of(), parents: HashMap.of()});
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
  const tree = new UpTree({nodeId, heads: HashMap.of(), parents: HashMap.of()});
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
    const parents1 = this1.parents.mapValues((edge) =>
      edge.copy({parent: build(edge.parent.nodeId, until)}),
    );
    return this1.copy({parents: parents1});
  }

  desiredHeads(): AbstractHeads {
    const ourHeads = HashMap.ofIterable<StreamId, "open" | OpList>(
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
    // We also need the heads for the parents, since they help to select the ops
    // that define this object
    // return this.parents.foldLeft(ourHeads, (heads, [, {parent}]) =>
    //   HashMap.ofIterable([...heads, ...parent.desiredHeads()]),
    // );
    return ourHeads;
  }

  private openWriterDevices(): HashSet<DeviceId> {
    // A node with no parents is writeable by the creator.
    if (this.parents.isEmpty()) return HashSet.of(this.nodeId.creator);
    return this.parents.foldLeft(HashSet.of(), (devices, [, edge]) =>
      HashSet.ofIterable([...devices, ...edge.parent.openWriterDevices()]),
    );
  }
}
