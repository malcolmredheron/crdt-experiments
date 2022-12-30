import {ObjectValue} from "./helper/ObjectValue";
import {TypedValue} from "./helper/TypedValue";
import {Timestamp} from "./helper/Timestamp";
import {ConsLinkedList, HashMap, HashSet, Option} from "prelude-ts";
import {concreteHeadsForAbstractHeads, undoHeadsOnce} from "./StreamHeads";

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
};

export function buildUpTree(
  universe: ConcreteHeads,
  until: Timestamp,
  nodeId: NodeId,
): UpTree {
  let lastTree = new UpTree({
    nodeId,
    parents: HashMap.of(),
  });
  let lastOpTimestamp = Timestamp.create(Number.MIN_SAFE_INTEGER);
  while (true) {
    const concreteHeads = concreteHeadsForAbstractHeads(
      universe,
      lastTree.desiredHeads(),
    );
    const op = nextOp(concreteHeads, lastOpTimestamp);
    if (op.isSome() && op.get().timestamp <= until) {
      const timestamp = op.get().timestamp;
      lastTree = lastTree.update(
        (nodeId) => buildUpTree(universe, timestamp, nodeId),
        timestamp,
        op,
      );
      lastOpTimestamp = timestamp;
    } else {
      // Give the tree a chance to update its children with anything that comes
      // after the last op that applies to the root node.
      return lastTree.update(
        (nodeId) => buildUpTree(universe, until, nodeId),
        until,
        Option.none(),
      );
    }
  }
}

function nextOp(concreteHeads: ConcreteHeads, after: Timestamp): Option<Op> {
  let next: Option<Op> = Option.none();
  let remainingHeads = concreteHeads;
  while (true) {
    const result = undoHeadsOnce(remainingHeads);
    if (result.isNone()) return next;
    const {op} = result.get();
    if (op.timestamp <= after) return next;
    next = Option.of(op);
    remainingHeads = result.get().remainingHeads;
  }
}

export class Edge extends ObjectValue<{
  parent: UpTree;
  rank: Rank;
}>() {}

export class UpTree extends ObjectValue<{
  readonly nodeId: NodeId;
  parents: HashMap<EdgeId, Edge>;
}>() {
  update(
    build: (nodeId: NodeId) => UpTree,
    until: Timestamp,
    op: Option<SetEdge>,
  ): UpTree {
    const this1 = this.copy({
      parents: this.parents.mapValues((edge) =>
        edge.copy({parent: build(edge.parent.nodeId)}),
      ),
    });
    const this2 = op
      .map((op) =>
        this1.copy({
          parents: this1.parents.put(
            op.edgeId,
            new Edge({parent: build(op.parentId), rank: op.rank}),
          ),
        }),
      )
      .getOrElse(this1);
    return this2;
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
    return openStreams;
  }

  private openWriterDevices(): HashSet<DeviceId> {
    // A node with no parents is writeable by the creator.
    if (this.parents.isEmpty()) return HashSet.of(this.nodeId.creator);
    return this.parents.foldLeft(HashSet.of(), (devices, [, edge]) =>
      HashSet.ofIterable([...devices, ...edge.parent.openWriterDevices()]),
    );
  }
}
