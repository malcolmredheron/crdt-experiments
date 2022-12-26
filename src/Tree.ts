import {ObjectValue} from "./helper/ObjectValue";
import {TypedValue} from "./helper/TypedValue";
import {Timestamp} from "./helper/Timestamp";
import {ConsLinkedList, HashMap, HashSet, Option, Vector} from "prelude-ts";
import {concreteHeadsForAbstractHeads, undoHeadsOnce} from "./StreamHeads";
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

interface InitialPersistentIterator<T> {
  value: T;
  next(): Option<PersistentIterator<T>>;
}

interface PersistentIterator<T> {
  value: T;
  op: Op;
  next(): Option<PersistentIterator<T>>;
}

export function buildUpTree(
  universe: ConcreteHeads,
  nodeId: NodeId,
): InitialPersistentIterator<UpTree> {
  const value = new UpTree({
    nodeId,
    heads: HashMap.of(),
    closedStreams: HashMap.of(),
    parents: HashMap.of(),
  });
  return {
    value,
    next: () =>
      nextIterator(
        universe,
        value,
        HashMap.of(),
        Timestamp.create(Number.MIN_SAFE_INTEGER),
      ),
  };
}

function nextIterator(
  universe: ConcreteHeads,
  tree: UpTree,
  parentIterators: HashMap<EdgeId, InitialPersistentIterator<UpTree>>,
  after: Timestamp,
): Option<PersistentIterator<UpTree>> {
  const concreteHeads = concreteHeadsForAbstractHeads(
    universe,
    tree.desiredHeads(),
  );
  const nextSelfOpInfo = nextOp(concreteHeads, after).map((info) => ({
    ...info,
    timestamp: info.op.timestamp,
    handle: (): {
      tree: UpTree;
      parentIterators: HashMap<EdgeId, InitialPersistentIterator<UpTree>>;
    } => advanceUpTree(universe, tree, parentIterators, info.op, info.opHeads),
  }));

  const nextParentOpInfos = parentIterators
    .toVector()
    .mapOption(([edgeId, iterator]) =>
      iterator.next().map(({op, value: parent, next}) => ({
        op,
        value: parent,
        timestamp: op.timestamp,
        handle: (): {
          tree: UpTree;
          parentIterators: HashMap<EdgeId, InitialPersistentIterator<UpTree>>;
        } => ({
          tree: tree.copy({
            parents: tree.parents.put(
              edgeId,
              tree.parents
                .get(edgeId)
                .map((edge) => edge.copy({parent}))
                .getOrCall(() => new Edge({parent, rank: op.rank})),
            ),
          }),
          parentIterators: parentIterators.put(edgeId, {value: parent, next}),
        }),
      })),
    );

  const nextOpInfoOption = Vector.ofIterable([
    ...nextParentOpInfos,
    ...nextSelfOpInfo.map((info) => [info]).getOrElse([]),
  ]).reduce((left, right) => (left.timestamp < right.timestamp ? left : right));

  if (nextOpInfoOption.isNone()) return Option.none();
  const nextOpInfo = nextOpInfoOption.get();
  const {tree: tree1, parentIterators: parentIterators1} = nextOpInfo.handle();
  return Option.some({
    value: tree1,
    op: nextOpInfo.op,
    next: () =>
      nextIterator(universe, tree1, parentIterators1, nextOpInfo.timestamp),
  });
}

function advanceUpTree(
  universe: ConcreteHeads,
  tree: UpTree,
  parentIterators: HashMap<EdgeId, InitialPersistentIterator<UpTree>>,
  op: Op,
  opHeads: ConcreteHeads,
): {
  tree: UpTree;
  parentIterators: HashMap<EdgeId, InitialPersistentIterator<UpTree>>;
} {
  const parentIterator = parentIterators
    .get(op.edgeId)
    .getOrCall(() =>
      advanceIteratorUntil(buildUpTree(universe, op.parentId), op.timestamp),
    );
  const tree1 = tree.copy({
    heads: opHeads.foldLeft(tree.heads, (heads, [streamId, opHead]) => {
      if (!streamId.nodeId.equals(tree.nodeId) || streamId.type !== "up")
        return heads;
      return heads.put(streamId, opHead);
    }),
    parents: tree.parents.put(
      op.edgeId,
      new Edge({
        parent: parentIterator.value,
        rank: op.rank,
      }),
    ),
  });
  const parentIterators1 = parentIterators.put(op.edgeId, parentIterator);
  return {tree: tree1, parentIterators: parentIterators1};
}

function nextOp(
  concreteHeads: ConcreteHeads,
  after: Timestamp,
): Option<{op: Op; opHeads: ConcreteHeads}> {
  let next: Option<{op: Op; opHeads: ConcreteHeads}> = Option.none();
  let remainingHeads = concreteHeads;
  while (true) {
    const result = undoHeadsOnce(remainingHeads);
    if (result.isNone()) return next;
    const {op, opHeads} = result.get();
    if (op.timestamp <= after) return next;
    next = Option.of({op, opHeads});
    remainingHeads = result.get().remainingHeads;
  }
}

export function advanceIteratorUntil<T>(
  iterator: InitialPersistentIterator<T>,
  until: Timestamp,
): InitialPersistentIterator<T> {
  while (true) {
    const next = iterator.next();
    if (next.isNone()) return iterator;
    if (next.get().op.timestamp > until) return iterator;
    iterator = next.get();
  }
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
