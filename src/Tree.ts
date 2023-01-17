import {ObjectValue} from "./helper/ObjectValue";
import {TypedValue} from "./helper/TypedValue";
import {Timestamp} from "./helper/Timestamp";
import {
  ConsLinkedList,
  HashMap,
  HashSet,
  LinkedList,
  Option,
  Vector,
} from "prelude-ts";
import {concreteHeadsForAbstractHeads} from "./StreamHeads";
import {
  consTail,
  mapMapOption,
  mapValuesStable,
  throwError,
} from "./helper/Collection";
import {match} from "ts-pattern";

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
type OpStream = ConsLinkedList<Op>;
export type OpList = OpStream;
export type AbstractHeads = HashMap<StreamId, "open" | OpStream>;
export type ConcreteHeads = HashMap<StreamId, OpStream>;
// Each item in the list is the same as the previous one except with a new op
// prepended.
type ForwardStream = ConsLinkedList<OpList>;

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
  next: () => Option<PersistentIterator<T>>;
}

interface PersistentIterator<T> {
  op: Op;
  result: InitialPersistentIterator<T>;
}

type UpTreeIteratorState = {
  readonly tree: UpTree;
  readonly forwardStreams: HashMap<StreamId, ForwardStream>;
  readonly parentIterators: HashMap<NodeId, InitialPersistentIterator<UpTree>>;
};

export function buildUpTree(
  universe: ConcreteHeads,
  nodeId: NodeId,
): InitialPersistentIterator<UpTree> {
  const tree = new UpTree({
    nodeId,
    heads: HashMap.of(),
    closedStreams: HashMap.of(),
    edges: HashMap.of(),
  });
  return {
    value: tree,
    next: () =>
      nextIterator(universe, {
        tree,
        forwardStreams: concreteHeadsForAbstractHeads(
          universe,
          tree.desiredHeads(),
        ).mapValues((stream) => forwardStreamForStream(stream)),
        parentIterators: HashMap.of(),
      }),
  };
}

function nextIterator(
  universe: ConcreteHeads,
  state: UpTreeIteratorState,
): Option<PersistentIterator<UpTree>> {
  const streamOps = state.forwardStreams
    .toVector()
    .map(([, forwardStream]) => forwardStream.head().get().head().get());
  const parentOps = state.parentIterators
    .toVector()
    .mapOption(([, parentIterator]) => parentIterator.next().map((i) => i.op));
  const opOption = Vector.ofIterable([...streamOps, ...parentOps]).reduce(
    (leftOp: Op, right: Op): Op =>
      leftOp.timestamp < right.timestamp
        ? leftOp
        : right.timestamp < leftOp.timestamp
        ? right
        : leftOp === right
        ? leftOp
        : throwError("non-identical ops have the same timestamp"),
  );
  if (opOption.isNone()) return Option.none();
  const op = opOption.get();

  const parentIterators1 = state.parentIterators.mapValues((i) =>
    i
      .next()
      .map((next) => (next.op === op ? next.result : i))
      .getOrElse(i),
  );
  const parentIterators2 = parentIterators1.containsKey(op.parentId)
    ? parentIterators1
    : parentIterators1.put(
        op.parentId,
        advanceIteratorUntil(buildUpTree(universe, op.parentId), op.timestamp),
      );
  const edges1 = mapValuesStable(state.tree.edges, (edge) =>
    parentIterators2
      .get(edge.parent.nodeId)
      .map((iterator) => edge.copy({parent: iterator.value}))
      .getOrElse(edge),
  );

  const forwardStreams1 = mapMapOption(
    state.forwardStreams,
    (streamId, forwardStream) => {
      return forwardStream.head().get().head().get() === op
        ? consTail(forwardStream)
        : Option.of(forwardStream);
    },
  );

  const opHeads: ConcreteHeads = mapMapOption(
    state.forwardStreams,
    (streamId, forwardStream) => {
      const stream = forwardStream.head().get();
      return stream.head().get() === op ? Option.of(stream) : Option.none();
    },
  );
  const tree1 = match({streamOps: !opHeads.isEmpty()})
    .with({streamOps: true}, () => {
      const edges2 = edges1.put(
        op.edgeId,
        new Edge({
          rank: op.rank,
          parent: parentIterators2.get(op.parentId).getOrThrow().value,
        }),
      );
      return state.tree.copy({
        edges: edges2,
        heads: state.tree.heads.mergeWith(opHeads, (v0, v1) => v1),
      });
    })
    .with({streamOps: false}, () => state.tree.copy({edges: edges1}))
    .exhaustive();
  return Option.of({
    op,
    result: {
      value: tree1,
      next: () =>
        nextIterator(universe, {
          tree: tree1,
          forwardStreams: forwardStreams1,
          parentIterators: parentIterators2,
        }),
    },
  });
}

// function advanceUpTree(
//   universe: ConcreteHeads,
//   tree: UpTree,
//   op: Op,
//   opHeads: ConcreteHeads,
// ): UpTree {
//   const parentIterator = advanceIteratorToAfter(
//     buildUpTree(universe, op.parentId),
//     op.timestamp,
//   );
//   const tree1 = tree.copy({
//     heads: opHeads.foldLeft(tree.heads, (heads, [streamId, opHead]) => {
//       if (!streamId.nodeId.equals(tree.nodeId) || streamId.type !== "up")
//         return heads;
//       return heads.put(streamId, opHead);
//     }),
//     edges: tree.edges.put(
//       op.edgeId,
//       new Edge({
//         parent: parentIterator.value,
//         rank: op.rank,
//       }),
//     ),
//   });
//
//   const addedWriterDeviceIds = tree1
//     .openWriterDevices()
//     .removeAll(tree.openWriterDevices());
//   const closedStreams1 = tree1.closedStreams.filterKeys((streamId) =>
//     addedWriterDeviceIds.contains(streamId.deviceId),
//   );
//
//   const removedWriterDeviceIds = tree
//     .openWriterDevices()
//     .removeAll(tree1.openWriterDevices());
//   const closedStreams2 = closedStreams1.mergeWith(
//     removedWriterDeviceIds.toVector().mapOption((removedWriterId) => {
//       const streamId = new StreamId({
//         deviceId: removedWriterId,
//         nodeId: tree.nodeId,
//         type: "up",
//       });
//       return op.contributingHeads.get(streamId).map((ops) => [streamId, ops]);
//     }),
//     (ops0, ops1) => throwError("Should not have stream-id collision"),
//   );
//   return tree1.copy({closedStreams: closedStreams2});
// }

// Advances the iterator until the next value is greater than `after`.
export function advanceIteratorUntil<T>(
  iterator: InitialPersistentIterator<T>,
  after: Timestamp,
): InitialPersistentIterator<T> {
  while (true) {
    const next = iterator.next();
    if (next.isNone()) return iterator;
    if (next.get().op.timestamp > after) return iterator;
    iterator = next.get().result;
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

  edges: HashMap<EdgeId, Edge>;
}>() {
  // update(
  //   build: (nodeId: NodeId, until: Timestamp) => UpTree,
  //   until: Timestamp,
  //   opInfo: Option<{op: SetEdge; opHeads: ConcreteHeads}>,
  // ): UpTree {
  //   const this1 = opInfo
  //     .map(({opHeads, op}) =>
  //       this.copy({
  //         heads: opHeads.foldLeft(this.heads, (heads, [streamId, opHead]) => {
  //           if (!streamId.nodeId.equals(this.nodeId) || streamId.type !== "up")
  //             return heads;
  //           return heads.put(streamId, opHead);
  //         }),
  //         edges: this.edges.put(
  //           op.edgeId,
  //           new Edge({parent: build(op.parentId, until), rank: op.rank}),
  //         ),
  //       }),
  //     )
  //     .getOrElse(this);
  //   const this2 = this1.copy({
  //     parents: this1.parents.mapValues((edge) =>
  //       edge.copy({parent: build(edge.parent.nodeId, until)}),
  //     ),
  //   });
  //   const this3 = opInfo
  //     .map(({opHeads, op}) => {
  //       const addedWriterDeviceIds = this2
  //         .openWriterDevices()
  //         .removeAll(this.openWriterDevices());
  //       const closedStreams1 = this2.closedStreams.filterKeys((streamId) =>
  //         addedWriterDeviceIds.contains(streamId.deviceId),
  //       );
  //
  //       const removedWriterDeviceIds = this.openWriterDevices().removeAll(
  //         this2.openWriterDevices(),
  //       );
  //       const closedStreams2 = closedStreams1.mergeWith(
  //         removedWriterDeviceIds.toVector().mapOption((removedWriterId) => {
  //           const streamId = new StreamId({
  //             deviceId: removedWriterId,
  //             nodeId: this.nodeId,
  //             type: "up",
  //           });
  //           return op.contributingHeads
  //             .get(streamId)
  //             .map((ops) => [streamId, ops]);
  //         }),
  //         (ops0, ops1) => throwError("Should not have stream-id collision"),
  //       );
  //       return this2.copy({closedStreams: closedStreams2});
  //     })
  //     .getOrElse(this2);
  //   return this3;
  // }

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
    if (this.edges.isEmpty()) return HashSet.of(this.nodeId.creator);
    return this.edges.foldLeft(HashSet.of(), (devices, [, edge]) =>
      HashSet.ofIterable([...devices, ...edge.parent.openWriterDevices()]),
    );
  }
}

function forwardStreamForStream(stream: OpStream): ForwardStream {
  return consTail(stream)
    .map((tail) => forwardStreamForStream(tail).prepend(stream))
    .getOrCall(() => LinkedList.of(stream));
}
