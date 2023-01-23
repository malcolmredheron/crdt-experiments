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
import {concreteHeadsForAbstractHeads, headsEqual} from "./StreamHeads";
import {
  asType,
  consTail,
  mapMapOption,
  mapValuesStable,
  throwError,
} from "./helper/Collection";
import {match} from "ts-pattern";
import {AssertFailed} from "./helper/Assert";

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
  needsReset: boolean;
  reset: () => InitialPersistentIterator<T>;
}

interface PersistentIterator<T> {
  op: Op;
  result: InitialPersistentIterator<T>;
}

type UpTreeIteratorState = {
  readonly tree: UpTree;
  // Unlike parentIterators, we map to PersistentIterators here because we
  // define a stream as containing an op, so we can't have a stream until after
  // the first op.
  readonly streamIterators: HashMap<StreamId, PersistentIterator<OpList>>;
  readonly parentIterators: HashMap<NodeId, InitialPersistentIterator<UpTree>>;
};

export function buildUpTree(
  universe: ConcreteHeads,
  nodeId: NodeId,
): InitialPersistentIterator<UpTree> {
  const streamIterators = concreteHeadsForAbstractHeads(
    universe,
    new UpTree({
      nodeId,
      heads: HashMap.of(),
      closedStreams: HashMap.of(),
      edges: HashMap.of(),
    }).desiredHeads(),
  ).mapValues((stream) => streamIteratorForStream(stream));
  return buildUpTreeInternal(universe, nodeId, streamIterators, HashMap.of());
}

function buildUpTreeInternal(
  universe: ConcreteHeads,
  nodeId: NodeId,
  streamIterators: HashMap<StreamId, PersistentIterator<OpList>>,
  parentIterators: HashMap<NodeId, InitialPersistentIterator<UpTree>>,
): InitialPersistentIterator<UpTree> {
  const tree = new UpTree({
    nodeId,
    heads: HashMap.of(),
    closedStreams: HashMap.of(),
    edges: HashMap.of(),
  });
  const state = asType<UpTreeIteratorState>({
    tree,
    streamIterators,
    parentIterators: parentIterators,
  });
  const iterator = {
    value: tree,
    next: () => nextIterator(universe, state),
    needsReset: false,
    reset: () => iterator,
    _state: state,
  };
  return iterator;
}

function nextIterator(
  universe: ConcreteHeads,
  state: UpTreeIteratorState,
): Option<PersistentIterator<UpTree>> {
  const streamOps = state.streamIterators
    .mapValues((streamIterator) => streamIterator.op)
    .valueIterable();
  const parentOps = mapMapOption(
    state.parentIterators,
    (streamId, parentIterator) => parentIterator.next().map((next) => next.op),
  ).valueIterable();
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
  const parentIterators2 =
    op.childId.equals(state.tree.nodeId) &&
    !parentIterators1.containsKey(op.parentId)
      ? parentIterators1.put(
          op.parentId,
          advanceIteratorUntil(
            buildUpTree(universe, op.parentId),
            op.timestamp,
          ),
        )
      : parentIterators1;
  const edges1 = mapValuesStable(state.tree.edges, (edge) =>
    parentIterators2
      .get(edge.parent.nodeId)
      .map((iterator) => edge.copy({parent: iterator.value}))
      .getOrElse(edge),
  );

  const streamIterators1 = mapMapOption(
    state.streamIterators,
    (streamId, streamIterator) => {
      return streamIterator.op === op
        ? streamIterator.result.next()
        : Option.of(streamIterator);
    },
  );

  const opHeads: ConcreteHeads = mapMapOption(
    state.streamIterators,
    (streamId, streamIterator) => {
      return streamIterator.op === op
        ? Option.of(streamIterator.result.value)
        : Option.none();
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

  const addedWriterDeviceIds = tree1
    .openWriterDevices()
    .removeAll(state.tree.openWriterDevices());
  const closedStreams1 = tree1.closedStreams.filterKeys((streamId) =>
    addedWriterDeviceIds.contains(streamId.deviceId),
  );
  const removedWriterDeviceIds = state.tree
    .openWriterDevices()
    .removeAll(tree1.openWriterDevices());
  const closedStreams2 = closedStreams1.mergeWith(
    removedWriterDeviceIds.toVector().mapOption((removedWriterId) => {
      const streamId = new StreamId({
        deviceId: removedWriterId,
        nodeId: state.tree.nodeId,
        type: "up",
      });
      return op.contributingHeads.get(streamId).map((ops) => [streamId, ops]);
    }),
    (ops0, ops1) => throwError("Should not have stream-id collision"),
  );
  const tree2 = tree1.copy({closedStreams: closedStreams2});

  const concreteHeads = concreteHeadsForAbstractHeads(
    universe,
    tree2.desiredHeads(),
  );
  const headsNeedReset = !headsEqual(concreteHeads, tree2.heads);
  const needsReset =
    headsNeedReset ||
    parentIterators2.anyMatch(
      (nodeId, parentIterator) => parentIterator.needsReset,
    );

  const state1 = {
    tree: tree2,
    streamIterators: streamIterators1,
    parentIterators: parentIterators2,
  };
  return Option.of({
    op,
    result: {
      value: tree2,
      next: () => nextIterator(universe, state1),
      needsReset,
      reset: () => {
        return buildUpTreeInternal(
          universe,
          tree2.nodeId,
          concreteHeads.mapValues((stream) => streamIteratorForStream(stream)),
          state1.parentIterators.mapValues((iterator) => iterator.reset()),
        );
      },
      _state: state,
      _state1: state1,
      _headsNeedReset: headsNeedReset,
      _concreteHeads: concreteHeads,
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
  // `iterators` and the limit of 10 times through the loop are for debugging.
  // We will have to find a more sophisticated way to handle this at some point.
  let iterators = LinkedList.of(iterator);
  for (let i = 0; i < 10; i++) {
    while (true) {
      const next = iterator.next();
      if (next.isNone()) break;
      if (next.get().op.timestamp > after) break;
      iterator = next.get().result;
    }
    if (!iterator.needsReset) return iterator;
    iterators = iterators.prepend(iterator);
    iterator = iterator.reset();
  }
  throw new AssertFailed("Iterator did not stabilize");
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
    return ourStreams;
  }

  public openWriterDevices(): HashSet<DeviceId> {
    // A node with no parents is writeable by the creator.
    if (this.edges.isEmpty()) return HashSet.of(this.nodeId.creator);
    return this.edges.foldLeft(HashSet.of(), (devices, [, edge]) =>
      HashSet.ofIterable([...devices, ...edge.parent.openWriterDevices()]),
    );
  }

  excludeFromEquals = HashSet.of("closedStreams");
  equals(other: unknown): boolean {
    if (Object.getPrototypeOf(this) !== Object.getPrototypeOf(other))
      return false;

    return (
      super.equals(other) &&
      headsEqual(this.closedStreams, (other as this).closedStreams)
    );
  }
}

function streamIteratorForStream(
  stream: OpStream,
  next: () => Option<PersistentIterator<OpStream>> = () => Option.none(),
): PersistentIterator<OpList> {
  const iterator: PersistentIterator<OpStream> = {
    op: stream.head().get(),
    result: {
      value: stream,
      next: next,
      needsReset: false,
      // Annoyingly, we don't have a way of resetting a stream iterator because
      // we'd need to return an InitialPersistentIterator, which we can't do
      // because the stream doesn't have a value until after the first op.
      reset: () => throwError("Not implemented"),
    },
  };
  return consTail(stream)
    .map((tail) => streamIteratorForStream(tail, () => Option.of(iterator)))
    .getOrElse(iterator);
}
