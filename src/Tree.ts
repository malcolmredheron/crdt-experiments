import {ObjectValue} from "./helper/ObjectValue";
import {TypedValue} from "./helper/TypedValue";
import {Timestamp} from "./helper/Timestamp";
import {HashMap, HashSet, LinkedList, Option, Vector} from "prelude-ts";
import {concreteHeadsForAbstractHeads, headsEqual} from "./StreamHeads";
import {
  asType,
  mapMapOption,
  mapValuesStable,
  throwError,
} from "./helper/Collection";
import {match, P} from "ts-pattern";
import {AssertFailed} from "./helper/Assert";
import {Seq} from "prelude-ts/dist/src/Seq";

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
export type OpStream = LinkedList<Op>;
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

type PermGroupIteratorState = {
  readonly tree: PermGroup;
  readonly streamIterators: HashMap<
    StreamId,
    PersistentIteratorValue<OpStream, Op>
  >;
  readonly parentIterators: HashMap<
    NodeId,
    PersistentIteratorValue<PermGroup, Op>
  >;
};

export function buildPermGroup(
  universe: ConcreteHeads,
  nodeId: NodeId,
): PersistentIteratorValue<PermGroup, Op> {
  const streamIterators = concreteHeadsForAbstractHeads(
    universe,
    new PermGroup({
      nodeId,
      heads: HashMap.of(),
      closedStreams: HashMap.of(),
      edges: HashMap.of(),
    }).desiredHeads(),
  ).mapValues((stream) => streamIteratorForStream(stream));
  return buildPermGroupInternal(
    universe,
    nodeId,
    streamIterators,
    HashMap.of(),
  );
}

function buildPermGroupInternal(
  universe: ConcreteHeads,
  nodeId: NodeId,
  streamIterators: HashMap<StreamId, PersistentIteratorValue<OpStream, Op>>,
  parentIterators: HashMap<NodeId, PersistentIteratorValue<PermGroup, Op>>,
): PersistentIteratorValue<PermGroup, Op> {
  const tree = new PermGroup({
    nodeId,
    heads: HashMap.of(),
    closedStreams: HashMap.of(),
    edges: HashMap.of(),
  });
  const state = asType<PermGroupIteratorState>({
    tree,
    streamIterators,
    parentIterators: parentIterators,
  });
  const iterator = {
    value: tree,
    next: () => nextPermGroupIterator(universe, state),
    needsReset: false,
    reset: () => iterator,
    _state: state,
  };
  return iterator;
}

function nextPermGroupIterator(
  universe: ConcreteHeads,
  state: PermGroupIteratorState,
): Option<PersistentIteratorOp<PermGroup, Op>> {
  const opOption = nextOp(
    Vector.of<PersistentIteratorValue<unknown, Op>>(
      ...state.streamIterators.valueIterable(),
      ...state.parentIterators.valueIterable(),
    ),
  );
  if (opOption.isNone()) return Option.none();
  const op = opOption.get();

  const parentIterators1 = state.parentIterators.mapValues((i) =>
    i
      .next()
      .map((next) => (next.op === op ? next.value : i))
      .getOrElse(i),
  );
  const parentIterators2 =
    op.childId.equals(state.tree.nodeId) &&
    !parentIterators1.containsKey(op.parentId)
      ? parentIterators1.put(
          op.parentId,
          advanceIteratorUntil(
            buildPermGroup(universe, op.parentId),
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

  const streamIterators1 = state.streamIterators.mapValues((streamIterator) => {
    return streamIterator
      .next()
      .map((next) => (next.op === op ? next.value : streamIterator))
      .getOrElse(streamIterator);
  });

  const opHeads: HashMap<StreamId, OpStream> = mapMapOption(
    state.streamIterators,
    (streamId, streamIterator) => {
      return streamIterator
        .next()
        .flatMap((next) =>
          next.op === op
            ? Option.of(next.value.value)
            : Option.none<OpStream>(),
        )
        .orElse(Option.none<OpStream>());
    },
  );
  const tree1 = match({opHeads})
    .with(
      {},
      ({opHeads}) => !opHeads.isEmpty(),
      ({opHeads}) =>
        state.tree.copy({
          edges: edges1.put(
            op.edgeId,
            new Edge({
              rank: op.rank,
              parent: parentIterators2.get(op.parentId).getOrThrow().value,
            }),
          ),
          heads: state.tree.heads.mergeWith(opHeads, (v0, v1) => v1),
        }),
    )
    .with(P._, () => state.tree.copy({edges: edges1}))
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
    value: {
      value: tree2,
      next: () => nextPermGroupIterator(universe, state1),
      needsReset,
      reset: () => {
        return buildPermGroupInternal(
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

export class Edge extends ObjectValue<{
  parent: PermGroup;
  rank: Rank;
}>() {}

export class PermGroup extends ObjectValue<{
  readonly nodeId: NodeId;
  heads: ConcreteHeads;
  closedStreams: ConcreteHeads;

  edges: HashMap<EdgeId, Edge>;
}>() {
  desiredHeads(): AbstractHeads {
    const openStreams = HashMap.ofIterable<StreamId, "open" | OpStream>(
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
  next: () => Option<PersistentIteratorOp<OpStream, Op>> = () => Option.none(),
): PersistentIteratorValue<OpStream, Op> {
  return stream
    .head()
    .map((head) =>
      streamIteratorForStream(stream.tail().getOrElse(LinkedList.of()), () =>
        Option.of({
          op: head,
          value: {
            value: stream,
            next,
            needsReset: false,
            reset: () => {
              throw new AssertFailed("not implemented");
            },
          },
        }),
      ),
    )
    .getOrElse({
      value: stream,
      next: next,
      needsReset: false,
      reset: () => {
        throw new AssertFailed("not implemented");
      },
    });
}

//------------------------------------------------------------------------------
// Iterators and generic ops

interface PersistentIteratorValue<Value, Op extends {timestamp: Timestamp}> {
  value: Value;
  next: () => Option<PersistentIteratorOp<Value, Op>>;
  needsReset: boolean;
  reset: () => PersistentIteratorValue<Value, Op>;
}

interface PersistentIteratorOp<Value, Op extends {timestamp: Timestamp}> {
  op: Op;
  value: PersistentIteratorValue<Value, Op>;
}

// Advances the iterator until the next value is greater than `after`.
export function advanceIteratorUntil<T, Op extends {timestamp: Timestamp}>(
  iterator: PersistentIteratorValue<T, Op>,
  after: Timestamp,
): PersistentIteratorValue<T, Op> {
  // `iterators` and the limit of 10 times through the loop are for debugging.
  // We will have to find a more sophisticated way to handle this at some point.
  let iterators = LinkedList.of(iterator);
  for (let i = 0; i < 10; i++) {
    while (true) {
      const next = iterator.next();
      if (next.isNone()) break;
      if (next.get().op.timestamp > after) break;
      iterator = next.get().value;
    }
    if (!iterator.needsReset) return iterator;
    iterators = iterators.prepend(iterator);
    iterator = iterator.reset();
  }
  throw new AssertFailed("Iterator did not stabilize");
}

function nextOp<Op extends {timestamp: Timestamp}>(
  iterators: Seq<PersistentIteratorValue<unknown, Op>>,
): Option<Op> {
  const ops = iterators.mapOption((iterator) =>
    iterator.next().map((next) => next.op),
  );
  const opOption = ops.reduce(
    (leftOp: Op, right: Op): Op =>
      leftOp.timestamp < right.timestamp
        ? leftOp
        : right.timestamp < leftOp.timestamp
        ? right
        : leftOp === right
        ? leftOp
        : throwError("non-identical ops have the same timestamp"),
  );
  return opOption;
}
