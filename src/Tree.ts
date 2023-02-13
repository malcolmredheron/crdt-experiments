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
import {match, P} from "ts-pattern";
import {AssertFailed} from "./helper/Assert";

export class DeviceId extends TypedValue<"DeviceId", string> {}
type NodeType = "up" | "down";
export class StreamId extends ObjectValue<{
  deviceId: DeviceId;
  nodeId: NodeId;
  type: NodeType;
  upRank: Rank;
}>() {}
export class NodeId extends ObjectValue<{
  creator: DeviceId;
  rest: string | undefined;
}>() {}
export class EdgeId extends TypedValue<"EdgeId", string> {}
export class Rank extends TypedValue<"EdgeId", number> {}
export const bootstrapRank = Rank.create(Number.MAX_SAFE_INTEGER);
export type Op = SetEdge;
export type OpStream = ConsLinkedList<Op>;
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
  readonly streamIterators: HashMap<StreamId, PersistentIterator<OpStream>>;
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
  streamIterators: HashMap<StreamId, PersistentIterator<OpStream>>,
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
  const opOption = nextOp(state.streamIterators, state.parentIterators);
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

  const opHeads = mapMapOption(
    state.streamIterators,
    (streamId, streamIterator) => {
      return streamIterator.op === op
        ? Option.of(streamIterator.result.value)
        : Option.none<OpStream>();
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

  const addedStreamIds = tree1
    .openStreams()
    .removeAll(state.tree.openStreams());
  const closedStreams1 = tree1.closedStreams.filterKeys(
    (streamId) => !addedStreamIds.contains(streamId),
  );
  const removedStreamIds = state.tree
    .openStreams()
    .removeAll(tree1.openStreams());
  const closedStreams2 = closedStreams1.mergeWith(
    removedStreamIds.toVector().mapOption((streamId) => {
      return (
        op.contributingHeads
          .get(streamId)
          // Automatically include a closed stream for any stream that contained
          // this operation. Without this, it's impossible to add the first parent
          // of a node (which closes the bootstrap stream) since the op can't
          // list itself in `contributingHeads`.
          .orElse(opHeads.get(streamId))
          .map((ops) => [streamId, ops])
      );
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

function nextOp(
  streamIterators: HashMap<StreamId, PersistentIterator<OpStream>>,
  parentIterators: HashMap<NodeId, InitialPersistentIterator<UpTree>>,
): Option<Op> {
  const streamOps = streamIterators
    .mapValues((streamIterator) => streamIterator.op)
    .valueIterable();
  const parentOps = mapMapOption(parentIterators, (streamId, parentIterator) =>
    parentIterator.next().map((next) => next.op),
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
  return opOption;
}

// Advances the iterator until the next value is greater than `after`.
export function advanceIteratorUntil<T>(
  iterator: InitialPersistentIterator<T>,
  after: Timestamp,
): InitialPersistentIterator<T> {
  // `iterators` and the limit of 10 times through the loop are for debugging.
  // We will have to find a more sophisticated way to handle this at some point.
  let iterators = LinkedList.of<{
    iterator: InitialPersistentIterator<T>;
    description: string;
  }>({iterator, description: "initial"});
  for (let i = 0; i < 10; i++) {
    while (true) {
      const next = iterator.next();
      if (next.isNone()) break;
      if (next.get().op.timestamp > after) break;
      iterator = next.get().result;
    }
    if (!iterator.needsReset) return iterator;
    iterators = iterators.prepend({iterator, description: "before reset"});
    iterator = iterator.reset();
    iterators = iterators.prepend({iterator, description: "after reset"});
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
  desiredHeads(): AbstractHeads {
    const openStreams = HashMap.ofIterable<StreamId, "open" | OpStream>(
      this.openStreams()
        .toVector()
        .map((streamId) => [streamId, "open"]),
    );
    const ourStreams = HashMap.ofIterable([
      ...openStreams,
      ...this.closedStreams,
    ]);
    return ourStreams;
  }

  private openWriterDevices(): HashSet<DeviceId> {
    // #Bootstrap: A node with no parents is writeable by the creator.
    if (this.edges.isEmpty()) return HashSet.of(this.nodeId.creator);
    return this.edges.foldLeft(HashSet.of(), (devices, [, edge]) =>
      HashSet.ofIterable([...devices, ...edge.parent.openWriterDevices()]),
    );
  }

  openStreams(): HashSet<StreamId> {
    // #Bootstrap: A node with no parents is writeable by the creator.
    if (this.edges.isEmpty())
      return HashSet.of(
        new StreamId({
          nodeId: this.nodeId,
          deviceId: this.nodeId.creator,
          type: "up",
          upRank: bootstrapRank,
        }),
      );
    return this.edges.foldLeft(HashSet.of(), (streams, [edgeId, edge]) =>
      streams.addAll(
        edge.parent.openWriterDevices().map(
          (deviceId) =>
            new StreamId({
              nodeId: this.nodeId,
              deviceId: deviceId,
              type: "up",
              upRank: edge.rank,
            }),
        ),
      ),
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
): PersistentIterator<OpStream> {
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
