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
  nodeId: PermGroupId;
  type: NodeType;
}>() {}
export type PermGroupId = StaticPermGroupId | DynamicPermGroupId;
export class StaticPermGroupId extends ObjectValue<{
  readonly writers: HashSet<DeviceId>;
}>() {
  readonly type = "static";
}
export class DynamicPermGroupId extends ObjectValue<{
  readonly creator: DeviceId;
  readonly rest: string | undefined;
}>() {
  readonly type = "dynamic";
}
export type Op = SetEdge;
export type OpStream = LinkedList<Op>;
export type AbstractHeads = HashMap<StreamId, "open" | OpStream>;
export type ConcreteHeads = HashMap<StreamId, OpStream>;

export type SetEdge = {
  timestamp: Timestamp;
  type: "set edge";

  childId: DynamicPermGroupId;
  parentId: PermGroupId;

  // This indicates the final op that we'll accept for any streams that get
  // removed by this op.
  contributingHeads: ConcreteHeads;
};

type DynamicPermGroupIteratorState = {
  readonly tree: DynamicPermGroup;
  readonly streamIterators: HashMap<
    StreamId,
    PersistentIteratorValue<OpStream, Op>
  >;
  readonly parentIterators: HashMap<
    PermGroupId,
    PersistentIteratorValue<PermGroup, Op>
  >;
};

export function buildPermGroup(
  universe: ConcreteHeads,
  id: PermGroupId,
): PersistentIteratorValue<PermGroup, Op> {
  switch (id.type) {
    case "static":
      return buildStaticPermGroup(id);
    case "dynamic":
      return buildDynamicPermGroup(universe, id);
  }
}

export function buildStaticPermGroup(
  id: StaticPermGroupId,
): PersistentIteratorValue<StaticPermGroup, Op> {
  const value: PersistentIteratorValue<StaticPermGroup, Op> = {
    next: () => Option.none(),
    value: new StaticPermGroup({id, devices: id.writers}),
    needsReset: false,
    reset: () => value,
  };
  return value;
}

export function buildDynamicPermGroup(
  universe: ConcreteHeads,
  id: DynamicPermGroupId,
): PersistentIteratorValue<DynamicPermGroup, Op> {
  const streamIterators = concreteHeadsForAbstractHeads(
    universe,
    new DynamicPermGroup({
      id,
      heads: HashMap.of(),
      closedStreams: HashMap.of(),
      writers: HashMap.of(),
    }).desiredHeads(),
  ).mapValues((stream) => streamIteratorForStream(stream));
  return buildDynamicPermGroupInternal(
    universe,
    id,
    streamIterators,
    HashMap.of(),
  );
}

function buildDynamicPermGroupInternal(
  universe: ConcreteHeads,
  id: DynamicPermGroupId,
  streamIterators: HashMap<StreamId, PersistentIteratorValue<OpStream, Op>>,
  parentIterators: HashMap<PermGroupId, PersistentIteratorValue<PermGroup, Op>>,
): PersistentIteratorValue<DynamicPermGroup, Op> {
  const tree = new DynamicPermGroup({
    id,
    heads: HashMap.of(),
    closedStreams: HashMap.of(),
    writers: HashMap.of(),
  });
  const state = asType<DynamicPermGroupIteratorState>({
    tree,
    streamIterators,
    parentIterators: parentIterators,
  });
  const iterator = {
    value: tree,
    next: () => nextDynamicPermGroupIterator(universe, state),
    needsReset: false,
    reset: () => iterator,
    _state: state,
  };
  return iterator;
}

function nextDynamicPermGroupIterator(
  universe: ConcreteHeads,
  state: DynamicPermGroupIteratorState,
): Option<PersistentIteratorOp<DynamicPermGroup, Op>> {
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
    op.childId.equals(state.tree.id) &&
    !parentIterators1.containsKey(op.parentId)
      ? parentIterators1.put(
          op.parentId,
          advanceIteratorUntil(
            buildPermGroup(universe, op.parentId),
            op.timestamp,
          ),
        )
      : parentIterators1;
  const writers1 = mapValuesStable(state.tree.writers, (writer) =>
    parentIterators2
      .get(writer.id)
      .map((iterator) => iterator.value)
      .getOrElse(writer),
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
          writers: writers1.put(
            op.parentId,
            parentIterators2.get(op.parentId).getOrThrow().value,
          ),
          heads: state.tree.heads.mergeWith(opHeads, (v0, v1) => v1),
        }),
    )
    .with(P._, () => state.tree.copy({writers: writers1}))
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
        nodeId: state.tree.id,
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
      next: () => nextDynamicPermGroupIterator(universe, state1),
      needsReset,
      reset: () => {
        return buildDynamicPermGroupInternal(
          universe,
          tree2.id,
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

interface PermGroup {
  readonly id: PermGroupId;
  openWriterDevices(): HashSet<DeviceId>;
}

export class StaticPermGroup extends ObjectValue<{
  readonly id: PermGroupId;
  readonly devices: HashSet<DeviceId>;
}>() {
  public openWriterDevices(): HashSet<DeviceId> {
    return this.devices;
  }
}

export class DynamicPermGroup extends ObjectValue<{
  readonly id: DynamicPermGroupId;
  heads: ConcreteHeads;
  closedStreams: ConcreteHeads;

  writers: HashMap<PermGroupId, PermGroup>;
}>() {
  desiredHeads(): AbstractHeads {
    const openStreams = HashMap.ofIterable<StreamId, "open" | OpStream>(
      this.openWriterDevices()
        .toVector()
        .map((deviceId) => [
          new StreamId({
            nodeId: this.id,
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
    if (this.writers.isEmpty()) return HashSet.of(this.id.creator);
    return this.writers.foldLeft(HashSet.of(), (devices, [, writer]) =>
      HashSet.ofIterable([...devices, ...writer.openWriterDevices()]),
    );
  }

  excludeFromEquals = HashSet.of("closedStreams", "heads");
  equals(other: unknown): boolean {
    if (Object.getPrototypeOf(this) !== Object.getPrototypeOf(other))
      return false;

    return (
      super.equals(other) &&
      headsEqual(this.closedStreams, (other as this).closedStreams) &&
      headsEqual(this.heads, (other as this).heads)
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
