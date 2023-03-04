import {ObjectValue} from "./helper/ObjectValue";
import {TypedValue} from "./helper/TypedValue";
import {Timestamp} from "./helper/Timestamp";
import {HashMap, HashSet, LinkedList, Option, Vector} from "prelude-ts";
import {concreteHeadsForAbstractHeads, headsEqual} from "./StreamHeads";
import {mapMapOption, mapValuesStable, throwError} from "./helper/Collection";
import {match, P} from "ts-pattern";
import {AssertFailed} from "./helper/Assert";
import {Seq} from "prelude-ts/dist/src/Seq";

export class DeviceId extends TypedValue<"DeviceId", string> {}
export type StreamId = DynamicPermGroupStreamId;
export type Op = AddWriter | RemoveWriter;
export type OpStream = LinkedList<Op>;
export type AbstractHeads = HashMap<StreamId, "open" | OpStream>;
export type ConcreteHeads = HashMap<StreamId, OpStream>;

//------------------------------------------------------------------------------
// Perm group

export type PermGroupId = StaticPermGroupId | DynamicPermGroupId;

interface PermGroup {
  readonly id: PermGroupId;
  openWriterDevices(): HashSet<DeviceId>;
}

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

//------------------------------------------------------------------------------
// Static perm group

export class StaticPermGroupId extends ObjectValue<{
  readonly writers: HashSet<DeviceId>;
}>() {
  readonly type = "static";
}

export class StaticPermGroup extends ObjectValue<{
  readonly id: PermGroupId;
  readonly writers: HashSet<DeviceId>;
}>() {
  public openWriterDevices(): HashSet<DeviceId> {
    return this.writers;
  }
}

export function buildStaticPermGroup(
  id: StaticPermGroupId,
): PersistentIteratorValue<StaticPermGroup, Op> {
  const value: PersistentIteratorValue<StaticPermGroup, Op> = {
    next: () => Option.none(),
    value: new StaticPermGroup({id, writers: id.writers}),
    needsReset: false,
    reset: () => value,
  };
  return value;
}

//------------------------------------------------------------------------------
// Dynamic perm group

export class DynamicPermGroupId extends ObjectValue<{
  readonly admin: PermGroupId;
  readonly rest: string | undefined;
}>() {
  readonly type = "dynamic";
}

export class DynamicPermGroupStreamId extends ObjectValue<{
  permGroupId: DynamicPermGroupId;
  deviceId: DeviceId;
}>() {
  readonly type = "DynamicPermGroup";
}

export type AddWriter = {
  timestamp: Timestamp;
  type: "add writer";

  groupId: DynamicPermGroupId;
  writerId: PermGroupId;

  // This indicates the final op that we'll accept for any streams that get
  // removed by this op.
  contributingHeads: ConcreteHeads;
};

export type RemoveWriter = {
  timestamp: Timestamp;
  type: "remove writer";

  groupId: DynamicPermGroupId;
  writerId: PermGroupId;

  // This indicates the final op that we'll accept for any streams that get
  // removed by this op.
  contributingHeads: ConcreteHeads;
};

export class DynamicPermGroup extends ObjectValue<{
  readonly id: DynamicPermGroupId;
  heads: ConcreteHeads;
  closedStreams: ConcreteHeads;

  // Admins can write to this perm group -- that is, they can add and remove
  // writers.
  admin: PermGroup;
  // Writers can write to objects that use this perm group to decide on their
  // writers, but can't write to this perm group.
  writers: HashMap<PermGroupId, PermGroup>;
}>() {
  // These are the heads that this perm group wants included in order to build
  // itself.
  desiredHeads(): AbstractHeads {
    const openStreams = HashMap.ofIterable<StreamId, "open" | OpStream>(
      this.admin
        .openWriterDevices()
        .toVector()
        .map((deviceId) => [
          new DynamicPermGroupStreamId({
            permGroupId: this.id,
            deviceId: deviceId,
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

  // These are the devices that should be used as writers for anything using
  // this perm group to decide who can write. *This is not the list of devices
  // that can write to this perm group*.
  public openWriterDevices(): HashSet<DeviceId> {
    return HashSet.of(
      ...this.admin.openWriterDevices(),
      ...this.writers.foldLeft(HashSet.of<DeviceId>(), (devices, [, group]) =>
        HashSet.of(...devices, ...group.openWriterDevices()),
      ),
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

type DynamicPermGroupIterators = {
  readonly adminIterator: PersistentIteratorValue<PermGroup, Op>;
  readonly streamIterators: HashMap<
    StreamId,
    PersistentIteratorValue<OpStream, Op>
  >;
  readonly writerIterators: HashMap<
    PermGroupId,
    PersistentIteratorValue<PermGroup, Op>
  >;
};

export function buildDynamicPermGroup(
  universe: ConcreteHeads,
  id: DynamicPermGroupId,
): PersistentIteratorValue<DynamicPermGroup, Op> {
  const adminIterator = buildPermGroup(universe, id.admin);
  const streamIterators = concreteHeadsForAbstractHeads(
    universe,
    new DynamicPermGroup({
      id,
      heads: HashMap.of(),
      closedStreams: HashMap.of(),
      admin: adminIterator.value,
      writers: HashMap.of(),
    }).desiredHeads(),
  ).mapValues((stream) => streamIteratorForStream(stream));
  return buildDynamicPermGroupInternal(universe, id, {
    adminIterator,
    streamIterators,
    writerIterators: HashMap.of(),
  });
}

function buildDynamicPermGroupInternal(
  universe: ConcreteHeads,
  id: DynamicPermGroupId,
  iterators: DynamicPermGroupIterators,
): PersistentIteratorValue<DynamicPermGroup, Op> {
  const group = new DynamicPermGroup({
    id,
    heads: HashMap.of(),
    closedStreams: HashMap.of(),
    admin: iterators.adminIterator.value,
    writers: HashMap.of(),
  });
  const iterator = {
    value: group,
    next: () => nextDynamicPermGroupIterator(universe, group, iterators),
    needsReset: false,
    reset: () => iterator,
    _iterators: iterators,
  };
  return iterator;
}

function nextDynamicPermGroupIterator(
  universe: ConcreteHeads,
  group: DynamicPermGroup,
  iterators: DynamicPermGroupIterators,
): Option<PersistentIteratorOp<DynamicPermGroup, Op>> {
  const opOption = nextOp(
    Vector.of<PersistentIteratorValue<unknown, Op>>(
      iterators.adminIterator,
      ...iterators.streamIterators.valueIterable(),
      ...iterators.writerIterators.valueIterable(),
    ),
  );
  if (opOption.isNone()) return Option.none();
  const op = opOption.get();

  const adminIterator1 = iterators.adminIterator
    .next()
    .flatMap((adminOp) =>
      adminOp.op === op
        ? Option.of(adminOp.value)
        : Option.none<typeof adminOp.value>(),
    )
    .getOrElse(iterators.adminIterator);

  const writerIterators1 = iterators.writerIterators.mapValues((i) =>
    i
      .next()
      .map((next) => (next.op === op ? next.value : i))
      .getOrElse(i),
  );
  const writerIterators2 =
    op.groupId.equals(group.id) && !writerIterators1.containsKey(op.writerId)
      ? writerIterators1.put(
          op.writerId,
          advanceIteratorUntil(
            buildPermGroup(universe, op.writerId),
            op.timestamp,
          ),
        )
      : writerIterators1;
  const writers1 = mapValuesStable(group.writers, (writer) =>
    writerIterators2
      .get(writer.id)
      .map((iterator) => iterator.value)
      .getOrElse(writer),
  );

  const streamIterators1 = iterators.streamIterators.mapValues(
    (streamIterator) => {
      return streamIterator
        .next()
        .map((next) => (next.op === op ? next.value : streamIterator))
        .getOrElse(streamIterator);
    },
  );

  const group1 = group.copy({
    admin: adminIterator1.value,
    writers: writers1,
  });

  const opHeads: HashMap<StreamId, OpStream> = mapMapOption(
    iterators.streamIterators,
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
  const group2 = match({op, opHeads})
    .with(
      {op: {type: "add writer"}},
      ({opHeads}) => !opHeads.isEmpty(),
      ({opHeads}) =>
        group1.copy({
          writers: group1.writers.put(
            op.writerId,
            writerIterators2.get(op.writerId).getOrThrow().value,
          ),
          heads: group.heads.mergeWith(opHeads, (v0, v1) => v1),
        }),
    )
    .with(
      {op: {type: "remove writer"}},
      ({opHeads}) => !opHeads.isEmpty(),
      ({opHeads}) =>
        group1.copy({
          writers: group1.writers.remove(op.writerId),
          heads: group.heads.mergeWith(opHeads, (v0, v1) => v1),
        }),
    )
    .with(
      {},
      ({opHeads}) => !opHeads.isEmpty(),
      () => throwError<DynamicPermGroup>("Op not handled"),
    )
    .with(P._, () => group1)
    .exhaustive();

  const addedWriterDeviceIds = group2
    .openWriterDevices()
    .removeAll(group.openWriterDevices());
  const closedStreams1 = group2.closedStreams.filterKeys((streamId) =>
    addedWriterDeviceIds.contains(streamId.deviceId),
  );
  const removedWriterDeviceIds = group
    .openWriterDevices()
    .removeAll(group2.openWriterDevices());
  const closedStreams2 = closedStreams1.mergeWith(
    removedWriterDeviceIds.toVector().mapOption((removedWriterId) => {
      const streamId = new DynamicPermGroupStreamId({
        permGroupId: group.id,
        deviceId: removedWriterId,
      });
      return op.contributingHeads.get(streamId).map((ops) => [streamId, ops]);
    }),
    (ops0, ops1) => throwError("Should not have stream-id collision"),
  );
  const group3 = group2.copy({closedStreams: closedStreams2});

  const concreteHeads = concreteHeadsForAbstractHeads(
    universe,
    group3.desiredHeads(),
  );
  const headsNeedReset = !headsEqual(concreteHeads, group3.heads);
  const needsReset =
    headsNeedReset ||
    adminIterator1.needsReset ||
    writerIterators2.anyMatch(
      (nodeId, writerIterator) => writerIterator.needsReset,
    );

  const iterators1: DynamicPermGroupIterators = {
    adminIterator: adminIterator1,
    streamIterators: streamIterators1,
    writerIterators: writerIterators2,
  };
  return Option.of({
    op,
    value: {
      value: group3,
      next: () => nextDynamicPermGroupIterator(universe, group3, iterators1),
      needsReset,
      reset: () => {
        return buildDynamicPermGroupInternal(universe, group3.id, {
          adminIterator: adminIterator1.reset(),
          streamIterators: concreteHeads.mapValues((stream) =>
            streamIteratorForStream(stream),
          ),
          writerIterators: iterators1.writerIterators.mapValues((iterator) =>
            iterator.reset(),
          ),
        });
      },
      _iterators: iterators,
      _iterators1: iterators1,
      _headsNeedReset: headsNeedReset,
      _concreteHeads: concreteHeads,
    },
  });
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
  let iterators = LinkedList.of<{
    iterator: PersistentIteratorValue<T, Op>;
    description: string;
  }>({iterator, description: "initial"});
  for (let i = 0; i < 10; i++) {
    while (true) {
      const next = iterator.next();
      if (next.isNone()) break;
      if (next.get().op.timestamp > after) break;
      iterator = next.get().value;
    }
    if (!iterator.needsReset) return iterator;
    iterators = iterators.prepend({iterator, description: "before reset"});
    iterator = iterator.reset();
    iterators = iterators.prepend({iterator, description: "after reset"});
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
