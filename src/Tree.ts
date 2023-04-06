import {ObjectValue} from "./helper/ObjectValue";
import {TypedValue, value} from "./helper/TypedValue";
import {Timestamp} from "./helper/Timestamp";
import {HashMap, HashSet, LinkedList, Option, Vector} from "prelude-ts";
import {concreteHeadsForAbstractHeads, headsEqual} from "./StreamHeads";
import {
  mapMapOption,
  mapMapValueOption,
  mapValuesStable,
  throwError,
} from "./helper/Collection";
import {match, P} from "ts-pattern";
import {AssertFailed} from "./helper/Assert";

export class DeviceId extends TypedValue<"DeviceId", string> {}
export type StreamId =
  | DynamicPermGroupStreamId
  | TreeValueStreamId
  | TreeParentStreamId;
export type Op = DynamicPermGroupOp | TreeOp;
export type OpStream = LinkedList<Op>;
export type AbstractHeads = HashMap<StreamId, "open" | OpStream>;
export type ConcreteHeads = HashMap<StreamId, OpStream>;

export class Device extends ObjectValue<{
  heads: ConcreteHeads;
}>() {
  excludeFromEquals = HashSet.of("heads");
  equals(other: unknown): boolean {
    if (Object.getPrototypeOf(this) !== Object.getPrototypeOf(other))
      return false;

    return super.equals(other) && headsEqual(this.heads, (other as this).heads);
  }
}

const maxTimestamp = Timestamp.create(Number.MAX_SAFE_INTEGER);

//------------------------------------------------------------------------------
// Perm group

export type PermGroupId = StaticPermGroupId | DynamicPermGroupId;

interface PermGroup {
  readonly id: PermGroupId;
  writerDevices(): HashMap<DeviceId, "open" | Device>;
  writerGroups(): HashSet<PermGroupId>;
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

export class StaticPermGroup
  extends ObjectValue<{
    readonly id: PermGroupId;
    readonly writers: HashSet<DeviceId>;
  }>()
  implements PermGroup
{
  writerDevices(): HashMap<DeviceId, "open"> {
    return HashMap.ofIterable(
      this.writers.toVector().map((deviceId) => [deviceId, "open"]),
    );
  }

  writerGroups(): HashSet<PermGroupId> {
    return HashSet.of(this.id);
  }
}

export function buildStaticPermGroup(
  id: StaticPermGroupId,
): PersistentIteratorValue<StaticPermGroup, Op> {
  const value: PersistentIteratorValue<StaticPermGroup, Op> = {
    next: Option.none(),
    value: new StaticPermGroup({id, writers: id.writers}),
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

type DynamicPermGroupOp = AddWriter | RemoveWriter;

export type AddWriter = {
  timestamp: Timestamp;
  type: "add writer";

  groupId: DynamicPermGroupId;
  writerId: PermGroupId;
};

export type RemoveWriter = {
  timestamp: Timestamp;
  type: "remove writer";

  groupId: DynamicPermGroupId;
  writerId: PermGroupId;

  // This indicates the final heads that we'll use for any writers devices that get
  // removed by this op.
  contributingDevices: HashMap<DeviceId, Device>;
};

export class DynamicPermGroup
  extends ObjectValue<{
    readonly id: DynamicPermGroupId;
    heads: ConcreteHeads;
    closedDevices: HashMap<DeviceId, Device>;

    // Admins can write to this perm group -- that is, they can add and remove
    // writers.
    admin: PermGroup;
    // Writers can write to objects that use this perm group to decide on their
    // writers, but can't write to this perm group.
    writers: HashMap<PermGroupId, PermGroup>;
  }>()
  implements PermGroup
{
  // These are the heads that this perm group wants included in order to build
  // itself.
  desiredHeads(): AbstractHeads {
    const openStreams = mapMapOption(
      this.admin.writerDevices(),
      (deviceId, openOrDevice) => {
        const streamId = new DynamicPermGroupStreamId({
          permGroupId: this.id,
          deviceId: deviceId,
        });
        const openOrOpStream =
          openOrDevice === "open"
            ? Option.of<"open">("open")
            : openOrDevice.heads.get(streamId);
        if (!openOrOpStream.isSome())
          return Option.none<{key: StreamId; value: "open" | OpStream}>();
        return Option.of({key: streamId, value: openOrOpStream.get()});
      },
    );
    return openStreams;
  }

  // These are the devices that should be used as writers for anything using
  // this perm group to decide who can write. *This is not the list of devices
  // that can write to this perm group*.
  public writerDevices(): HashMap<DeviceId, "open" | Device> {
    const openDeviceIds = (group: PermGroup): HashSet<DeviceId> =>
      group
        .writerDevices()
        .filterValues((openOrDevice) => openOrDevice === "open")
        .keySet();
    const openWriterDevices = HashSet.of(
      ...openDeviceIds(this.admin),
      ...this.writers.foldLeft(HashSet.of<DeviceId>(), (devices, [, group]) =>
        HashSet.of(...devices, ...openDeviceIds(group)),
      ),
    );
    return HashMap.of(
      ...openWriterDevices
        .toVector()
        .map((deviceId) => [deviceId, "open"] as [DeviceId, "open" | Device]),
      ...this.closedDevices,
    );
  }

  writerGroups(): HashSet<PermGroupId> {
    return HashSet.of<PermGroupId>(
      this.id,
      ...this.admin.writerGroups(),
      ...Vector.ofIterable(this.writers.valueIterable()).flatMap(
        (writerGroup) => writerGroup.writerGroups().toVector(),
      ),
    );
  }

  excludeFromEquals = HashSet.of("closedStreams", "heads");
  equals(other: unknown): boolean {
    if (Object.getPrototypeOf(this) !== Object.getPrototypeOf(other))
      return false;

    return super.equals(other) && headsEqual(this.heads, (other as this).heads);
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
  const adminIterator = advanceIteratorBeyond(
    buildPermGroup(universe, id.admin),
    maxTimestamp,
  );
  const streamIterators = concreteHeadsForAbstractHeads(
    universe,
    new DynamicPermGroup({
      id,
      heads: HashMap.of(),
      closedDevices: HashMap.of(),
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
    closedDevices: HashMap.of(),
    admin: iterators.adminIterator.value,
    writers: HashMap.of(),
  });
  const iterator = {
    value: group,
    next: nextDynamicPermGroupIterator(universe, group, iterators),
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
    iterators.adminIterator,
    ...iterators.streamIterators.valueIterable(),
    ...iterators.writerIterators.valueIterable(),
  );
  if (opOption.isNone()) return Option.none();
  const op = opOption.get();

  return Option.of({
    op,
    value: () => {
      const adminIterator1 = iterators.adminIterator.next
        .flatMap((adminOp) =>
          adminOp.op === op
            ? Option.of(adminOp.value())
            : Option.none<PersistentIteratorValue<PermGroup, Op>>(),
        )
        .getOrElse(iterators.adminIterator);

      const writerIterators1 = iterators.writerIterators.mapValues((i) =>
        i.next.map((next) => (next.op === op ? next.value() : i)).getOrElse(i),
      );
      const writers1 = mapValuesStable(group.writers, (writer) =>
        writerIterators1
          .get(writer.id)
          .map((iterator) => iterator.value)
          .getOrElse(writer),
      );
      const streamIterators1 = iterators.streamIterators.mapValues(
        (streamIterator) => {
          return streamIterator.next
            .map((next) => (next.op === op ? next.value() : streamIterator))
            .getOrElse(streamIterator);
        },
      );
      const opHeads = mapMapValueOption(
        iterators.streamIterators,
        (streamId, streamIterator) => {
          return streamIterator.next
            .flatMap((next) =>
              next.op === op
                ? Option.of(next.value().value)
                : Option.none<OpStream>(),
            )
            .orElse(Option.none<OpStream>());
        },
      );
      const group1 = group.copy({
        admin: adminIterator1.value,
        writers: writers1,
        heads: group.heads.mergeWith(opHeads, (v0, v1) => v1),
      });

      const {group: group2, writerIterators: writerIterators2} = match({
        op,
        opHeads: opHeads,
      })
        .with(
          {op: {type: "add writer"}},
          ({opHeads}) => !opHeads.isEmpty(),
          ({op, opHeads}) => {
            const writerIterator = advanceIteratorBeyond(
              buildPermGroup(universe, op.writerId),
              Timestamp.create(value(op.timestamp) - 1),
            );
            if (writerIterator.value.writerGroups().contains(group.id))
              return {group: group1, writerIterators: writerIterators1};

            const writerIterators2 = !writerIterators1.containsKey(op.writerId)
              ? writerIterators1.put(op.writerId, writerIterator)
              : writerIterators1;
            return {
              group: group1.copy({
                writers: group1.writers.put(
                  op.writerId,
                  writerIterators2.get(op.writerId).getOrThrow().value,
                ),
              }),
              writerIterators: writerIterators2,
            };
          },
        )
        .with(
          {op: {type: "remove writer"}},
          ({opHeads}) => !opHeads.isEmpty(),
          ({op, opHeads}) => {
            return {
              group: group1.copy({
                writers: group1.writers.remove(op.writerId),
              }),
              writerIterators: writerIterators1,
            };
          },
        )
        .with(
          {},
          ({opHeads}) => !opHeads.isEmpty(),
          () =>
            throwError<{
              group: DynamicPermGroup;
              writerIterators: typeof writerIterators1;
            }>("Op not handled"),
        )
        .with(P._, () => ({group: group1, writerIterators: writerIterators1}))
        .exhaustive();

      const openDeviceIds = (group: PermGroup): HashSet<DeviceId> =>
        group
          .writerDevices()
          .filterValues((openOrDevice) => openOrDevice === "open")
          .keySet();
      const group3 = match({
        op,
        group,
        group1: group2,
        openDeviceIds0: openDeviceIds(group),
        openDeviceIds1: openDeviceIds(group2),
      })
        .with(
          {op: {type: "add writer"}},
          ({group, group1, openDeviceIds0, openDeviceIds1}) => {
            const openedWriterDeviceIds =
              openDeviceIds1.removeAll(openDeviceIds0);
            const closedDevices1 = group1.closedDevices.filterKeys(
              (deviceId) =>
                !(openedWriterDeviceIds.contains(deviceId) as boolean),
            );
            return group1.copy({closedDevices: closedDevices1});
          },
        )
        .with(
          {op: {type: "remove writer"}},
          ({op, group, group1, openDeviceIds0, openDeviceIds1}) => {
            const removedWriterDeviceIds =
              openDeviceIds0.removeAll(openDeviceIds1);
            const closedDevices1 = group1.closedDevices.mergeWith(
              removedWriterDeviceIds.toVector().mapOption((removedWriterId) => {
                return op.contributingDevices
                  .get(removedWriterId)
                  .map((device) => [removedWriterId, device]);
              }),
              (ops0, ops1) => throwError("Should not have stream-id collision"),
            );
            return group1.copy({closedDevices: closedDevices1});
          },
        )
        .otherwise(({group1}) => group1);
      // const closedDevices = group2.writers
      //   .foldLeft(group2.admin._closedDevices(), (current, [, writer]) =>
      //     current.mergeWith(
      //       writer._closedDevices(),
      //       (curentDevice, newDevice) =>
      //         throwError("Device closed twice -- not supported yet"),
      //     ),
      //   )
      //   .filterKeys((deviceId) => openDeviceIds(group2).contains(deviceId));
      // const group3 = group2.copy({closedDevices});

      const concreteHeads = concreteHeadsForAbstractHeads(
        universe,
        group3.desiredHeads(),
      );

      const iterators1: DynamicPermGroupIterators = {
        adminIterator: adminIterator1,
        streamIterators: streamIterators1,
        writerIterators: writerIterators2,
      };
      return {
        value: group3,
        next: nextDynamicPermGroupIterator(universe, group3, iterators1),
        _iterators: iterators,
        _iterators1: iterators1,
        _concreteHeads: concreteHeads,
      };
    },
  });
}

//------------------------------------------------------------------------------
// Tree

export class TreeId extends ObjectValue<{
  readonly permGroupId: PermGroupId;
  readonly rest: string | undefined;
}>() {
  readonly type = "tree";
}

export class TreeValueStreamId extends ObjectValue<{
  treeId: TreeId;
  deviceId: DeviceId;
}>() {
  readonly type = "tree value";
}

export class TreeParentStreamId extends ObjectValue<{
  treeId: TreeId;
  parentPermGroupId: PermGroupId;
  deviceId: DeviceId;
}>() {
  readonly type = "tree parent";
}

type TreeOp = SetParent;

export type SetParent = {
  timestamp: Timestamp;
  type: "set parent";

  childId: TreeId;
  parentId: TreeId;
};

export class Tree extends ObjectValue<{
  readonly id: TreeId;
  readonly parentPermGroupId: PermGroupId;
  permGroup: PermGroup;
  parentId: Option<TreeId>; // We don't know this until we get the SetParent op
  children: HashMap<TreeId, Tree>;
}>() {
  // These are the heads that this tree wants included in order to build itself.
  desiredHeads(): AbstractHeads {
    const valueStreams = mapMapOption(
      this.permGroup.writerDevices(),
      (deviceId, openOrDevice) => {
        const streamId = new TreeValueStreamId({
          treeId: this.id,
          deviceId: deviceId,
        });
        const openOrOpStream =
          openOrDevice === "open"
            ? Option.of<"open">("open")
            : openOrDevice.heads.get(streamId);
        if (!openOrOpStream.isSome())
          return Option.none<{key: StreamId; value: "open" | OpStream}>();
        return Option.of({key: streamId, value: openOrOpStream.get()});
      },
    );

    const parentStreams = mapMapOption(
      // TODO: this should use parentPermGroup istead of our own perm group.
      this.permGroup.writerDevices(),
      (deviceId, openOrDevice) => {
        const streamId = new TreeParentStreamId({
          treeId: this.id,
          parentPermGroupId: this.parentPermGroupId,
          deviceId: deviceId,
        });
        const openOrOpStream =
          openOrDevice === "open"
            ? Option.of<"open">("open")
            : openOrDevice.heads.get(streamId);
        if (!openOrOpStream.isSome())
          return Option.none<{key: StreamId; value: "open" | OpStream}>();
        return Option.of({key: streamId, value: openOrOpStream.get()});
      },
    );

    return valueStreams.mergeWith(parentStreams, () =>
      throwError("Stream ids should not collide"),
    );
  }

  containsId(id: TreeId): boolean {
    if (this.id.equals(id)) return true;
    return Vector.ofIterable(this.children.valueIterable()).anyMatch((tree) =>
      tree.containsId(id),
    );
  }
}

type TreeIterators = {
  readonly permGroupIterator: PersistentIteratorValue<PermGroup, Op>;
  readonly streamIterators: HashMap<
    StreamId,
    PersistentIteratorValue<OpStream, Op>
  >;
  readonly childIterators: HashMap<TreeId, PersistentIteratorValue<Tree, Op>>;
};

export function buildTree(
  universe: ConcreteHeads,
  id: TreeId,
  parentPermGroupId: PermGroupId,
): PersistentIteratorValue<Tree, Op> {
  const permGroupIterator = advanceIteratorBeyond(
    buildPermGroup(universe, id.permGroupId),
    maxTimestamp,
  );
  const streamIterators = concreteHeadsForAbstractHeads(
    universe,
    new Tree({
      id,
      parentPermGroupId,
      permGroup: permGroupIterator.value,
      parentId: Option.none(),
      children: HashMap.of(),
    }).desiredHeads(),
  ).mapValues((stream) => streamIteratorForStream(stream));
  return buildTreeInternal(universe, id, parentPermGroupId, {
    permGroupIterator,
    streamIterators,
    childIterators: HashMap.of(),
  });
}

function buildTreeInternal(
  universe: ConcreteHeads,
  id: TreeId,
  parentPermGroupId: PermGroupId,
  iterators: TreeIterators,
): PersistentIteratorValue<Tree, Op> {
  const tree = new Tree({
    id,
    parentPermGroupId,
    permGroup: iterators.permGroupIterator.value,
    parentId: Option.none(),
    children: HashMap.of(),
  });
  const iterator = {
    value: tree,
    next: nextTreeIterator(universe, tree, iterators),
    _iterators: iterators,
  };
  return iterator;
}

function nextTreeIterator(
  universe: ConcreteHeads,
  tree: Tree,
  iterators: TreeIterators,
): Option<PersistentIteratorOp<Tree, Op>> {
  const opOption = nextOp(
    iterators.permGroupIterator,
    ...iterators.streamIterators.valueIterable(),
    ...iterators.childIterators.valueIterable(),
  );
  if (opOption.isNone()) return Option.none();
  const op = opOption.get();

  return Option.of({
    op,
    value: () => {
      const permGroupIterator1 = iterators.permGroupIterator.next
        .flatMap((permGroupOp) =>
          permGroupOp.op === op
            ? Option.of(permGroupOp.value())
            : Option.none<PersistentIteratorValue<PermGroup, Op>>(),
        )
        .getOrElse(iterators.permGroupIterator);

      const childIterators1 = iterators.childIterators.mapValues((i) =>
        i.next.map((next) => (next.op === op ? next.value() : i)).getOrElse(i),
      );
      const children1 = mapValuesStable(tree.children, (child) =>
        childIterators1
          .get(child.id)
          .map((iterator) => iterator.value)
          .getOrElse(child),
      );
      const streamIterators1 = iterators.streamIterators.mapValues(
        (streamIterator) => {
          return streamIterator.next
            .map((next) => (next.op === op ? next.value() : streamIterator))
            .getOrElse(streamIterator);
        },
      );
      const opHeads = mapMapValueOption(
        iterators.streamIterators,
        (streamId, streamIterator) => {
          return streamIterator.next
            .flatMap((next) =>
              next.op === op
                ? Option.of(next.value().value)
                : Option.none<OpStream>(),
            )
            .orElse(Option.none<OpStream>());
        },
      );
      const tree1 = tree.copy({
        permGroup: permGroupIterator1.value,
        children: children1,
      });

      const {tree: tree2, childIterators: childIterators2} = match({
        op,
        opHeads: opHeads,
      })
        .with(
          {op: {type: "set parent"}},
          ({op, opHeads}) => op.parentId.equals(tree.id) && !opHeads.isEmpty(),
          ({op, opHeads}) => {
            if (childIterators1.containsKey(op.childId))
              return {tree: tree1, childIterators: childIterators1};

            // #TreeCycles: In order to avoid infinite recursion when
            // confronting a cycle, we must avoid computing the next value of
            // the child iterator here in the case where there will be a cycle.
            const childIterator = advanceIteratorBeyond(
              buildTree(universe, op.childId, tree.id.permGroupId),
              Timestamp.create(value(op.timestamp) - 1),
            );
            const childIteratorNext = childIterator.next;
            const childIterator1 =
              childIteratorNext.isSome() &&
              childIteratorNext.get().op === op &&
              !childIterator.value.containsId(op.parentId)
                ? childIteratorNext.get().value()
                : childIterator;

            const childParentId = childIterator1.value.parentId;
            if (childParentId.isNone() || !childParentId.get().equals(tree.id))
              return {tree: tree1, childIterators: childIterators1};
            return {
              tree: tree1.copy({
                children: tree1.children.put(op.childId, childIterator1.value),
              }),
              childIterators: childIterators1.put(op.childId, childIterator1),
            };
          },
        )
        .with(
          {op: {type: "set parent"}},
          ({op, opHeads}) => op.childId.equals(tree.id) && !opHeads.isEmpty(),
          ({op, opHeads}) => {
            // ##TreeCycles: This is the logical place to avoid cycles, and it's
            // necessary. But it's not sufficient.
            if (tree1.containsId(op.parentId))
              return {
                tree: tree1,
                childIterators: childIterators1,
              };
            return {
              tree: tree1.copy({
                parentId: Option.of(op.parentId),
              }),
              childIterators: childIterators1,
            };
          },
        )
        .with(
          {},
          ({opHeads}) => !opHeads.isEmpty(),
          () =>
            throwError<{tree: Tree; childIterators: typeof childIterators1}>(
              "Op not handled",
            ),
        )
        .with(P._, () => ({tree: tree1, childIterators: childIterators1}))
        .exhaustive();

      return treeIterator(universe, tree2, {
        permGroupIterator: permGroupIterator1,
        streamIterators: streamIterators1,
        childIterators: childIterators2,
      });
    },
  });
}

function treeIterator(
  universe: ConcreteHeads,
  tree: Tree,
  iterators: TreeIterators,
): PersistentIteratorValue<Tree, Op> {
  return {
    value: tree,
    next: nextTreeIterator(universe, tree, iterators),
    // _iterators: iterators,
    // _iterators1: iterators1,
  };
}

//------------------------------------------------------------------------------
// Iterators and generic ops

function streamIteratorForStream(
  stream: OpStream,
  next: Option<PersistentIteratorOp<OpStream, Op>> = Option.none(),
): PersistentIteratorValue<OpStream, Op> {
  return stream
    .head()
    .map((head) =>
      streamIteratorForStream(
        stream.tail().getOrElse(LinkedList.of()),
        Option.of({
          op: head,
          value: () => ({
            value: stream,
            next,
          }),
        }),
      ),
    )
    .getOrElse({
      value: stream,
      next: next,
    });
}

interface PersistentIteratorValue<Value, Op extends {timestamp: Timestamp}> {
  value: Value;
  next: Option<PersistentIteratorOp<Value, Op>>;
}

interface PersistentIteratorOp<Value, Op extends {timestamp: Timestamp}> {
  op: Op;
  // This is lazy so that the caller can inspect the op before computing the
  // value, which is necessary for avoiding cycles:
  // - 0: Add B as a writer on A
  // - 1: Add A as a writer on B
  // When processing op 0 we try to build (B until 0) to check for cycles. But
  // if that processes op 1 before rejecting it based on the timestamp then
  // we'll build (A until 1), which will process op 0 again, etc.
  value: () => PersistentIteratorValue<Value, Op>;
}

// Advances the iterator until there are no more ops or the next op is greater
// than `until`.
export function advanceIteratorBeyond<T, Op extends {timestamp: Timestamp}>(
  iterator: PersistentIteratorValue<T, Op>,
  until: Timestamp,
): PersistentIteratorValue<T, Op> {
  let lastOp = Option.none<Op>();
  while (true) {
    const next = iterator.next;
    if (next.isNone()) break;
    const nextIteratorOp = next.get();
    if (
      lastOp.isSome() &&
      lastOp.get().timestamp >= nextIteratorOp.op.timestamp
    ) {
      throw new AssertFailed("Timestamp failed to progress");
    }
    if (nextIteratorOp.op.timestamp > until) break;
    iterator = nextIteratorOp.value();
    lastOp = Option.of(nextIteratorOp.op);
  }
  return iterator;
}

function nextOp<Op extends {timestamp: Timestamp}>(
  ...iterators: Array<PersistentIteratorValue<unknown, Op>>
): Option<Op> {
  const ops = Vector.ofIterable(iterators).mapOption((iterator) =>
    iterator.next.map((next) => next.op),
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
