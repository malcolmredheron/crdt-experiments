import {ObjectValue} from "./helper/ObjectValue";
import {TypedValue, value} from "./helper/TypedValue";
import {Timestamp} from "./helper/Timestamp";
import {HashMap, HashSet, LinkedList, Option, Vector} from "prelude-ts";
import {concreteHeadsForAbstractHeads, headsEqual} from "./StreamHeads";
import {mapMapOption, mapMapValueOption, throwError} from "./helper/Collection";
import {match} from "ts-pattern";

export class DeviceId extends TypedValue<"DeviceId", string> {}
export type StreamId =
  | DynamicPermGroupStreamId
  | TreeValueStreamId
  | TreeParentStreamId;
export type Op = DynamicPermGroupOp | TreeOp;
export type OpStream = LinkedList<Op>; // The newest op is at the front.
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

function buildPermGroup(
  universe: ConcreteHeads,
  including: Timestamp,
  id: PermGroupId,
): PermGroup {
  switch (id.type) {
    case "static":
      return buildStaticPermGroup(id);
    case "dynamic":
      return buildDynamicPermGroup(universe, including, id);
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

export function buildStaticPermGroup(id: StaticPermGroupId): StaticPermGroup {
  return new StaticPermGroup({id, writers: id.writers});
}

//------------------------------------------------------------------------------
// Dynamic perm group

export class DynamicPermGroupId extends ObjectValue<{
  readonly adminId: PermGroupId;
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

  // This indicates the final heads that we'll use for any writers devices that
  // get removed by this op.
  contributingDevices: HashMap<DeviceId, Device>;
};

export class DynamicPermGroup
  extends ObjectValue<{
    readonly id: DynamicPermGroupId;
    // heads: ConcreteHeads;
    // closedDevices: HashMap<DeviceId, Device>;

    // Admins can write to this perm group -- that is, they can add and remove
    // writers.
    readonly admin: PermGroup;
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
      // ...this.closedDevices,
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

  // excludeFromEquals = HashSet.of("closedStreams", "heads");
  // equals(other: unknown): boolean {
  //   if (Object.getPrototypeOf(this) !== Object.getPrototypeOf(other))
  //     return false;
  //
  //   return super.equals(other) && headsEqual(this.heads, (other as this).heads);
  // }
}

export function buildDynamicPermGroup(
  universe: ConcreteHeads,
  including: Timestamp,
  id: DynamicPermGroupId,
): DynamicPermGroup {
  function initialValue(admin: PermGroup): DynamicPermGroup {
    return new DynamicPermGroup({
      id,
      admin,
      writers: HashMap.of(),
    });
  }
  function updateChildren(
    including: Timestamp,
    group: DynamicPermGroup,
  ): DynamicPermGroup {
    return group.copy({
      writers: group.writers.mapValues((writer) =>
        buildPermGroup(universe, including, writer.id),
      ),
    });
  }
  function updateForOp(group: DynamicPermGroup, op: Op): DynamicPermGroup {
    return match({op, group})
      .with({op: {type: "add writer"}}, ({op, group}) => {
        if (group.writers.containsKey(op.writerId)) return group;

        const earlierWriter = buildPermGroup(
          universe,
          Timestamp.create(value(op.timestamp) - 1),
          op.writerId,
        );
        if (earlierWriter.writerGroups().contains(group.id)) return group;

        return group.copy({
          writers: group.writers.put(
            op.writerId,
            buildPermGroup(universe, op.timestamp, op.writerId),
          ),
        });
      })
      .with({op: {type: "remove writer"}}, ({op, group}) =>
        group.copy({
          writers: group.writers.remove(op.writerId),
        }),
      )
      .otherwise(({group}) => group);
  }
  return buildValue(
    universe,
    including,
    id,
    initialValue,
    updateChildren,
    updateForOp,
  );
}

//------------------------------------------------------------------------------
// Tree

export class TreeId extends ObjectValue<{
  readonly adminId: PermGroupId;
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
  readonly admin: PermGroup;
  parentId: Option<TreeId>; // We don't know this until we get the SetParent op
  children: HashMap<TreeId, Tree>;
}>() {
  // These are the heads that this tree wants included in order to build itself.
  desiredHeads(): AbstractHeads {
    const valueStreams = mapMapOption(
      this.admin.writerDevices(),
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
      this.admin.writerDevices(),
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

export function buildTree(
  universe: ConcreteHeads,
  including: Timestamp,
  id: TreeId,
  parentPermGroupId: PermGroupId,
): Tree {
  function initialValue(admin: PermGroup): Tree {
    return new Tree({
      id,
      parentPermGroupId,
      admin: buildPermGroup(universe, maxTimestamp, id.adminId),
      parentId: Option.none(),
      children: HashMap.of(),
    });
  }
  function updateChildren(including: Timestamp, tree: Tree): Tree {
    return tree.copy({
      children: mapMapValueOption(tree.children, (childId, child) => {
        const child1 = buildTree(
          universe,
          including,
          child.id,
          child.parentPermGroupId,
        );
        // TODO: we also need to check that the op that set the parent is the op
        // that we think set the parent.
        return child1.parentId
          .map((parentId) => parentId.equals(tree.id))
          .getOrElse(false)
          ? Option.some(child1)
          : Option.none<Tree>();
      }),
    });
  }
  function updateForOp(tree: Tree, op: Op): Tree {
    return match({op, tree})
      .with(
        {op: {type: "set parent"}},
        ({op, tree}) => op.childId.equals(tree.id),
        ({op, tree}) =>
          tree.containsId(op.parentId)
            ? tree
            : tree.copy({parentId: Option.some(op.parentId)}),
      )
      .with(
        {op: {type: "set parent"}},
        ({op, tree}) => op.parentId.equals(tree.id),
        ({op, tree}) => {
          const childId = op.childId;
          if (tree.children.containsKey(childId)) return tree;

          // #TreeCycles: In order to avoid infinite recursion when
          // confronting a cycle, we must avoid computing the current value of
          // the child here in the case where there will be a cycle.
          const earlierChild = buildTree(
            universe,
            Timestamp.create(value(op.timestamp) - 1),
            childId,
            tree.admin.id,
          );
          if (earlierChild.containsId(op.parentId)) return tree;

          const child = buildTree(
            universe,
            op.timestamp,
            childId,
            tree.admin.id,
          );
          if (
            !child.parentId
              .map((parentId) => parentId.equals(tree.id))
              .getOrElse(false)
          )
            return tree;

          return tree.copy({
            children: tree.children.put(childId, child),
          });
        },
      )
      .otherwise(({tree}) => tree);
  }
  return buildValue(
    universe,
    including,
    id,
    initialValue,
    updateChildren,
    updateForOp,
  );
}

//------------------------------------------------------------------------------
// Helpers

function buildValue<
  Id extends {readonly adminId: PermGroupId},
  Value extends {desiredHeads: () => AbstractHeads},
>(
  universe: ConcreteHeads,
  including: Timestamp,
  id: Id,
  initialValue: (admin: PermGroup) => Value,
  updateChildren: (including: Timestamp, value: Value) => Value,
  updateForOp: (value: Value, op: Op) => Value,
): Value {
  let value = initialValue(buildPermGroup(universe, maxTimestamp, id.adminId));
  let streamIterators = concreteHeadsForAbstractHeads(
    universe,
    value.desiredHeads(),
  ).mapValues((stream) => stream.reverse());

  while (true) {
    const opOption = nextOp(...streamIterators.valueIterable()).flatMap((op) =>
      op.timestamp > including ? Option.none<Op>() : Option.some(op),
    );
    const opTimestamp = opOption.map((op) => op.timestamp).getOrElse(including);
    const value1 = updateChildren(opTimestamp, value);

    if (opOption.isNone()) {
      value = value1;
      break;
    }

    const op = opOption.get();
    value = updateForOp(value1, op);
    streamIterators = mapMapValueOption(
      streamIterators,
      (streamId, streamIterator) => {
        return streamIterator
          .head()
          .flatMap((head) =>
            head === op ? streamIterator.tail() : Option.some(streamIterator),
          );
      },
    );
  }

  return value;
}

function nextOp<Op extends {timestamp: Timestamp}>(
  ...iterators: Array<LinkedList<Op>>
): Option<Op> {
  const ops = Vector.ofIterable(iterators).mapOption((iterator) =>
    iterator.head(),
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
