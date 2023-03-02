import {
  advanceIteratorUntil,
  buildDynamicPermGroup,
  DeviceId,
  DynamicPermGroup,
  DynamicPermGroupId,
  DynamicPermGroupStreamId,
  Op,
  OpStream,
  PermGroupId,
  SetEdge,
  StaticPermGroup,
  StaticPermGroupId,
  StreamId,
} from "./Tree";
import {HashMap, HashSet, LinkedList} from "prelude-ts";
import {Timestamp} from "./helper/Timestamp";
import {expectIdentical, expectPreludeEqual} from "./helper/Shared.testing";
import {CountingClock} from "./helper/Clock.testing";
import {Clock} from "./helper/Clock";
import {headsEqual} from "./StreamHeads";

describe("Tree", () => {
  let clock: Clock;
  beforeEach(() => (clock = new CountingClock()));

  function opsList(...ops: Op[]): OpStream {
    return LinkedList.ofIterable(ops).reverse() as OpStream;
  }

  function addWriter(
    groupId: DynamicPermGroupId,
    writerId: PermGroupId,
    extras?: {streams?: HashMap<StreamId, OpStream>},
  ): SetEdge {
    return {
      timestamp: clock.now(),
      type: "add writer",
      groupId: groupId,
      writerId: writerId,
      contributingHeads: extras?.streams || HashMap.of(),
    };
  }

  const deviceId = DeviceId.create("device");
  const adminId = new StaticPermGroupId({writers: HashSet.of(deviceId)});
  const admin = new StaticPermGroup({id: adminId, writers: adminId.writers});
  const rootId = new DynamicPermGroupId({admin: adminId, rest: undefined});
  const maxTimestamp = Timestamp.create(Number.MAX_SAFE_INTEGER);

  describe("desiredStreams", () => {
    it("writeable by admins but not writers", () => {
      const otherDeviceId = DeviceId.create("other device");
      const parentId = new StaticPermGroupId({
        writers: HashSet.of(otherDeviceId),
      });
      const group = new DynamicPermGroup({
        id: rootId,
        closedStreams: HashMap.of(),
        heads: HashMap.of(),
        admin: admin,
        writers: HashMap.of([
          parentId,
          new StaticPermGroup({
            id: parentId,
            writers: HashSet.of(otherDeviceId),
          }),
        ]),
      });
      expectPreludeEqual(
        group.desiredHeads(),
        HashMap.of([
          new DynamicPermGroupStreamId({
            deviceId: deviceId,
            permGroupId: rootId,
          }),
          "open" as const,
        ]),
      );
      expectPreludeEqual(
        group.openWriterDevices(),
        HashSet.of(deviceId, otherDeviceId),
      );
    });

    it("includes closed streams", () => {
      const otherDeviceId = DeviceId.create("other device");
      const parentId = new StaticPermGroupId({
        writers: HashSet.of(otherDeviceId),
      });
      const otherDeviceOps = opsList(addWriter(rootId, parentId));
      const otherStreamId = new DynamicPermGroupStreamId({
        permGroupId: rootId,
        deviceId: otherDeviceId,
      });
      const group = new DynamicPermGroup({
        id: rootId,
        heads: HashMap.of(),
        closedStreams: HashMap.of([otherStreamId, otherDeviceOps]),
        admin,
        writers: HashMap.of(),
      });
      expectIdentical(
        headsEqual(
          group.desiredHeads(),
          HashMap.of<StreamId, "open" | OpStream>(
            [
              new DynamicPermGroupStreamId({
                deviceId: deviceId,
                permGroupId: rootId,
              }),
              "open" as const,
            ],
            [otherStreamId, otherDeviceOps],
          ),
        ),
        true,
      );
    });
  });

  describe("buildDynamicPermGroup", () => {
    it("initial value", () => {
      const group = buildDynamicPermGroup(HashMap.of(), rootId).value;
      expectPreludeEqual(group.id, rootId);
    });

    it("applies one op", () => {
      const parentId = new DynamicPermGroupId({
        admin: adminId,
        rest: "parent",
      });
      const op = addWriter(rootId, parentId);
      const universe = HashMap.of([
        new DynamicPermGroupStreamId({
          permGroupId: rootId,
          deviceId: deviceId,
        }),
        opsList(op),
      ]);
      const group = advanceIteratorUntil(
        buildDynamicPermGroup(universe, rootId),
        maxTimestamp,
      ).value;
      const parent = group.writers.single().getOrThrow()[1];
      expectPreludeEqual(parent.id, parentId);
    });

    it("ignores later op", () => {
      const parentId = new DynamicPermGroupId({
        admin: adminId,
        rest: "parent",
      });
      const op = addWriter(rootId, parentId);
      const universe = HashMap.of([
        new DynamicPermGroupStreamId({
          permGroupId: rootId,
          deviceId: deviceId,
        }),
        opsList(op),
      ]);
      expectIdentical(op.timestamp, Timestamp.create(0));
      const group = advanceIteratorUntil(
        buildDynamicPermGroup(universe, rootId),
        Timestamp.create(-1),
      ).value;
      expectPreludeEqual(group.writers, HashMap.of());
    });

    it("applies one op, adds a parent and updates it with an earlier op", () => {
      const parentId = new DynamicPermGroupId({
        admin: adminId,
        rest: "parent",
      });
      const grandparentId = new DynamicPermGroupId({
        admin: adminId,
        rest: "grandparent",
      });
      const universe = HashMap.of(
        [
          new DynamicPermGroupStreamId({
            permGroupId: parentId,
            deviceId: deviceId,
          }),
          opsList(addWriter(parentId, grandparentId)),
        ],
        [
          new DynamicPermGroupStreamId({
            permGroupId: rootId,
            deviceId: deviceId,
          }),
          opsList(addWriter(rootId, parentId)),
        ],
      );
      const group = advanceIteratorUntil(
        buildDynamicPermGroup(universe, rootId),
        maxTimestamp,
      ).value;
      expectIdentical(
        headsEqual(
          group.heads,
          universe.filterKeys((streamId) =>
            streamId.permGroupId.equals(rootId),
          ),
        ),
        true,
      );
      const parent = group.writers.single().getOrThrow()[1];
      expectPreludeEqual(parent.id, parentId);
      const grandparent = (parent as DynamicPermGroup).writers
        .single()
        .getOrThrow()[1];
      expectPreludeEqual(grandparent.id, grandparentId);
    });

    it("applies one op, adds a parent and updates it with a later op", () => {
      const parentId = new DynamicPermGroupId({
        admin: adminId,
        rest: "parent",
      });
      const grandparentId = new DynamicPermGroupId({
        admin: adminId,
        rest: "grandparent",
      });
      const universe = HashMap.of(
        [
          new DynamicPermGroupStreamId({
            permGroupId: rootId,
            deviceId: deviceId,
          }),
          opsList(addWriter(rootId, parentId)),
        ],
        [
          new DynamicPermGroupStreamId({
            permGroupId: parentId,
            deviceId: deviceId,
          }),
          opsList(addWriter(parentId, grandparentId)),
        ],
      );
      const group = advanceIteratorUntil(
        buildDynamicPermGroup(universe, rootId),
        maxTimestamp,
      ).value;
      expectIdentical(
        headsEqual(
          group.heads,
          universe.filterKeys((streamId) =>
            streamId.permGroupId.equals(rootId),
          ),
        ),
        true,
      );
      const parent = group.writers.single().getOrThrow()[1];
      expectPreludeEqual(parent.id, parentId);
      const grandparent = (parent as DynamicPermGroup).writers
        .single()
        .getOrThrow()[1];
      expectPreludeEqual(grandparent.id, grandparentId);
    });

    // Doing all of this on a parent forces us to handle nested resets.
    it("(to a parent) applies one op, adds a parent and updates the root with an earlier op from that parent", () => {
      const deviceAId = DeviceId.create("device a");
      const deviceBId = DeviceId.create("device b");
      const childId = new DynamicPermGroupId({
        admin: rootId,
        rest: "child",
      });
      const parentAId = new StaticPermGroupId({
        writers: HashSet.of(deviceAId),
      });
      const parentBId = new StaticPermGroupId({
        writers: HashSet.of(deviceBId),
      });
      const universe = HashMap.of(
        // This op is earlier than the second one, but isn't part of the
        // original desired heads for the root.
        [
          new DynamicPermGroupStreamId({
            permGroupId: childId,
            deviceId: deviceAId,
          }),
          opsList(addWriter(childId, parentBId)),
        ],
        [
          new DynamicPermGroupStreamId({
            permGroupId: rootId,
            deviceId: deviceId,
          }),
          opsList(addWriter(rootId, parentAId)),
        ],
      );
      const iterator = advanceIteratorUntil(
        buildDynamicPermGroup(universe, childId),
        maxTimestamp,
      );
      const group = iterator.value;
      const root = group;
      expectIdentical(
        headsEqual(
          root.heads,
          universe.filterKeys((streamId) =>
            streamId.permGroupId.equals(root.id),
          ),
        ),
        true,
      );
      expectPreludeEqual(
        root.openWriterDevices(),
        HashSet.of(deviceId, deviceAId, deviceBId),
      );
    });

    it("closes streams for removed writers", () => {
      const deviceAId = DeviceId.create("device a");
      const deviceBId = DeviceId.create("device b");
      const deviceCId = DeviceId.create("device c");
      const parentId = new StaticPermGroupId({
        writers: HashSet.of(deviceId),
      });
      const parentAId = new StaticPermGroupId({
        writers: HashSet.of(deviceAId),
      });
      const parentBId = new StaticPermGroupId({
        writers: HashSet.of(deviceBId),
      });
      const parentCId = new StaticPermGroupId({
        writers: HashSet.of(deviceCId),
      });
      const otherRootStreamId = new DynamicPermGroupStreamId({
        permGroupId: rootId,
        deviceId: deviceAId,
      });
      const otherRootOps = opsList(addWriter(rootId, parentCId));
      const deviceRootStreamId = new DynamicPermGroupStreamId({
        permGroupId: rootId,
        deviceId: deviceId,
      });
      const deviceRootEarlyOps = opsList(
        // So that deviceId can continue to write to the root even after
        // parents are added.
        addWriter(rootId, parentId),
        // Add otherDeviceId as a writer.
        addWriter(rootId, parentAId),
      );
      const universe = HashMap.of([
        deviceRootStreamId,
        deviceRootEarlyOps.prepend(
          // Remove otherDeviceId as a writer.
          addWriter(rootId, parentBId, {
            streams: HashMap.of([otherRootStreamId, otherRootOps]),
          }),
        ),
      ]);
      /*const group =*/ advanceIteratorUntil(
        buildDynamicPermGroup(universe, rootId),
        maxTimestamp,
      ).value;
      // Only the stream for the removed writer gets closed.
      // expectIdentical(
      //   headsEqual(
      //     group.closedStreams,
      //     HashMap.of([otherRootStreamId, otherRootOps]),
      //   ),
      //   true,
      // );
      // expectPreludeEqual(
      //   group.writers.keySet(),
      //   HashSet.of(
      //     EdgeId.create("permanent"),
      //     EdgeId.create("edge"),
      //     EdgeId.create("from contributions"),
      //   ),
      // );
    });
  });
});
