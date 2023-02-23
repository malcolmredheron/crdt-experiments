import {
  advanceIteratorUntil,
  buildDynamicPermGroup,
  DeviceId,
  DynamicPermGroup,
  DynamicPermGroupId,
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

  function setEdge(
    parentId: PermGroupId,
    childId: DynamicPermGroupId,
    extras?: {
      streams?: HashMap<StreamId, OpStream>;
    },
  ): SetEdge {
    return {
      timestamp: clock.now(),
      type: "set edge",
      parentId,
      childId,
      contributingHeads: extras?.streams || HashMap.of(),
    };
  }

  const deviceId = DeviceId.create("device");
  const adminId = new StaticPermGroupId({writers: HashSet.of(deviceId)});
  const admin = new StaticPermGroup({id: adminId, writers: adminId.writers});
  const rootId = new DynamicPermGroupId({admin: adminId, rest: undefined});
  const maxTimestamp = Timestamp.create(Number.MAX_SAFE_INTEGER);

  describe("desiredStreams", () => {
    it("writeable by creator when no parents", () => {
      const group = new DynamicPermGroup({
        id: rootId,
        heads: HashMap.of(),
        closedStreams: HashMap.of(),
        admin,
        writers: HashMap.of(),
      });
      expectPreludeEqual(
        group.desiredHeads(),
        HashMap.of([
          new StreamId({deviceId: deviceId, nodeId: rootId, type: "up"}),
          "open" as const,
        ]),
      );
    });

    it("writeable by parents when parents", () => {
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
          new StreamId({deviceId: otherDeviceId, nodeId: rootId, type: "up"}),
          "open" as const,
        ]),
      );
    });

    it("includes closed streams", () => {
      const otherDeviceId = DeviceId.create("other device");
      const parentId = new StaticPermGroupId({
        writers: HashSet.of(otherDeviceId),
      });
      const otherDeviceOps = opsList(setEdge(parentId, rootId));
      const otherStreamId = new StreamId({
        nodeId: rootId,
        deviceId: otherDeviceId,
        type: "up",
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
              new StreamId({deviceId: deviceId, nodeId: rootId, type: "up"}),
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
      const op = setEdge(parentId, rootId);
      const universe = HashMap.of([
        new StreamId({nodeId: rootId, deviceId: deviceId, type: "up"}),
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
      const op = setEdge(parentId, rootId);
      const universe = HashMap.of([
        new StreamId({nodeId: rootId, deviceId: deviceId, type: "up"}),
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
          new StreamId({nodeId: parentId, deviceId: deviceId, type: "up"}),
          opsList(setEdge(grandparentId, parentId)),
        ],
        [
          new StreamId({nodeId: rootId, deviceId: deviceId, type: "up"}),
          opsList(setEdge(parentId, rootId)),
        ],
      );
      const group = advanceIteratorUntil(
        buildDynamicPermGroup(universe, rootId),
        maxTimestamp,
      ).value;
      expectIdentical(
        headsEqual(
          group.heads,
          universe.filterKeys((streamId) => streamId.nodeId.equals(rootId)),
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
          new StreamId({nodeId: rootId, deviceId: deviceId, type: "up"}),
          opsList(setEdge(parentId, rootId)),
        ],
        [
          new StreamId({nodeId: parentId, deviceId: deviceId, type: "up"}),
          opsList(setEdge(grandparentId, parentId)),
        ],
      );
      const group = advanceIteratorUntil(
        buildDynamicPermGroup(universe, rootId),
        maxTimestamp,
      ).value;
      expectIdentical(
        headsEqual(
          group.heads,
          universe.filterKeys((streamId) => streamId.nodeId.equals(rootId)),
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

    it("applies one op, adds a parent and updates the root with an earlier op from that parent", () => {
      const deviceAId = DeviceId.create("device a");
      const deviceBId = DeviceId.create("device b");
      const parentId = new StaticPermGroupId({
        writers: HashSet.of(deviceId),
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
          new StreamId({
            nodeId: rootId,
            deviceId: deviceAId,
            type: "up",
          }),
          opsList(setEdge(parentBId, rootId)),
        ],
        [
          new StreamId({nodeId: rootId, deviceId: deviceId, type: "up"}),
          opsList(
            // So that deviceId can continue to write to the root even after
            // parents are added.
            setEdge(parentId, rootId),
            setEdge(parentAId, rootId),
          ),
        ],
      );
      const group = advanceIteratorUntil(
        buildDynamicPermGroup(universe, rootId),
        maxTimestamp,
      ).value;
      expectIdentical(
        headsEqual(
          group.heads,
          universe.filterKeys((streamId) => streamId.nodeId.equals(rootId)),
        ),
        true,
      );
      expectPreludeEqual(
        group.writers.keySet(),
        HashSet.of(parentId, parentAId, parentBId),
      );
    });

    // Doing all of this on a parent forces us to handle nested resets.
    it("(to a parent) applies one op, adds a parent and updates the root with an earlier op from that parent", () => {
      const deviceAId = DeviceId.create("device a");
      const deviceBId = DeviceId.create("device b");
      const childId = new DynamicPermGroupId({
        admin: adminId,
        rest: "child",
      });
      const parentId = new StaticPermGroupId({
        writers: HashSet.of(deviceId),
      });
      const parentAId = new StaticPermGroupId({
        writers: HashSet.of(deviceAId),
      });
      const parentBId = new StaticPermGroupId({
        writers: HashSet.of(deviceBId),
      });
      const universe = HashMap.of(
        [
          new StreamId({
            nodeId: childId,
            deviceId: deviceId,
            type: "up",
          }),
          opsList(setEdge(rootId, childId)),
        ],
        // This op is earlier than the second one, but isn't part of the
        // original desired heads for the root.
        [
          new StreamId({
            nodeId: rootId,
            deviceId: deviceAId,
            type: "up",
          }),
          opsList(setEdge(parentBId, rootId)),
        ],
        [
          new StreamId({nodeId: rootId, deviceId: deviceId, type: "up"}),
          opsList(
            // So that deviceId can continue to write to the root even after
            // parents are added.
            setEdge(parentId, rootId),
            setEdge(parentAId, rootId),
          ),
        ],
      );
      const iterator = advanceIteratorUntil(
        buildDynamicPermGroup(universe, childId),
        maxTimestamp,
      );
      const group = iterator.value;
      const root = group.writers.single().getOrThrow()[1] as DynamicPermGroup;
      expectIdentical(
        headsEqual(
          root.heads,
          universe.filterKeys((streamId) => streamId.nodeId.equals(rootId)),
        ),
        true,
      );
      expectPreludeEqual(
        root.writers.keySet(),
        HashSet.of(parentId, parentAId, parentBId),
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
      const otherRootStreamId = new StreamId({
        nodeId: rootId,
        deviceId: deviceAId,
        type: "up",
      });
      const otherRootOps = opsList(setEdge(parentCId, rootId));
      const deviceRootStreamId = new StreamId({
        nodeId: rootId,
        deviceId: deviceId,
        type: "up",
      });
      const deviceRootEarlyOps = opsList(
        // So that deviceId can continue to write to the root even after
        // parents are added.
        setEdge(parentId, rootId),
        // Add otherDeviceId as a writer.
        setEdge(parentAId, rootId),
      );
      const universe = HashMap.of([
        deviceRootStreamId,
        deviceRootEarlyOps.prepend(
          // Remove otherDeviceId as a writer.
          setEdge(parentBId, rootId, {
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
