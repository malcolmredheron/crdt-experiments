import {
  advanceIteratorUntil,
  buildDynamicPermGroup,
  DeviceId,
  DynamicPermGroupId,
  Edge,
  EdgeId,
  Op,
  OpStream,
  PermGroup,
  Rank,
  SetEdge,
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
    parentId: DynamicPermGroupId,
    childId: DynamicPermGroupId,
    extras?: {
      edgeId?: EdgeId;
      streams?: HashMap<StreamId, OpStream>;
    },
  ): SetEdge {
    return {
      timestamp: clock.now(),
      type: "set edge",
      edgeId: extras?.edgeId || EdgeId.create("edge"),
      parentId,
      childId,
      rank: Rank.create(0),
      contributingHeads: extras?.streams || HashMap.of(),
    };
  }

  const deviceId = DeviceId.create("device");
  const rootId = new DynamicPermGroupId({creator: deviceId, rest: undefined});
  const maxTimestamp = Timestamp.create(Number.MAX_SAFE_INTEGER);

  describe("desiredStreams", () => {
    it("writeable by creator when no parents", () => {
      const group = new PermGroup({
        id: rootId,
        heads: HashMap.of(),
        closedStreams: HashMap.of(),
        edges: HashMap.of(),
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
      const edgeId = EdgeId.create("edge");
      const rank = Rank.create(0);
      const otherDeviceId = DeviceId.create("other device");
      const parentId = new DynamicPermGroupId({
        creator: otherDeviceId,
        rest: "parent",
      });
      const group = new PermGroup({
        id: rootId,
        closedStreams: HashMap.of(),
        heads: HashMap.of(),
        edges: HashMap.of([
          edgeId,
          new Edge({
            rank,
            parent: new PermGroup({
              id: parentId,
              heads: HashMap.of(),
              closedStreams: HashMap.of(),
              edges: HashMap.of(),
            }),
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
      const parentId = new DynamicPermGroupId({
        creator: otherDeviceId,
        rest: "parent",
      });
      const otherDeviceOps = opsList(setEdge(parentId, rootId));
      const otherStreamId = new StreamId({
        nodeId: rootId,
        deviceId: otherDeviceId,
        type: "up",
      });
      const group = new PermGroup({
        id: rootId,
        heads: HashMap.of(),
        closedStreams: HashMap.of([otherStreamId, otherDeviceOps]),
        edges: HashMap.of(),
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
        creator: deviceId,
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
      const parent = group.edges.single().getOrThrow()[1].parent;
      expectPreludeEqual(parent.id, parentId);
    });

    it("ignores later op", () => {
      const parentId = new DynamicPermGroupId({
        creator: deviceId,
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
      expectPreludeEqual(group.edges, HashMap.of());
    });

    it("applies one op, adds a parent and updates it with an earlier op", () => {
      const parentId = new DynamicPermGroupId({
        creator: deviceId,
        rest: "parent",
      });
      const grandparentId = new DynamicPermGroupId({
        creator: deviceId,
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
      const parent = group.edges.single().getOrThrow()[1].parent;
      expectPreludeEqual(parent.id, parentId);
      const grandparent = parent.edges.single().getOrThrow()[1].parent;
      expectPreludeEqual(grandparent.id, grandparentId);
    });

    it("applies one op, adds a parent and updates it with a later op", () => {
      const parentId = new DynamicPermGroupId({
        creator: deviceId,
        rest: "parent",
      });
      const grandparentId = new DynamicPermGroupId({
        creator: deviceId,
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
      const parent = group.edges.single().getOrThrow()[1].parent;
      expectPreludeEqual(parent.id, parentId);
      const grandparent = parent.edges.single().getOrThrow()[1].parent;
      expectPreludeEqual(grandparent.id, grandparentId);
    });

    it("applies one op, adds a parent and updates the root with an earlier op from that parent", () => {
      const otherDeviceId = DeviceId.create("other device");
      const parentId = new DynamicPermGroupId({
        creator: deviceId,
        rest: "parent",
      });
      const parentAId = new DynamicPermGroupId({
        creator: otherDeviceId,
        rest: "parent a",
      });
      const parentBId = new DynamicPermGroupId({
        creator: otherDeviceId,
        rest: "parent b",
      });
      const universe = HashMap.of(
        // This op is earlier than the second one, but isn't part of the
        // original desired heads for the root.
        [
          new StreamId({
            nodeId: rootId,
            deviceId: otherDeviceId,
            type: "up",
          }),
          opsList(setEdge(parentBId, rootId, {edgeId: EdgeId.create("B")})),
        ],
        [
          new StreamId({nodeId: rootId, deviceId: deviceId, type: "up"}),
          opsList(
            // So that deviceId can continue to write to the root even after
            // parents are added.
            setEdge(parentId, rootId, {edgeId: EdgeId.create("parent")}),
            setEdge(parentAId, rootId, {edgeId: EdgeId.create("A")}),
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
        group.edges.keySet(),
        HashSet.of(
          EdgeId.create("parent"),
          EdgeId.create("A"),
          EdgeId.create("B"),
        ),
      );
    });

    // Doing all of this on a parent forces us to handle nested resets.
    it("(to a parent) applies one op, adds a parent and updates the root with an earlier op from that parent", () => {
      const otherDeviceId = DeviceId.create("other device");
      const childId = new DynamicPermGroupId({
        creator: deviceId,
        rest: "child",
      });
      const parentId = new DynamicPermGroupId({
        creator: deviceId,
        rest: "parent",
      });
      const parentAId = new DynamicPermGroupId({
        creator: otherDeviceId,
        rest: "parent a",
      });
      const parentBId = new DynamicPermGroupId({
        creator: otherDeviceId,
        rest: "parent b",
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
            deviceId: otherDeviceId,
            type: "up",
          }),
          opsList(setEdge(parentBId, rootId, {edgeId: EdgeId.create("B")})),
        ],
        [
          new StreamId({nodeId: rootId, deviceId: deviceId, type: "up"}),
          opsList(
            // So that deviceId can continue to write to the root even after
            // parents are added.
            setEdge(parentId, rootId, {edgeId: EdgeId.create("parent")}),
            setEdge(parentAId, rootId, {edgeId: EdgeId.create("A")}),
          ),
        ],
      );
      const iterator = advanceIteratorUntil(
        buildDynamicPermGroup(universe, childId),
        maxTimestamp,
      );
      const group = iterator.value;
      const root = group.edges.single().getOrThrow()[1].parent;
      expectIdentical(
        headsEqual(
          root.heads,
          universe.filterKeys((streamId) => streamId.nodeId.equals(rootId)),
        ),
        true,
      );
      expectPreludeEqual(
        root.edges.keySet(),
        HashSet.of(
          EdgeId.create("parent"),
          EdgeId.create("A"),
          EdgeId.create("B"),
        ),
      );
    });

    it("closes streams for removed writers", () => {
      const otherDeviceId = DeviceId.create("other device");
      const parentId = new DynamicPermGroupId({
        creator: deviceId,
        rest: "parent",
      });
      const parentAId = new DynamicPermGroupId({
        creator: otherDeviceId,
        rest: "parent a",
      });
      const parentBId = new DynamicPermGroupId({
        creator: deviceId,
        rest: "parent b",
      });
      const parentCId = new DynamicPermGroupId({
        creator: deviceId,
        rest: "parent c",
      });
      const otherRootStreamId = new StreamId({
        nodeId: rootId,
        deviceId: otherDeviceId,
        type: "up",
      });
      const otherRootOps = opsList(
        setEdge(parentCId, rootId, {
          edgeId: EdgeId.create("from contributions"),
        }),
      );
      const deviceRootStreamId = new StreamId({
        nodeId: rootId,
        deviceId: deviceId,
        type: "up",
      });
      const deviceRootEarlyOps = opsList(
        // So that deviceId can continue to write to the root even after
        // parents are added.
        setEdge(parentId, rootId, {edgeId: EdgeId.create("permanent")}),
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
      const group = advanceIteratorUntil(
        buildDynamicPermGroup(universe, rootId),
        maxTimestamp,
      ).value;
      // Only the stream for the removed writer gets closed.
      expectIdentical(
        headsEqual(
          group.closedStreams,
          HashMap.of([otherRootStreamId, otherRootOps]),
        ),
        true,
      );
      expectPreludeEqual(
        group.edges.keySet(),
        HashSet.of(
          EdgeId.create("permanent"),
          EdgeId.create("edge"),
          EdgeId.create("from contributions"),
        ),
      );
    });
  });
});
