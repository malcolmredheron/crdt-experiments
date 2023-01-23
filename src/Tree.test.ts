import {
  advanceIteratorUntil,
  buildUpTree,
  DeviceId,
  Edge,
  EdgeId,
  NodeId,
  Op,
  OpStream,
  Rank,
  SetEdge,
  StreamId,
  UpTree,
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
    parentId: NodeId,
    childId: NodeId,
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
  const rootId = new NodeId({creator: deviceId, rest: undefined});
  const maxTimestamp = Timestamp.create(Number.MAX_SAFE_INTEGER);

  describe("desiredStreams", () => {
    it("writeable by creator when no parents", () => {
      const tree = new UpTree({
        nodeId: rootId,
        heads: HashMap.of(),
        closedStreams: HashMap.of(),
        edges: HashMap.of(),
      });
      expectPreludeEqual(
        tree.desiredHeads(),
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
      const parentId = new NodeId({
        creator: otherDeviceId,
        rest: "parent",
      });
      const tree = new UpTree({
        nodeId: rootId,
        closedStreams: HashMap.of(),
        heads: HashMap.of(),
        edges: HashMap.of([
          edgeId,
          new Edge({
            rank,
            parent: new UpTree({
              nodeId: parentId,
              heads: HashMap.of(),
              closedStreams: HashMap.of(),
              edges: HashMap.of(),
            }),
          }),
        ]),
      });
      expectPreludeEqual(
        tree.desiredHeads(),
        HashMap.of([
          new StreamId({deviceId: otherDeviceId, nodeId: rootId, type: "up"}),
          "open" as const,
        ]),
      );
    });

    it("includes closed streams", () => {
      const otherDeviceId = DeviceId.create("other device");
      const parentId = new NodeId({
        creator: otherDeviceId,
        rest: "parent",
      });
      const otherDeviceOps = opsList(setEdge(parentId, rootId));
      const otherStreamId = new StreamId({
        nodeId: rootId,
        deviceId: otherDeviceId,
        type: "up",
      });
      const tree = new UpTree({
        nodeId: rootId,
        heads: HashMap.of(),
        closedStreams: HashMap.of([otherStreamId, otherDeviceOps]),
        edges: HashMap.of(),
      });
      expectIdentical(
        headsEqual(
          tree.desiredHeads(),
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

  describe("buildUpTree", () => {
    it("initial tree", () => {
      const tree = buildUpTree(HashMap.of(), rootId).value;
      expectPreludeEqual(tree.nodeId, rootId);
    });

    it("applies one op", () => {
      const parentId = new NodeId({creator: deviceId, rest: "parent"});
      const op = setEdge(parentId, rootId);
      const universe = HashMap.of([
        new StreamId({nodeId: rootId, deviceId: deviceId, type: "up"}),
        opsList(op),
      ]);
      const tree = advanceIteratorUntil(
        buildUpTree(universe, rootId),
        maxTimestamp,
      ).value;
      const parent = tree.edges.single().getOrThrow()[1].parent;
      expectPreludeEqual(parent.nodeId, parentId);
    });

    it("ignores later op", () => {
      const parentId = new NodeId({creator: deviceId, rest: "parent"});
      const op = setEdge(parentId, rootId);
      const universe = HashMap.of([
        new StreamId({nodeId: rootId, deviceId: deviceId, type: "up"}),
        opsList(op),
      ]);
      expectIdentical(op.timestamp, Timestamp.create(0));
      const tree = advanceIteratorUntil(
        buildUpTree(universe, rootId),
        Timestamp.create(-1),
      ).value;
      expectPreludeEqual(tree.edges, HashMap.of());
    });

    it("applies one op, adds a parent and updates it with an earlier op", () => {
      const parentId = new NodeId({creator: deviceId, rest: "parent"});
      const grandparentId = new NodeId({
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
      const tree = advanceIteratorUntil(
        buildUpTree(universe, rootId),
        maxTimestamp,
      ).value;
      expectIdentical(
        headsEqual(
          tree.heads,
          universe.filterKeys((streamId) => streamId.nodeId.equals(rootId)),
        ),
        true,
      );
      const parent = tree.edges.single().getOrThrow()[1].parent;
      expectPreludeEqual(parent.nodeId, parentId);
      const grandparent = parent.edges.single().getOrThrow()[1].parent;
      expectPreludeEqual(grandparent.nodeId, grandparentId);
    });

    it("applies one op, adds a parent and updates it with a later op", () => {
      const parentId = new NodeId({creator: deviceId, rest: "parent"});
      const grandparentId = new NodeId({
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
      const tree = advanceIteratorUntil(
        buildUpTree(universe, rootId),
        maxTimestamp,
      ).value;
      expectIdentical(
        headsEqual(
          tree.heads,
          universe.filterKeys((streamId) => streamId.nodeId.equals(rootId)),
        ),
        true,
      );
      const parent = tree.edges.single().getOrThrow()[1].parent;
      expectPreludeEqual(parent.nodeId, parentId);
      const grandparent = parent.edges.single().getOrThrow()[1].parent;
      expectPreludeEqual(grandparent.nodeId, grandparentId);
    });

    it("applies one op, adds a parent and updates the root with an earlier op from that parent", () => {
      const otherDeviceId = DeviceId.create("other device");
      const parentId = new NodeId({creator: deviceId, rest: "parent"});
      const parentAId = new NodeId({creator: otherDeviceId, rest: "parent a"});
      const parentBId = new NodeId({creator: otherDeviceId, rest: "parent b"});
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
      const tree = advanceIteratorUntil(
        buildUpTree(universe, rootId),
        maxTimestamp,
      ).value;
      expectIdentical(
        headsEqual(
          tree.heads,
          universe.filterKeys((streamId) => streamId.nodeId.equals(rootId)),
        ),
        true,
      );
      expectPreludeEqual(
        tree.edges.keySet(),
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
      const childId = new NodeId({creator: deviceId, rest: "child"});
      const parentId = new NodeId({creator: deviceId, rest: "parent"});
      const parentAId = new NodeId({creator: otherDeviceId, rest: "parent a"});
      const parentBId = new NodeId({creator: otherDeviceId, rest: "parent b"});
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
        buildUpTree(universe, childId),
        maxTimestamp,
      );
      const tree = iterator.value;
      const root = tree.edges.single().getOrThrow()[1].parent;
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
      const parentId = new NodeId({creator: deviceId, rest: "parent"});
      const parentAId = new NodeId({creator: otherDeviceId, rest: "parent a"});
      const parentBId = new NodeId({creator: deviceId, rest: "parent b"});
      const parentCId = new NodeId({creator: deviceId, rest: "parent c"});
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
      const tree = advanceIteratorUntil(
        buildUpTree(universe, rootId),
        maxTimestamp,
      ).value;
      // Only the stream for the removed writer gets closed.
      expectIdentical(
        headsEqual(
          tree.closedStreams,
          HashMap.of([otherRootStreamId, otherRootOps]),
        ),
        true,
      );
      expectPreludeEqual(
        tree.edges.keySet(),
        HashSet.of(
          EdgeId.create("permanent"),
          EdgeId.create("edge"),
          EdgeId.create("from contributions"),
        ),
      );
    });
  });
});
