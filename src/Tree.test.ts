import {
  buildUpTree,
  DeviceId,
  Edge,
  EdgeId,
  NodeId,
  Op,
  OpList,
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
  const deviceId = DeviceId.create("device");
  const rootId = new NodeId({creator: deviceId, rest: undefined});
  const maxTimestamp = Timestamp.create(Number.MAX_SAFE_INTEGER);

  describe("desiredStreams", () => {
    it("writeable by creator when no parents", () => {
      const tree = new UpTree({
        nodeId: rootId,
        parents: HashMap.of(),
        heads: HashMap.of(),
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
        parents: HashMap.of([
          edgeId,
          new Edge({
            rank,
            parent: new UpTree({
              nodeId: parentId,
              heads: HashMap.of(),
              parents: HashMap.of(),
            }),
          }),
        ]),
        heads: HashMap.of(),
      });
      expectPreludeEqual(
        tree.desiredHeads(),
        HashMap.of(
          [
            new StreamId({deviceId: otherDeviceId, nodeId: rootId, type: "up"}),
            "open" as const,
          ],
          // [
          //   new StreamId({
          //     deviceId: otherDeviceId,
          //     nodeId: parentId,
          //     type: "up",
          //   }),
          //   "open" as const,
          // ],
        ),
      );
    });
  });

  describe("buildUpTree", () => {
    let clock: Clock;
    beforeEach(() => (clock = new CountingClock()));

    function opsList(...ops: Op[]): OpList {
      return LinkedList.ofIterable(ops).reverse() as OpList;
    }

    function setEdge(
      parentId: NodeId,
      childId: NodeId,
      extras?: {
        edgeId?: EdgeId;
        streams?: HashMap<StreamId, OpList>;
      },
    ): SetEdge {
      return {
        timestamp: clock.now(),
        type: "set edge",
        edgeId: extras?.edgeId || EdgeId.create("edge"),
        parentId,
        childId,
        rank: Rank.create(0),
        streams: extras?.streams || HashMap.of(),
      };
    }

    it("initial tree", () => {
      const tree = buildUpTree(HashMap.of(), Timestamp.create(0), rootId);
      expectPreludeEqual(tree.nodeId, rootId);
    });

    it("applies one op", () => {
      const parentId = new NodeId({creator: deviceId, rest: "parent"});
      const op = setEdge(parentId, rootId);
      const universe = HashMap.of([
        new StreamId({nodeId: rootId, deviceId: deviceId, type: "up"}),
        opsList(op),
      ]);
      const tree = buildUpTree(universe, maxTimestamp, rootId);
      const parent = tree.parents.single().getOrThrow()[1].parent;
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
      const tree = buildUpTree(universe, Timestamp.create(-1), rootId);
      expectPreludeEqual(tree.parents, HashMap.of());
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
      const tree = buildUpTree(universe, maxTimestamp, rootId);
      expectIdentical(
        headsEqual(
          tree.heads,
          universe.filterKeys((streamId) => streamId.nodeId.equals(rootId)),
        ),
        true,
      );
      const parent = tree.parents.single().getOrThrow()[1].parent;
      expectPreludeEqual(parent.nodeId, parentId);
      const grandparent = parent.parents.single().getOrThrow()[1].parent;
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
      const tree = buildUpTree(universe, maxTimestamp, rootId);
      expectIdentical(
        headsEqual(
          tree.heads,
          universe.filterKeys((streamId) => streamId.nodeId.equals(rootId)),
        ),
        true,
      );
      expectPreludeEqual(
        tree.parents.keySet(),
        HashSet.of(
          EdgeId.create("parent"),
          EdgeId.create("A"),
          EdgeId.create("B"),
        ),
      );
    });
  });
});
