import {
  advanceIteratorUntil,
  buildUpTree,
  DeviceId,
  EdgeId,
  Op,
  OpStream,
  PermGroupId,
  SetEdge,
  Tree,
  TreeId,
  TreeStreamId,
} from "./Tree";
import {HashMap, HashSet, LinkedList} from "prelude-ts";
import {Timestamp} from "./helper/Timestamp";
import {expectIdentical, expectPreludeEqual} from "./helper/Shared.testing";
import {CountingClock} from "./helper/Clock.testing";
import {Clock} from "./helper/Clock";

describe("Tree", () => {
  let clock: Clock;
  beforeEach(() => (clock = new CountingClock()));

  function opsList(...ops: Op[]): OpStream {
    return LinkedList.ofIterable(ops).reverse() as OpStream;
  }

  const deviceId = DeviceId.create("device");
  const rootId = TreeId.create("root");
  const maxTimestamp = Timestamp.create(Number.MAX_SAFE_INTEGER);

  function setEdge(
    parentId: TreeId,
    childId: TreeId,
    childPermGroupId: PermGroupId,
    extras?: {
      edgeId?: EdgeId;
      streams?: HashMap<TreeStreamId, OpStream>;
    },
  ): SetEdge {
    return {
      timestamp: clock.now(),
      type: "set edge",
      edgeId: extras?.edgeId || EdgeId.create("edge"),
      parentId,
      childId,
      childPermGroupId,
    };
  }

  describe("desiredStreams", () => {
    it("writeable by writers of perm group", () => {
      const permGroupId = new PermGroupId({writers: HashSet.of(deviceId)});
      const tree = new Tree({
        treeId: rootId,
        edges: HashMap.of(),
        permGroupId,
      });
      expectPreludeEqual(
        tree.desiredHeads(),
        HashMap.of([
          new TreeStreamId({
            type: "tree",
            deviceId: deviceId,
            treeId: rootId,
          }),
          "open" as const,
        ]),
      );
    });
  });

  describe("buildUpTree", () => {
    it("initial tree", () => {
      const permGroupId = new PermGroupId({writers: HashSet.of(deviceId)});
      const tree = buildUpTree(HashMap.of(), permGroupId, rootId).value;
      expectIdentical(tree.treeId, rootId);
    });

    it("applies one op", () => {
      const permGroupId = new PermGroupId({writers: HashSet.of(deviceId)});
      const childId = TreeId.create("child");
      const op = setEdge(rootId, childId, permGroupId);
      const universe = HashMap.of([
        new TreeStreamId({
          type: "tree",
          treeId: rootId,
          deviceId: deviceId,
        }),
        opsList(op),
      ]);
      const tree = advanceIteratorUntil(
        buildUpTree(universe, permGroupId, rootId),
        maxTimestamp,
      ).value;
      const child = tree.edges.single().getOrThrow()[1].tree;
      expectIdentical(child.treeId, childId);
    });

    it("ignores later op", () => {
      const permGroupId = new PermGroupId({writers: HashSet.of(deviceId)});
      const childId = TreeId.create("child");
      const op = setEdge(rootId, childId, permGroupId);
      const universe = HashMap.of([
        new TreeStreamId({
          type: "tree",
          treeId: rootId,
          deviceId: deviceId,
        }),
        opsList(op),
      ]);
      const tree = advanceIteratorUntil(
        buildUpTree(universe, permGroupId, rootId),
        Timestamp.create(-1),
      ).value;
      expectPreludeEqual(tree.edges, HashMap.of());
    });

    // it("closes streams for removed writers", () => {
    //   const otherDeviceId = DeviceId.create("other device");
    //   const parentId = new TreeId({creator: deviceId, rest: "parent"});
    //   const parentAId = new TreeId({creator: otherDeviceId, rest: "parent a"});
    //   const parentBId = new TreeId({creator: deviceId, rest: "parent b"});
    //   const parentCId = new TreeId({creator: deviceId, rest: "parent c"});
    //   const otherRootStreamId = new TreeStreamId({
    //     nodeId: rootId,
    //     deviceId: otherDeviceId,
    //     type: "up",
    //   });
    //   const otherRootOps = opsList(
    //     setEdge(parentCId, rootId, {
    //       edgeId: EdgeId.create("from contributions"),
    //     }),
    //   );
    //   const deviceRootStreamId = new TreeStreamId({
    //     nodeId: rootId,
    //     deviceId: deviceId,
    //     type: "up",
    //   });
    //   const deviceRootEarlyOps = opsList(
    //     // So that deviceId can continue to write to the root even after
    //     // parents are added.
    //     setEdge(parentId, rootId, {edgeId: EdgeId.create("permanent")}),
    //     // Add otherDeviceId as a writer.
    //     setEdge(parentAId, rootId),
    //   );
    //   const universe = HashMap.of([
    //     deviceRootStreamId,
    //     deviceRootEarlyOps.prepend(
    //       // Remove otherDeviceId as a writer.
    //       setEdge(parentBId, rootId, {
    //         streams: HashMap.of([otherRootStreamId, otherRootOps]),
    //       }),
    //     ),
    //   ]);
    //   const tree = advanceIteratorUntil(
    //     buildUpTree(universe, rootId),
    //     maxTimestamp,
    //   ).value;
    //   // Only the stream for the removed writer gets closed.
    //   expectIdentical(
    //     headsEqual(
    //       tree.closedStreams,
    //       HashMap.of([otherRootStreamId, otherRootOps]),
    //     ),
    //     true,
    //   );
    //   expectPreludeEqual(
    //     tree.edges.keySet(),
    //     HashSet.of(
    //       EdgeId.create("permanent"),
    //       EdgeId.create("edge"),
    //       EdgeId.create("from contributions"),
    //     ),
    //   );
    // });
  });
});
