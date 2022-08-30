import {
  AppliedOp,
  createPermissionedTree,
  DeviceId,
  NodeId,
  NodeKey,
  PriorityStatus,
  ShareData,
  SharedNode,
  ShareId,
  StreamId,
} from "./NestedPermissionedTree";
import {CountingClock} from "./helper/Clock.testing";
import {
  expectDeepEqual,
  expectIdentical,
  expectPreludeEqual,
} from "./helper/Shared.testing";
import {HashMap, LinkedList, Option} from "prelude-ts";
import {asType} from "./helper/Collection";

describe("NestedPermissionedTree", () => {
  const clock = new CountingClock();
  const deviceA = DeviceId.create("A");
  const deviceB = DeviceId.create("B");
  const shareId = new ShareId({creator: deviceA, id: NodeId.create("share")});
  const deviceAStreamId = new StreamId({deviceId: deviceA, shareId});
  const tree = createPermissionedTree(shareId);

  function openWriterOp(
    device: DeviceId,
    writer: DeviceId,
    priority: number,
  ): AppliedOp["op"] {
    return {
      timestamp: clock.now(),
      device: device,
      type: "set writer",
      targetWriter: writer,
      priority: priority,
      status: "open",
    };
  }
  function createNodeOp(device: DeviceId, nodeId: NodeId): AppliedOp["op"] {
    return {
      timestamp: clock.now(),
      device,
      type: "create node",
      node: nodeId,
      shareId: undefined,
    };
  }
  function setParentOp(
    device: DeviceId,
    nodeId: NodeId,
    parentNodeId: NodeId,
    position: number,
  ): AppliedOp["op"] {
    return {
      timestamp: clock.now(),
      device,
      type: "set parent",
      node: nodeId,
      shareId: Option.none(),
      parent: parentNodeId,
      position,
    };
  }

  describe("permissions", () => {
    it("adds a lower-ranked writer", () => {
      const tree1 = tree.update(
        HashMap.of([
          new StreamId(deviceAStreamId),
          LinkedList.of(openWriterOp(deviceA, deviceB, -1)),
        ]),
      );
      expectDeepEqual(
        tree1.value.roots
          .get(tree1.value.rootKey)
          .getOrThrow()
          .shareData!.writers.get(deviceB)
          .getOrUndefined(),
        new PriorityStatus({
          priority: -1,
          status: "open",
        }),
      );
    });

    it("ignores a SetWriter to add an equal-priority writer", () => {
      const tree1 = tree.update(
        HashMap.of([
          deviceAStreamId,
          LinkedList.of(openWriterOp(deviceA, deviceB, 0)),
        ]),
      );
      expectIdentical(
        tree1.value.roots
          .get(tree1.value.rootKey)
          .getOrThrow()
          .shareData!.writers.get(deviceB)
          .getOrUndefined(),
        undefined,
      );
    });

    it("ignores a SetWriter to modify an equal-priority writer", () => {
      const tree1 = tree.update(
        HashMap.of([
          deviceAStreamId,
          LinkedList.of(openWriterOp(deviceA, deviceB, -1)).prepend(
            openWriterOp(deviceB, deviceB, -2),
          ),
        ]),
      );
      expectDeepEqual(
        tree1.value.roots
          .get(tree1.value.rootKey)
          .getOrThrow()
          .shareData!.writers.get(deviceB)
          .getOrThrow(),
        new PriorityStatus({
          status: "open",
          priority: -1,
        }),
      );
    });
  });

  describe("tree manipulation", () => {
    const nodeA = NodeId.create("a");
    const nodeB = NodeId.create("b");

    it("starts with the shared node", () => {
      const rootKey = new NodeKey({shareId, nodeId: shareId.id});
      expectPreludeEqual(tree.value.rootKey, rootKey);
      expectPreludeEqual(
        tree.value.roots,
        HashMap.of([
          rootKey,
          new SharedNode({
            id: shareId.id,
            shareId: shareId,
            shareData: new ShareData({
              writers: HashMap.of([
                deviceA,
                new PriorityStatus({priority: 0, status: "open"}),
              ]),
            }),
            children: HashMap.of(),
          }),
        ]),
      );
    });

    describe("create node", () => {
      it("creates a node as a new root", () => {
        const tree1 = tree.update(
          HashMap.of([
            deviceAStreamId,
            LinkedList.of(createNodeOp(deviceA, nodeA)),
          ]),
        );
        expectPreludeEqual(
          tree1.value.roots.get(tree1.value.rootKey).getOrThrow().children,
          HashMap.of(),
        );
        expectPreludeEqual(
          tree1.value.roots
            .get(new NodeKey({shareId, nodeId: nodeA}))
            .getOrThrow(),
          new SharedNode({
            shareId,
            id: nodeA,
            shareData: undefined,
            children: HashMap.of(),
          }),
        );
      });

      it("creates a shared node", () => {
        const sharedNodeId = NodeId.create("child shared");
        const shareId = new ShareId({id: sharedNodeId, creator: deviceA});
        const tree1 = tree.update(
          HashMap.of([
            deviceAStreamId,
            LinkedList.of({
              timestamp: clock.now(),
              device: deviceA,
              type: "create node",
              node: sharedNodeId,
              shareId: shareId,
            }),
          ]),
        );
        expectPreludeEqual(
          tree1.value.roots
            .get(new NodeKey({shareId, nodeId: sharedNodeId}))
            .getOrThrow().shareData!.writers,
          HashMap.of([
            deviceA,
            new PriorityStatus({priority: 0, status: "open"}),
          ]),
        );
      });

      it("does nothing if already preesnt", () => {
        const head1 = LinkedList.of(createNodeOp(deviceA, nodeA));
        const tree1 = tree.update(HashMap.of([deviceAStreamId, head1]));
        const head2 = head1.prepend(createNodeOp(deviceA, nodeA));
        const tree2 = tree1.update(HashMap.of([deviceAStreamId, head2]));
        expectIdentical(tree2.value, tree1.value);
      });
    });

    describe("set parent", () => {
      it("moves an existing root into a parent if both present", () => {
        const tree1 = tree.update(
          HashMap.of([
            deviceAStreamId,
            LinkedList.of(
              createNodeOp(deviceA, nodeA),
              setParentOp(deviceA, nodeA, shareId.id, 0),
            ).reverse(),
          ]),
        );
        expectIdentical(
          tree1.value.roots
            .single()
            .getOrThrow()[1]
            .children.containsKey(nodeA),
          true,
        );
      });

      it("moves an existing node to be a root if parent is missing", () => {
        const tree1 = tree.update(
          HashMap.of([
            deviceAStreamId,
            LinkedList.of(
              createNodeOp(deviceA, nodeA),
              setParentOp(deviceA, nodeA, shareId.id, 0),
              setParentOp(deviceA, nodeA, nodeB, 0),
            ).reverse(),
          ]),
        );
        expectIdentical(
          tree1.value.roots.containsKey(new NodeKey({shareId, nodeId: nodeA})),
          true,
        );
      });

      // This is interesting because it fails if we return after just removing
      // the direct child.
      it("moves a child into a sibling of the child", () => {
        const tree1 = tree.update(
          HashMap.of([
            deviceAStreamId,
            LinkedList.of(
              createNodeOp(deviceA, nodeA),
              setParentOp(deviceA, nodeA, shareId.id, 0),
              createNodeOp(deviceA, nodeB),
              setParentOp(deviceA, nodeB, shareId.id, 1),
              setParentOp(deviceA, nodeA, nodeB, 0),
            ).reverse(),
          ]),
        );
        const root = tree1.value.roots.single().getOrThrow()[1];
        expectIdentical(root.id, shareId.id);
        const rootChild = root.children.single().getOrThrow()[1].node;
        expectIdentical(rootChild.id, nodeB);
        const rootChildChild = rootChild.children.single().getOrThrow()[1].node;
        expectIdentical(rootChildChild.id, nodeA);
      });

      it("does nothing if not preseent", () => {
        const tree1 = tree.update(
          HashMap.of([
            deviceAStreamId,
            LinkedList.of(setParentOp(deviceA, nodeA, shareId.id, 0)).reverse(),
          ]),
        );
        expectIdentical(tree1.value, tree.value);
      });

      it("avoids a cycle", () => {
        const tree1 = tree.update(
          HashMap.of(
            [
              deviceAStreamId,
              LinkedList.of(
                createNodeOp(deviceA, nodeA),
                setParentOp(deviceA, nodeA, shareId.id, 0),
                createNodeOp(deviceA, nodeB),
                setParentOp(deviceA, nodeB, shareId.id, 1),
                setParentOp(deviceA, nodeA, nodeB, 0),
              ).reverse(),
            ],
            // [
            //   deviceBStreamId,
            //   LinkedList.of(setParentOp(deviceA, nodeB, nodeA, 0)).reverse(),
            // ],
          ),
        );
        const root = tree1.value.roots.single().getOrThrow()[1];
        expectIdentical(root.id, shareId.id);
        const rootChild = root.children.single().getOrThrow()[1].node;
        expectIdentical(rootChild.id, nodeB);
        const rootChildChild = rootChild.children.single().getOrThrow()[1].node;
        expectIdentical(rootChildChild.id, nodeA);
      });
    });
  });

  it("nested shares", () => {
    const shareA = new ShareId({creator: deviceA, id: NodeId.create("a root")});
    const shareShared = new ShareId({
      creator: deviceB,
      id: NodeId.create("shared"),
    });
    const nodeA = NodeId.create("nodeA");
    const treeA = createPermissionedTree(shareA);
    const nodeShared = NodeId.create("nodeShared");

    // Make the op in the shared node first, so that the tree has to handle
    // getting an op before it knows where to put it.
    const opBShared00 = asType<AppliedOp["op"]>({
      timestamp: clock.now(),
      device: deviceB,
      type: "create node",
      node: nodeShared,
      shareId: shareShared,
    });
    const opBShared0 = createNodeOp(deviceB, nodeA);
    const opBShared1 = setParentOp(deviceB, nodeA, nodeShared, 0);
    const opAA0 = asType<AppliedOp["op"]>({
      timestamp: clock.now(),
      device: deviceA,
      type: "create node",
      node: nodeShared,
      shareId: shareShared,
    });
    const opAA1 = asType<AppliedOp["op"]>({
      timestamp: clock.now(),
      device: deviceA,
      type: "set parent",
      node: nodeShared,
      shareId: Option.some(shareShared),
      parent: shareA.id,
      position: 0,
    });

    const treeA1 = treeA.update(
      HashMap.of(
        [
          new StreamId({deviceId: deviceA, shareId: shareA}),
          LinkedList.of(opAA0, opAA1).reverse(),
        ],
        [
          new StreamId({deviceId: deviceB, shareId: shareShared}),
          LinkedList.of(opBShared00, opBShared0, opBShared1).reverse(),
        ],
      ),
    );

    expectPreludeEqual(
      treeA1.desiredHeads(treeA1.value),
      HashMap.of(
        [new StreamId({deviceId: deviceA, shareId: shareA}), "open" as const],
        [
          new StreamId({deviceId: deviceB, shareId: shareShared}),
          "open" as const,
        ],
      ),
    );
    // expectPreludeEqual(
    //   treeA1.value.roots.get(shareA).getOrThrow().nodes,
    //   HashMap.of([
    //     nodeShared,
    //     new NodeInfo({
    //       parent: rootNodeId,
    //       position: 0,
    //       shareId: shareShared,
    //     }),
    //   ]),
    // );
    // expectPreludeEqual(
    //   treeA1.value.roots.get(shareShared).getOrThrow().nodes,
    //   HashMap.of([
    //     nodeA,
    //     new NodeInfo({
    //       parent: rootNodeId,
    //       position: 0,
    //       shareId: undefined,
    //     }),
    //   ]),
    // );
  });
});
