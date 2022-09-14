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
import {ConsLinkedList, HashMap, LinkedList, Option} from "prelude-ts";

describe("NestedPermissionedTree", () => {
  const clock = new CountingClock();
  const deviceA = DeviceId.create("A");
  const deviceB = DeviceId.create("B");

  function opsList(...ops: AppliedOp["op"][]): ConsLinkedList<AppliedOp["op"]> {
    return LinkedList.ofIterable(ops).reverse() as ConsLinkedList<
      AppliedOp["op"]
    >;
  }
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
  function setParentOp(args: {
    device: DeviceId;
    nodeId: NodeId;
    nodeShareId?: ShareId;
    parentNodeId: NodeId;
    position?: number;
  }): AppliedOp["op"] {
    return {
      timestamp: clock.now(),
      device: args.device,
      type: "set parent",
      nodeId: args.nodeId,
      nodeShareId: args.nodeShareId
        ? Option.some(args.nodeShareId)
        : Option.none(),
      parentNodeId: args.parentNodeId,
      position: args.position === undefined ? 0 : args.position,
    };
  }

  describe("permissions", () => {
    const shareId = new ShareId({creator: deviceA, id: NodeId.create("share")});
    const deviceAStreamId = new StreamId({deviceId: deviceA, shareId});

    it("adds a lower-ranked writer", () => {
      const tree = createPermissionedTree(shareId);
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

    // This is what happens when we share a node with a device before adding the
    // node to the device's tree.
    it("creates shared node when setting writer on an unknown node", () => {
      const shareA = new ShareId({
        creator: deviceA,
        id: NodeId.create("a root"),
      });
      const tree = createPermissionedTree(shareA);
      const tree1 = tree.update(
        HashMap.of(
          [
            new StreamId(new StreamId({deviceId: deviceA, shareId})),
            LinkedList.of(openWriterOp(deviceA, deviceB, -1)),
          ],
          [
            new StreamId(new StreamId({deviceId: deviceA, shareId: shareA})),
            LinkedList.of(
              // Make this after openWriterOp, to force the tree to handle the
              // writer when it doesn't know about the shared node yet.
              setParentOp({
                nodeId: shareId.id,
                nodeShareId: shareId,
                parentNodeId: shareA.id,
                device: deviceA,
              }),
            ),
          ],
        ),
      );
      expectPreludeEqual(
        tree1.value
          .nodeForNodeKey(new NodeKey({shareId, nodeId: shareId.id}))
          .getOrThrow()
          .shareData!.writers.get(deviceB),
        Option.some(new PriorityStatus({status: "open", priority: -1})),
      );
    });

    it("ignores a SetWriter to add an equal-priority writer", () => {
      const tree = createPermissionedTree(shareId);
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
      const tree = createPermissionedTree(shareId);
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
    const shareId = new ShareId({creator: deviceA, id: NodeId.create("share")});
    const deviceAStreamId = new StreamId({deviceId: deviceA, shareId});
    const tree = createPermissionedTree(shareId);

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

    describe("set parent", () => {
      const nodeA = NodeId.create("a");
      const nodeB = NodeId.create("b");

      it("unknown child, unknown parent => add parent as root", () => {
        const tree1 = tree.update(
          HashMap.of([
            deviceAStreamId,
            opsList(
              setParentOp({
                device: deviceA,
                nodeId: nodeA,
                parentNodeId: nodeB,
              }),
            ),
          ]),
        );
        expectIdentical(tree1.value.root().children.isEmpty(), true);
        const parent = tree1.value.roots
          .get(
            new NodeKey({shareId: tree.value.rootKey.shareId, nodeId: nodeB}),
          )
          .getOrThrow();
        expectIdentical(parent.children.containsKey(nodeA), true);
      });

      it("unknown child, known parent => add child", () => {
        const tree1 = tree.update(
          HashMap.of([
            deviceAStreamId,
            opsList(
              setParentOp({
                device: deviceA,
                nodeId: nodeA,
                parentNodeId: tree.value.rootKey.nodeId,
              }),
            ),
          ]),
        );
        expectIdentical(tree1.value.root().children.containsKey(nodeA), true);
      });

      it("known child, unknown parent => add parent as root, move child", () => {
        const tree1 = tree.update(
          HashMap.of([
            deviceAStreamId,
            opsList(
              setParentOp({
                device: deviceA,
                nodeId: nodeA,
                parentNodeId: tree.value.rootKey.nodeId,
              }),
              setParentOp({
                device: deviceA,
                nodeId: nodeA,
                parentNodeId: nodeB,
              }),
            ),
          ]),
        );
        expectIdentical(tree1.value.root().children.isEmpty(), true);
        const parent = tree1.value.roots
          .get(
            new NodeKey({shareId: tree.value.rootKey.shareId, nodeId: nodeB}),
          )
          .getOrThrow();
        expectIdentical(parent.children.containsKey(nodeA), true);
      });

      it("known child, known parent => move child", () => {
        const tree1 = tree.update(
          HashMap.of([
            deviceAStreamId,
            opsList(
              setParentOp({
                device: deviceA,
                nodeId: nodeA,
                parentNodeId: tree.value.rootKey.nodeId,
              }),
              setParentOp({
                device: deviceA,
                nodeId: nodeB,
                parentNodeId: tree.value.rootKey.nodeId,
              }),
              setParentOp({
                device: deviceA,
                nodeId: nodeA,
                parentNodeId: nodeB,
              }),
            ),
          ]),
        );
        const root = tree1.value.roots.single().getOrThrow()[1];
        expectIdentical(root.id, shareId.id);
        const rootChild = root.children.single().getOrThrow()[1].node;
        expectIdentical(rootChild.id, nodeB);
        const rootChildChild = rootChild.children.single().getOrThrow()[1].node;
        expectIdentical(rootChildChild.id, nodeA);
      });

      it("known child, unchanged known parent => adjust position", () => {
        const tree1 = tree.update(
          HashMap.of([
            deviceAStreamId,
            opsList(
              setParentOp({
                device: deviceA,
                nodeId: nodeA,
                parentNodeId: tree.value.rootKey.nodeId,
                position: 0,
              }),
              setParentOp({
                device: deviceA,
                nodeId: nodeA,
                parentNodeId: tree.value.rootKey.nodeId,
                position: 1,
              }),
            ),
          ]),
        );
        const root = tree1.value.roots.single().getOrThrow()[1];
        expectIdentical(root.id, shareId.id);
        const rootChildInfo = root.children.single().getOrThrow();
        expectIdentical(rootChildInfo[1].node.id, nodeA);
        expectIdentical(rootChildInfo[1].position, 1);
      });

      it("known root child, known parent => move child, remove root", () => {
        const tree1 = tree.update(
          HashMap.of([
            deviceAStreamId,
            opsList(
              setParentOp({
                device: deviceA,
                nodeId: nodeA,
                parentNodeId: nodeB,
              }),
              setParentOp({
                device: deviceA,
                nodeId: nodeB,
                parentNodeId: tree.value.rootKey.nodeId,
              }),
            ),
          ]),
        );
        const root = tree1.value.roots.single().getOrThrow()[1];
        expectIdentical(root.id, shareId.id);
        const rootChild = root.children.single().getOrThrow()[1].node;
        expectIdentical(rootChild.id, nodeB);
        const rootChildChild = rootChild.children.single().getOrThrow()[1].node;
        expectIdentical(rootChildChild.id, nodeA);
      });

      it("avoids a cycle", () => {
        const tree1 = tree.update(
          HashMap.of([
            deviceAStreamId,
            opsList(
              setParentOp({
                device: deviceA,
                nodeId: nodeA,
                parentNodeId: tree.value.rootKey.nodeId,
              }),
              setParentOp({
                device: deviceA,
                nodeId: nodeB,
                parentNodeId: tree.value.rootKey.nodeId,
              }),
              setParentOp({
                device: deviceA,
                nodeId: nodeA,
                parentNodeId: nodeB,
              }),
              setParentOp({
                device: deviceA,
                nodeId: nodeB,
                parentNodeId: nodeA,
              }),
            ),
          ]),
        );
        const root = tree1.value.roots.single().getOrThrow()[1];
        expectIdentical(root.id, shareId.id);
        const rootChild = root.children.single().getOrThrow()[1].node;
        expectIdentical(rootChild.id, nodeB);
        const rootChildChild = rootChild.children.single().getOrThrow()[1].node;
        expectIdentical(rootChildChild.id, nodeA);
        expectIdentical(rootChildChild.children.length(), 0);
      });
    });
  });

  describe("nested shares", () => {
    it("multiple writers", () => {
      const shareA = new ShareId({
        creator: deviceA,
        id: NodeId.create("a root"),
      });
      const shareShared = new ShareId({
        creator: deviceB,
        id: NodeId.create("shared"),
      });
      const nodeA = NodeId.create("nodeA");
      const treeA = createPermissionedTree(shareA);

      // Make the op in the shared node first, so that the tree has to handle
      // getting an op before it knows where to put it.
      const opBShared0 = setParentOp({
        device: deviceB,
        nodeId: nodeA,
        parentNodeId: shareShared.id,
      });
      const opAA0 = setParentOp({
        device: deviceA,
        nodeId: shareShared.id,
        nodeShareId: shareShared,
        parentNodeId: shareA.id,
      });

      const tree1 = treeA.update(
        HashMap.of(
          [new StreamId({deviceId: deviceA, shareId: shareA}), opsList(opAA0)],
          [
            new StreamId({deviceId: deviceB, shareId: shareShared}),
            opsList(opBShared0),
          ],
        ),
      );

      expectPreludeEqual(
        tree1.desiredHeads(tree1.value),
        HashMap.of(
          [new StreamId({deviceId: deviceA, shareId: shareA}), "open" as const],
          [
            new StreamId({deviceId: deviceB, shareId: shareShared}),
            "open" as const,
          ],
        ),
      );
      expectIdentical(tree1.value.roots.length(), 1);
      expectIdentical(tree1.value.root().id, shareA.id);
      const root = tree1.value.roots.single().getOrThrow()[1];
      expectIdentical(root.id, shareA.id);
      const rootChild = root.children.single().getOrThrow()[1].node;
      expectIdentical(rootChild.id, shareShared.id);
      const rootChildChild = rootChild.children.single().getOrThrow()[1].node;
      expectIdentical(rootChildChild.id, nodeA);
      expectIdentical(rootChildChild.children.length(), 0);
    });

    it("treats edges to multiple parents separately", () => {
      const deviceId = DeviceId.create("device");
      const shareRoot = new ShareId({
        creator: deviceId,
        id: NodeId.create("root"),
      });
      const shareA = new ShareId({creator: deviceId, id: NodeId.create("a")});
      const shareB = new ShareId({creator: deviceId, id: NodeId.create("b")});
      const shareC = new ShareId({creator: deviceId, id: NodeId.create("c")});
      const tree = createPermissionedTree(shareRoot);
      const opAInRoot = setParentOp({
        device: deviceId,
        nodeId: shareA.id,
        nodeShareId: shareA,
        parentNodeId: shareRoot.id,
        position: 0,
      });
      const opBInRoot = setParentOp({
        device: deviceId,
        nodeId: shareB.id,
        nodeShareId: shareB,
        parentNodeId: shareRoot.id,
        position: 0,
      });
      const opCInA = setParentOp({
        device: deviceId,
        nodeId: shareC.id,
        nodeShareId: shareC,
        parentNodeId: shareA.id,
      });
      const opCInB = setParentOp({
        device: deviceId,
        nodeId: shareC.id,
        nodeShareId: shareC,
        parentNodeId: shareB.id,
      });

      // Adding shared node C into shared node B should not remove it from
      // shared node A.
      const tree1 = tree.update(
        HashMap.of(
          [
            new StreamId({deviceId, shareId: shareRoot}),
            opsList(opAInRoot, opBInRoot),
          ],
          [new StreamId({deviceId, shareId: shareA}), opsList(opCInA)],
          [new StreamId({deviceId, shareId: shareB}), opsList(opCInB)],
        ),
      );
      expectIdentical(
        tree1.value
          .root()
          .nodeForNodeKey(new NodeKey({shareId: shareA, nodeId: shareA.id}))
          .getOrThrow()
          .nodeForNodeKey(new NodeKey({shareId: shareC, nodeId: shareC.id}))
          .isSome(),
        true,
      );
      expectIdentical(
        tree1.value
          .root()
          .nodeForNodeKey(new NodeKey({shareId: shareB, nodeId: shareB.id}))
          .getOrThrow()
          .nodeForNodeKey(new NodeKey({shareId: shareC, nodeId: shareC.id}))
          .isSome(),
        true,
      );
    });

    it("reports writers for all roots", () => {
      const shareA = new ShareId({
        creator: deviceA,
        id: NodeId.create("a root"),
      });
      const shareShared = new ShareId({
        creator: deviceB,
        id: NodeId.create("shared"),
      });
      const tree = createPermissionedTree(shareA);

      // Create a shared node (with a different writer) inside the root share,
      // but not inside the root node, so that it forms a different root.
      const op = setParentOp({
        device: deviceA,
        nodeId: shareShared.id,
        nodeShareId: shareShared,
        parentNodeId: NodeId.create("not in share A"),
      });

      const tree1 = tree.update(
        HashMap.of([
          new StreamId({deviceId: deviceA, shareId: shareA}),
          opsList(op),
        ]),
      );

      expectPreludeEqual(
        tree1.desiredHeads(tree1.value),
        HashMap.of(
          [new StreamId({deviceId: deviceA, shareId: shareA}), "open" as const],
          [
            new StreamId({deviceId: deviceB, shareId: shareShared}),
            "open" as const,
          ],
        ),
      );
    });
  });
});
