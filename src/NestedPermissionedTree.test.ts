import {
  AppliedOp,
  createPermissionedTree,
  DeviceId,
  NodeId,
  NodeKey,
  WriterInfo,
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
  function openWriterOp(writer: ShareId, priority: number): AppliedOp["op"] {
    return {
      timestamp: clock.now(),
      type: "set writer",
      writer: writer,
      priority: priority,
      status: "open",
    };
  }
  function setChildOp(args: {
    nodeId: NodeId;
    nodeShareId?: ShareId;
    parentNodeId: NodeId;
    position?: number;
  }): AppliedOp["op"] {
    return {
      timestamp: clock.now(),
      type: "set child",
      childNodeId: args.nodeId,
      childShareId: args.nodeShareId
        ? Option.some(args.nodeShareId)
        : Option.none(),
      nodeId: args.parentNodeId,
      position: args.position === undefined ? 0 : args.position,
    };
  }

  describe("permissions", () => {
    const shareId = new ShareId({creator: deviceA, id: NodeId.create("share")});
    const shareIdOther = new ShareId({
      creator: deviceB,
      id: NodeId.create("shareOther"),
    });

    it("starts with one desired head", () => {
      const tree = createPermissionedTree(shareId);
      expectPreludeEqual(
        tree.desiredHeads(tree.value),
        HashMap.of(
          [
            new StreamId({
              deviceId: shareId.creator,
              shareId: shareId,
              type: "shared node",
            }),
            "open" as const,
          ],
          [
            new StreamId({
              deviceId: shareId.creator,
              shareId: shareId,
              type: "share data",
            }),
            "open" as const,
          ],
        ),
      );
    });

    it("adds a lower-ranked writer", () => {
      const tree = createPermissionedTree(shareId);
      const tree1 = tree.update(
        HashMap.of([
          new StreamId({deviceId: deviceA, shareId, type: "share data"}),
          LinkedList.of(openWriterOp(shareIdOther, -1)),
        ]),
      );
      expectDeepEqual(
        tree1.value.roots
          .get(tree1.value.rootKey)
          .getOrThrow()
          .shareData!.writers.get(shareIdOther)
          .getOrUndefined(),
        new WriterInfo({
          writer: ShareData.create(shareIdOther),
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
            new StreamId({deviceId: deviceA, shareId, type: "share data"}),
            LinkedList.of(openWriterOp(shareIdOther, -1)),
          ],
          [
            new StreamId({
              deviceId: deviceA,
              shareId: shareA,
              type: "shared node",
            }),
            LinkedList.of(
              // Make this after openWriterOp, to force the tree to handle the
              // writer when it doesn't know about the shared node yet.
              setChildOp({
                nodeId: shareId.id,
                nodeShareId: shareId,
                parentNodeId: shareA.id,
              }),
            ),
          ],
        ),
      );
      expectPreludeEqual(
        tree1.value
          .nodeForNodeKey(new NodeKey({shareId, nodeId: shareId.id}))
          .getOrThrow()
          .shareData!.writers.get(shareIdOther),
        Option.some(
          new WriterInfo({
            writer: ShareData.create(shareIdOther),
            status: "open",
          }),
        ),
      );
    });

    it("ignores a SetWriter to add an equal-priority writer", () => {
      const tree = createPermissionedTree(shareId);
      const tree1 = tree.update(
        HashMap.of([
          new StreamId({deviceId: deviceA, shareId, type: "shared node"}),
          LinkedList.of(openWriterOp(shareIdOther, 0)),
        ]),
      );
      expectIdentical(
        tree1.value.roots
          .get(tree1.value.rootKey)
          .getOrThrow()
          .shareData!.writers.get(shareIdOther)
          .getOrUndefined(),
        undefined,
      );
    });

    // #WriterPriority
    // it("ignores a SetWriter to modify an equal-priority writer", () => {
    //   const tree = createPermissionedTree(shareId);
    //   const tree1 = tree.update(
    //     HashMap.of([
    //       new StreamId({deviceId: deviceA, shareId, type: "share data"}),
    //       LinkedList.of(openWriterOp(deviceA, deviceB, -1)).prepend(
    //         openWriterOp(deviceB, deviceB, -2),
    //       ),
    //     ]),
    //   );
    //   expectDeepEqual(
    //     tree1.value.roots
    //       .get(tree1.value.rootKey)
    //       .getOrThrow()
    //       .shareData!.writers.get(deviceB)
    //       .getOrThrow(),
    //     new WriterInfo({
    //       status: "open",
    //       priority: -1,
    //     }),
    //   );
    // });

    it("shared node inherits writers from parent shared node", () => {
      const tree = createPermissionedTree(shareId);
      const tree1 = tree.update(
        HashMap.of([
          new StreamId({deviceId: deviceA, shareId, type: "share data"}),
          LinkedList.of(openWriterOp(shareIdOther, -1)),
        ]),
      );
      expectPreludeEqual(
        tree1.desiredHeads(tree1.value),
        HashMap.of(
          [
            new StreamId({
              deviceId: shareId.creator,
              shareId: shareId,
              type: "shared node",
            }),
            "open" as const,
          ],
          [
            new StreamId({
              deviceId: shareId.creator,
              shareId: shareId,
              type: "share data",
            }),
            "open" as const,
          ],
          [
            new StreamId({
              deviceId: shareIdOther.creator,
              shareId: shareIdOther,
              type: "shared node",
            }),
            "open" as const,
          ],
          [
            new StreamId({
              deviceId: shareIdOther.creator,
              shareId: shareIdOther,
              type: "share data",
            }),
            "open" as const,
          ],
          [
            new StreamId({
              deviceId: shareIdOther.creator,
              shareId: shareId,
              type: "shared node",
            }),
            "open" as const,
          ],
          [
            new StreamId({
              deviceId: shareIdOther.creator,
              shareId: shareId,
              type: "share data",
            }),
            "open" as const,
          ],
        ),
      );
    });
  });

  describe("tree manipulation", () => {
    const shareId = new ShareId({creator: deviceA, id: NodeId.create("share")});
    const deviceAStreamId = new StreamId({
      deviceId: deviceA,
      shareId,
      type: "shared node",
    });
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
            shareId,
            shareData: new ShareData({
              shareId,
              writers: HashMap.of(),
            }),
            children: HashMap.of(),
          }),
        ]),
      );
    });

    describe("set child", () => {
      const nodeA = NodeId.create("a");
      const nodeB = NodeId.create("b");

      it("unknown child, unknown parent => add parent as root", () => {
        const tree1 = tree.update(
          HashMap.of([
            deviceAStreamId,
            opsList(
              setChildOp({
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
              setChildOp({
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
              setChildOp({
                nodeId: nodeA,
                parentNodeId: tree.value.rootKey.nodeId,
              }),
              setChildOp({
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
              setChildOp({
                nodeId: nodeA,
                parentNodeId: tree.value.rootKey.nodeId,
              }),
              setChildOp({
                nodeId: nodeB,
                parentNodeId: tree.value.rootKey.nodeId,
              }),
              setChildOp({
                nodeId: nodeA,
                parentNodeId: nodeB,
              }),
            ),
          ]),
        );
        const root = tree1.value.roots.single().getOrThrow()[1];
        expectIdentical(root.id, shareId.id);
        const rootChild = root.children.single().getOrThrow()[1].child;
        expectIdentical(rootChild.id, nodeB);
        const rootChildChild = rootChild.children
          .single()
          .getOrThrow()[1].child;
        expectIdentical(rootChildChild.id, nodeA);
      });

      it("known child, unchanged known parent => adjust position", () => {
        const tree1 = tree.update(
          HashMap.of([
            deviceAStreamId,
            opsList(
              setChildOp({
                nodeId: nodeA,
                parentNodeId: tree.value.rootKey.nodeId,
                position: 0,
              }),
              setChildOp({
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
        expectIdentical(rootChildInfo[1].child.id, nodeA);
        expectIdentical(rootChildInfo[1].position, 1);
      });

      it("known root child, known parent => move child, remove root", () => {
        const tree1 = tree.update(
          HashMap.of([
            deviceAStreamId,
            opsList(
              setChildOp({
                nodeId: nodeA,
                parentNodeId: nodeB,
              }),
              setChildOp({
                nodeId: nodeB,
                parentNodeId: tree.value.rootKey.nodeId,
              }),
            ),
          ]),
        );
        const root = tree1.value.roots.single().getOrThrow()[1];
        expectIdentical(root.id, shareId.id);
        const rootChild = root.children.single().getOrThrow()[1].child;
        expectIdentical(rootChild.id, nodeB);
        const rootChildChild = rootChild.children
          .single()
          .getOrThrow()[1].child;
        expectIdentical(rootChildChild.id, nodeA);
      });

      it("avoids a cycle", () => {
        const tree1 = tree.update(
          HashMap.of([
            deviceAStreamId,
            opsList(
              setChildOp({
                nodeId: nodeA,
                parentNodeId: tree.value.rootKey.nodeId,
              }),
              setChildOp({
                nodeId: nodeB,
                parentNodeId: tree.value.rootKey.nodeId,
              }),
              setChildOp({
                nodeId: nodeA,
                parentNodeId: nodeB,
              }),
              setChildOp({
                nodeId: nodeB,
                parentNodeId: nodeA,
              }),
            ),
          ]),
        );
        const root = tree1.value.roots.single().getOrThrow()[1];
        expectIdentical(root.id, shareId.id);
        const rootChild = root.children.single().getOrThrow()[1].child;
        expectIdentical(rootChild.id, nodeB);
        const rootChildChild = rootChild.children
          .single()
          .getOrThrow()[1].child;
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
      const opBShared0 = setChildOp({
        nodeId: nodeA,
        parentNodeId: shareShared.id,
      });
      const opAA0 = setChildOp({
        nodeId: shareShared.id,
        nodeShareId: shareShared,
        parentNodeId: shareA.id,
      });

      const tree1 = treeA.update(
        HashMap.of(
          [
            new StreamId({
              deviceId: deviceA,
              shareId: shareA,
              type: "shared node",
            }),
            opsList(opAA0),
          ],
          [
            new StreamId({
              deviceId: deviceB,
              shareId: shareShared,
              type: "shared node",
            }),
            opsList(opBShared0),
          ],
        ),
      );

      expectPreludeEqual(
        tree1.desiredHeads(tree1.value),
        HashMap.of(
          [
            new StreamId({
              deviceId: deviceA,
              shareId: shareA,
              type: "shared node",
            }),
            "open" as const,
          ],
          [
            new StreamId({
              deviceId: deviceA,
              shareId: shareA,
              type: "share data",
            }),
            "open" as const,
          ],
          [
            new StreamId({
              deviceId: deviceB,
              shareId: shareShared,
              type: "shared node",
            }),
            "open" as const,
          ],
          [
            new StreamId({
              deviceId: deviceB,
              shareId: shareShared,
              type: "share data",
            }),
            "open" as const,
          ],
        ),
      );
      expectIdentical(tree1.value.roots.length(), 1);
      expectIdentical(tree1.value.root().id, shareA.id);
      const root = tree1.value.roots.single().getOrThrow()[1];
      expectIdentical(root.id, shareA.id);
      const rootChild = root.children.single().getOrThrow()[1].child;
      expectIdentical(rootChild.id, shareShared.id);
      const rootChildChild = rootChild.children.single().getOrThrow()[1].child;
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
      const opAInRoot = setChildOp({
        nodeId: shareA.id,
        nodeShareId: shareA,
        parentNodeId: shareRoot.id,
        position: 0,
      });
      const opBInRoot = setChildOp({
        nodeId: shareB.id,
        nodeShareId: shareB,
        parentNodeId: shareRoot.id,
        position: 0,
      });
      const opCInA = setChildOp({
        nodeId: shareC.id,
        nodeShareId: shareC,
        parentNodeId: shareA.id,
      });
      const opCInB = setChildOp({
        nodeId: shareC.id,
        nodeShareId: shareC,
        parentNodeId: shareB.id,
      });

      // Adding shared node C into shared node B should not remove it from
      // shared node A.
      const tree1 = tree.update(
        HashMap.of(
          [
            new StreamId({deviceId, shareId: shareRoot, type: "shared node"}),
            opsList(opAInRoot, opBInRoot),
          ],
          [
            new StreamId({deviceId, shareId: shareA, type: "shared node"}),
            opsList(opCInA),
          ],
          [
            new StreamId({deviceId, shareId: shareB, type: "shared node"}),
            opsList(opCInB),
          ],
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
      const op = setChildOp({
        nodeId: shareShared.id,
        nodeShareId: shareShared,
        parentNodeId: NodeId.create("not in share A"),
      });

      const tree1 = tree.update(
        HashMap.of([
          new StreamId({
            deviceId: deviceA,
            shareId: shareA,
            type: "shared node",
          }),
          opsList(op),
        ]),
      );

      expectPreludeEqual(
        tree1.desiredHeads(tree1.value),
        HashMap.of(
          [
            new StreamId({
              deviceId: deviceA,
              shareId: shareA,
              type: "shared node",
            }),
            "open" as const,
          ],
          [
            new StreamId({
              deviceId: deviceA,
              shareId: shareA,
              type: "share data",
            }),
            "open" as const,
          ],
          [
            new StreamId({
              deviceId: deviceB,
              shareId: shareShared,
              type: "shared node",
            }),
            "open" as const,
          ],
          [
            new StreamId({
              deviceId: deviceB,
              shareId: shareShared,
              type: "share data",
            }),
            "open" as const,
          ],
        ),
      );
    });
  });
});
