import {
  AppliedOp,
  createPermissionedTree,
  DeviceId,
  DownNode,
  EdgeId,
  NestedPermissionedTree,
  NodeId,
  NodeKey,
  Rank,
  StreamId,
  UpNode,
} from "./NestedPermissionedTree";
import {expectIdentical, expectPreludeEqual} from "./helper/Shared.testing";
import {ConsLinkedList, HashMap, HashSet, LinkedList, Option} from "prelude-ts";
import {CountingClock} from "./helper/Clock.testing";
import {OpList} from "./ControlledOpSet";

function opsList(...ops: AppliedOp["op"][]): ConsLinkedList<AppliedOp["op"]> {
  return LinkedList.ofIterable(ops).reverse() as ConsLinkedList<
    AppliedOp["op"]
  >;
}

function upKey(nodeId: NodeId): NodeKey {
  return new NodeKey({nodeId, type: "up"});
}

function downKey(nodeId: NodeId): NodeKey {
  return new NodeKey({nodeId, type: "down"});
}

function upNodeForNodeId(
  tree: NestedPermissionedTree,
  nodeId: NodeId,
): Option<UpNode> {
  return tree.value.nodeForNodeKey(
    new NodeKey({nodeId, type: "up"}),
  ) as Option<UpNode>;
}

function downNodeForNodeId(
  tree: NestedPermissionedTree,
  nodeId: NodeId,
): Option<DownNode> {
  return tree.value.nodeForNodeKey(
    new NodeKey({nodeId, type: "down"}),
  ) as Option<DownNode>;
}

describe("NestedPermissionedTree", () => {
  const clock = new CountingClock();

  function setEdge(
    parentId: NodeId,
    childId: NodeId,
    extras?: {
      edgeId?: EdgeId;
      streams?: HashMap<StreamId, OpList<AppliedOp>>;
    },
  ): AppliedOp["op"] {
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

  const deviceId = DeviceId.create("device");
  const tree = createPermissionedTree(deviceId);
  const rootId = new NodeId({creator: deviceId, rest: undefined});
  const parentId = new NodeId({creator: deviceId, rest: "parent"});
  const childId = new NodeId({creator: deviceId, rest: "child"});
  const rootUpStreamId = new StreamId({
    deviceId,
    nodeId: tree.value.rootNodeKey.nodeId,
    type: "up",
  });
  const rootDownStreamId = new StreamId({
    deviceId,
    nodeId: tree.value.rootNodeKey.nodeId,
    type: "down",
  });
  it("initial state", () => {
    expectPreludeEqual(
      tree.desiredHeads(tree.value),
      HashMap.of(
        [rootUpStreamId, "open" as const],
        [rootDownStreamId, "open" as const],
      ),
    );
  });

  it("up-streamed op", () => {
    const tree1 = tree.update(
      HashMap.of([rootUpStreamId, opsList(setEdge(parentId, rootId))]),
    );
    expectPreludeEqual(
      tree1.value.roots.keySet(),
      HashSet.of(tree1.value.rootNodeKey),
    );

    // The up edge exists.
    const edge = upNodeForNodeId(tree1, rootId)
      .getOrThrow()
      .parents.get(EdgeId.create("edge"))
      .getOrThrow();
    expectPreludeEqual(edge.parent.nodeId, parentId);

    expectPreludeEqual(
      tree1.desiredHeads(tree1.value),
      HashMap.of(
        [rootUpStreamId, "open" as const],
        [rootDownStreamId, "open" as const],
        [
          new StreamId({deviceId, nodeId: parentId, type: "up"}),
          "open" as const,
        ],
      ),
    );
  });

  it("down-streamed op", () => {
    const tree1 = tree.update(
      HashMap.of([rootDownStreamId, opsList(setEdge(rootId, childId))]),
    );

    expectPreludeEqual(
      tree1.value.roots.keySet(),
      HashSet.of(tree1.value.rootNodeKey, downKey(childId)),
    );

    // The down edge should not have been created because the up-child does not
    // list the parent, which is because we didn't provide the op in the right
    // up stream.
    expectIdentical(
      downNodeForNodeId(tree1, rootId).getOrThrow().children.isEmpty(),
      true,
    );

    expectPreludeEqual(
      tree1.desiredHeads(tree1.value),
      HashMap.of(
        [rootUpStreamId, "open" as const],
        [rootDownStreamId, "open" as const],
        // We must subscribe to this so that if the stream is available we read
        // it and find the up version of the SetEdge.
        [
          new StreamId({deviceId, nodeId: childId, type: "up"}),
          "open" as const,
        ],
        [
          new StreamId({deviceId, nodeId: childId, type: "down"}),
          "open" as const,
        ],
      ),
    );
  });

  it("up-streamed and down-streamed op", () => {
    const op = setEdge(rootId, childId);
    const tree1 = tree.update(
      HashMap.of(
        [new StreamId(rootDownStreamId), opsList(op)],
        [new StreamId({deviceId, nodeId: childId, type: "up"}), opsList(op)],
      ),
    );

    expectPreludeEqual(
      tree1.value.roots.keySet(),
      HashSet.of(tree1.value.rootNodeKey),
    );

    const edge = upNodeForNodeId(tree1, childId)
      .getOrThrow()
      .parents.get(EdgeId.create("edge"))
      .getOrThrow();
    expectPreludeEqual(edge.parent.nodeId, rootId);

    expectIdentical(
      downNodeForNodeId(tree1, rootId)
        .getOrThrow()
        .children.get(childId)
        .isSome(),
      true,
    );

    expectPreludeEqual(
      tree1.desiredHeads(tree1.value),
      HashMap.of(
        [rootUpStreamId, "open" as const],
        [rootDownStreamId, "open" as const],
        [
          new StreamId({deviceId, nodeId: childId, type: "up"}),
          "open" as const,
        ],
        [
          new StreamId({deviceId, nodeId: childId, type: "down"}),
          "open" as const,
        ],
      ),
    );
  });

  it("move a child to a parent with a different creator", () => {
    const otherDeviceId = DeviceId.create("other device");
    const newParentId = new NodeId({
      creator: otherDeviceId,
      rest: "other parent",
    });
    const op = setEdge(newParentId, rootId);

    // This is a bit naughty: would be better to use .update() but that checks
    // that the actual heads match the desired heads when we are done, which
    // requires that we move the node into a parent where deviceId can still
    // write to it. Since we want to test the case where deviceId can't write to
    // the node, we'd need to create a somewhat complicated sharing setup.
    const tree1 = tree.updateWithOneOp(op, HashSet.of(rootUpStreamId));

    expectPreludeEqual(
      tree1.value.roots.keySet(),
      HashSet.of(tree1.value.rootNodeKey),
    );

    const edge = upNodeForNodeId(tree1, rootId)
      .getOrThrow()
      .parents.get(EdgeId.create("edge"))
      .getOrThrow();
    expectPreludeEqual(edge.parent.nodeId, newParentId);

    // Since op doesn't list any contributions and deviceId can't write to the
    // root after the move (the default "edge" is gone, and only otherDeviceId
    // can write to the new parent), deviceId's stream gets closed with no
    // contributions -- ie, removed.
    expectPreludeEqual(
      tree1.desiredHeads(tree1.value),
      HashMap.of<StreamId, "open" | OpList<AppliedOp>>(
        [
          new StreamId({
            deviceId: otherDeviceId,
            nodeId: newParentId,
            type: "up",
          }),
          "open",
        ],
        [
          new StreamId({
            deviceId: otherDeviceId,
            nodeId: rootId,
            type: "up",
          }),
          "open",
        ],
        [
          new StreamId({
            deviceId: otherDeviceId,
            nodeId: rootId,
            type: "down",
          }),
          "open",
        ],
      ),
    );
  });

  it("move a child to a new parent", () => {
    const otherDeviceId = DeviceId.create("other device");
    const newParentId = new NodeId({creator: otherDeviceId, rest: "junk"});

    const tree1 = tree
      .updateWithOneOp(
        setEdge(parentId, rootId),
        HashSet.of(
          new StreamId({deviceId, nodeId: parentId, type: "down"}),
          new StreamId({deviceId, nodeId: rootId, type: "up"}),
        ),
      )
      .updateWithOneOp(
        setEdge(newParentId, rootId),
        HashSet.of(new StreamId({deviceId, nodeId: rootId, type: "up"})),
      );

    expectPreludeEqual(
      tree1.value.roots.keySet(),
      HashSet.of(tree1.value.rootNodeKey, downKey(parentId)),
    );

    const edge = upNodeForNodeId(tree1, rootId)
      .getOrThrow()
      .parents.get(EdgeId.create("edge"))
      .getOrThrow();
    // The child should list the new parent.
    expectPreludeEqual(edge.parent.nodeId, newParentId);

    // The child should be removed from the parent's list of children.
    expectIdentical(
      downNodeForNodeId(tree1, parentId).getOrThrow().children.isEmpty(),
      true,
    );

    expectPreludeEqual(
      tree1.desiredHeads(tree1.value),
      HashMap.of(
        [
          new StreamId({deviceId: otherDeviceId, nodeId: rootId, type: "up"}),
          "open" as const,
        ],
        [
          new StreamId({
            deviceId: otherDeviceId,
            nodeId: rootId,
            type: "down",
          }),
          "open" as const,
        ],
        [
          new StreamId({deviceId, nodeId: parentId, type: "up"}),
          "open" as const,
        ],
        [
          new StreamId({
            deviceId,
            nodeId: parentId,
            type: "down",
          }),
          "open" as const,
        ],
        [
          new StreamId({
            deviceId: otherDeviceId,
            nodeId: newParentId,
            type: "up",
          }),
          "open" as const,
        ],
      ),
    );
  });

  it("retains an old parent when the child gets a new parent", () => {
    const junkId = new NodeId({creator: deviceId, rest: "junk"});

    const tree1 = tree.updateWithOneOp(
      setEdge(parentId, childId),
      HashSet.of(new StreamId({deviceId, nodeId: childId, type: "up"})),
    );
    const tree2 = tree1.updateWithOneOp(
      {
        timestamp: clock.now(),
        type: "set edge",
        edgeId: EdgeId.create("edge"),
        parentId: junkId,
        childId: childId,
        rank: Rank.create(0),
        streams: HashMap.of(),
      },
      HashSet.of(new StreamId({deviceId, nodeId: childId, type: "up"})),
    );

    // upParent should have been retained as a root.
    expectPreludeEqual(
      tree2.value.roots.keySet(),
      HashSet.of(tree1.value.rootNodeKey, upKey(parentId), upKey(childId)),
    );

    const edge = upNodeForNodeId(tree2, childId)
      .getOrThrow()
      .parents.get(EdgeId.create("edge"))
      .getOrThrow();
    expectPreludeEqual(edge.parent.nodeId, junkId);
  });
});

/*
describe("NestedPermissionedTree old", () => {
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

  describe("writers", () => {
    const shareId = new ShareId({creator: deviceA, id: NodeId.create("share")});
    const shareIdOther = new ShareId({
      creator: deviceB,
      id: NodeId.create("shareOther"),
    });
    const deviceAShareDataStream = new StreamId({
      deviceId: deviceA,
      shareId,
      type: "share data",
    });
    const deviceASharedNodeStream = new StreamId({
      deviceId: deviceA,
      shareId,
      type: "shared node",
    });
    const deviceBShareDataStream = new StreamId({
      deviceId: shareIdOther.creator,
      shareId,
      type: "share data",
    });
    const deviceBSharedNodeStream = new StreamId({
      deviceId: shareIdOther.creator,
      shareId,
      type: "shared node",
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
        new Edge({
          shareData: UpNode.create(shareIdOther),
        }),
      );
    });

    // This is what happens when we share a node with a device before adding the
    // node to the device's tree.
    it("makes nodes for unknown children, only share datas for unknown writers", () => {
      const shareRoot = new ShareId({
        creator: deviceA,
        id: NodeId.create("a root"),
      });
      const shareIdOther2 = new ShareId({
        creator: deviceB,
        id: NodeId.create("shareOther2"),
      });
      const tree = createPermissionedTree(shareRoot);
      const tree1 = tree.update(
        HashMap.of(
          [
            new StreamId({
              deviceId: deviceB,
              shareId: shareIdOther,
              type: "share data",
            }),
            LinkedList.of(openWriterOp(shareIdOther2, -1)),
          ],
          [
            new StreamId({deviceId: deviceA, shareId, type: "share data"}),
            LinkedList.of(openWriterOp(shareIdOther, -1)),
          ],
          [
            new StreamId({
              deviceId: deviceA,
              shareId: shareRoot,
              type: "shared node",
            }),
            LinkedList.of(
              // Make this after openWriterOp, to force the tree to handle the
              // writer when it doesn't know about the shared node yet.
              setChildOp({
                nodeId: shareId.id,
                nodeShareId: shareId,
                parentNodeId: shareRoot.id,
              }),
            ),
          ],
        ),
      );

      expectPreludeEqual(
        tree1.desiredHeads(tree1.value),
        desiredHeads(
          HashMap.of(
            // The root has both node and share data.
            [
              shareRoot,
              HashMap.of([
                deviceA,
                HashMap.of(["share data", "open"], ["shared node", "open"]),
              ]),
            ],
            // shareId has both node and share data because it's a child in the
            // tree.
            [
              shareId,
              HashMap.of(
                [
                  deviceA,
                  HashMap.of(["share data", "open"], ["shared node", "open"]),
                ],
                [
                  deviceB,
                  HashMap.of(["share data", "open"], ["shared node", "open"]),
                ],
              ),
            ],
            // We should only have the share data for these, since they don't
            // appear in our tree (just as a writer on things in the tree).
            [
              shareIdOther,
              HashMap.of([deviceB, HashMap.of(["share data", "open"])]),
            ],
            [
              shareIdOther2,
              HashMap.of([deviceB, HashMap.of(["share data", "open"])]),
            ],
          ),
        ),
      );
    });

    it("uses existing share data for parent when adding a writer", () => {
      const tree = createPermissionedTree(shareId).update(
        HashMap.of(
          [
            deviceASharedNodeStream,
            opsList(
              setChildOp({
                parentNodeId: shareId.id,
                nodeShareId: shareIdOther,
                nodeId: shareIdOther.id,
              }),
            ),
          ],
          [
            new StreamId({
              deviceId: deviceB,
              shareId: shareIdOther,
              type: "share data",
            }),
            opsList(openWriterOp(shareId, -1)),
          ],
        ),
      );
      expectIdentical(tree.value.upNodeForNodeId(shareId).isSome(), true);
      expectIdentical(tree.value.shareDataRoots.containsKey(shareId), false);
      expectIdentical(tree.value.upNodeForNodeId(shareIdOther).isSome(), true);
      expectIdentical(
        tree.value.shareDataRoots.containsKey(shareIdOther),
        false,
      );
      expectPreludeEqual(
        tree.value.upNodeForNodeId(shareId).getOrThrow(),
        tree.value
          .upNodeForNodeId(shareIdOther)
          .getOrThrow()
          .parents.get(shareId)
          .getOrThrow().shareData,
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
          deviceAShareDataStream,
          LinkedList.of(openWriterOp(shareIdOther, -1)),
        ]),
      );
      expectPreludeEqual(
        tree1.desiredHeads(tree1.value),
        HashMap.of(
          [deviceASharedNodeStream, "open" as const],
          [deviceAShareDataStream, "open" as const],
          // This is not an active head, since only the share data is relevant
          // to our tree.
          // [
          //   new StreamId({
          //     deviceId: shareIdOther.creator,
          //     shareId: shareIdOther,
          //     type: "shared node",
          //   }),
          //   "open" as const,
          // ],
          [
            new StreamId({
              deviceId: shareIdOther.creator,
              shareId: shareIdOther,
              type: "share data",
            }),
            "open" as const,
          ],
          [deviceBSharedNodeStream, "open" as const],
          [deviceBShareDataStream, "open" as const],
        ),
      );
    });

    it("applies ops to parent writers", () => {
      const shareIdOther2 = new ShareId({
        creator: deviceB,
        id: NodeId.create("shareOther2"),
      });
      const tree = createPermissionedTree(shareId).update(
        HashMap.of(
          [
            deviceAShareDataStream,
            LinkedList.of(openWriterOp(shareIdOther, -1)),
          ],
          [
            new StreamId({
              deviceId: shareIdOther.creator,
              shareId: shareIdOther,
              type: "share data",
            }),
            LinkedList.of(openWriterOp(shareIdOther2, -1)),
          ],
        ),
      );
      // The share data should not be in the roots ...
      expectIdentical(
        tree.value.shareDataRoots.containsKey(shareIdOther),
        false,
      );
      // ... but it should still have been updated.
      expectIdentical(
        tree.value
          .upNodeForNodeId(shareIdOther)
          .getOrThrow()
          .parents.containsKey(shareIdOther2),
        true,
      );
    });

    it("remove parent writer completely", () => {
      const tree = createPermissionedTree(shareId);
      const nodeIdChild = NodeId.create("child");
      const nodeIdParent = NodeId.create("parent");
      const deviceAOp0 = openWriterOp(shareIdOther, -1);
      const deviceCOp0 = setChildOp({
        nodeId: nodeIdChild,
        parentNodeId: nodeIdParent,
      });
      const deviceAOp1 = asType<AppliedOp["op"]>({
        timestamp: clock.now(),
        type: "remove writer",

        writer: shareIdOther,
        streams: HashMap.of(),
      });
      const tree1 = tree.update(
        HashMap.of(
          [deviceAShareDataStream, opsList(deviceAOp0)],
          [deviceBSharedNodeStream, opsList(deviceCOp0)],
        ),
      );
      expect(tree1.heads.containsKey(deviceBSharedNodeStream)).true;

      const tree2 = tree1.update(
        HashMap.of(
          [deviceAShareDataStream, opsList(deviceAOp0, deviceAOp1)],
          [deviceBSharedNodeStream, opsList(deviceCOp0)],
        ),
      );
      expect(tree2.heads.containsKey(deviceBSharedNodeStream)).false;
    });

    it("remove parent writer keeping some ops", () => {
      const tree = createPermissionedTree(shareId);
      const nodeIdChild = NodeId.create("child");
      const nodeIdParent = NodeId.create("parent");
      const deviceBOp0 = setChildOp({
        nodeId: nodeIdChild,
        parentNodeId: nodeIdParent,
      });
      const deviceAOp0 = openWriterOp(shareIdOther, -1);
      const deviceAOp1 = {
        timestamp: clock.now(),
        type: "remove writer",

        writer: shareIdOther,
        streams: HashMap.of([deviceBSharedNodeStream, opsList(deviceBOp0)]),
      } as AppliedOp["op"];
      const tree1 = tree.update(
        HashMap.of(
          [deviceAShareDataStream, opsList(deviceAOp0, deviceAOp1)],
          [deviceBSharedNodeStream, opsList(deviceBOp0)],
        ),
      );
      // We can't use expectPreludeEqual because ops don't support .equals. And
      // we can't use expectIdentical because the two lists aren't identical.
      expectDeepEqual(
        tree1
          .desiredHeads(tree1.value)
          .get(deviceBSharedNodeStream)
          .getOrThrow(),
        opsList(deviceBOp0),
      );
    });

    it("remove parent writer keeping some ops, then re-add", () => {
      const tree = createPermissionedTree(shareId);
      const nodeIdChild = NodeId.create("child");
      const nodeIdParent = NodeId.create("parent");
      const deviceBOp0 = setChildOp({
        nodeId: nodeIdChild,
        parentNodeId: nodeIdParent,
      });
      const deviceAOp0 = openWriterOp(shareIdOther, -1);
      const deviceAOp1 = {
        timestamp: clock.now(),
        type: "remove writer",

        writer: shareIdOther,
        streams: HashMap.of([deviceBSharedNodeStream, opsList(deviceBOp0)]),
      } as AppliedOp["op"];
      const deviceAOp2 = openWriterOp(shareIdOther, -1);
      const tree1 = tree.update(
        HashMap.of(
          [deviceAShareDataStream, opsList(deviceAOp0, deviceAOp1, deviceAOp2)],
          [deviceBSharedNodeStream, opsList(deviceBOp0)],
        ),
      );
      expectIdentical(
        tree1
          .desiredHeads(tree1.value)
          .get(deviceBSharedNodeStream)
          .getOrThrow(),
        "open",
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
          new DownNode({
            id: shareId.id,
            shareId,
            shareData: new UpNode({
              shareId,
              writers: HashMap.of(),
              closedWriterDevicesForShareData: HashMap.of(),
              closedWriterDevicesForSharedNode: HashMap.of(),
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
          .downNodeForNodeId(new NodeKey({shareId: shareA, nodeId: shareA.id}))
          .getOrThrow()
          .downNodeForNodeId(new NodeKey({shareId: shareC, nodeId: shareC.id}))
          .isSome(),
        true,
      );
      expectIdentical(
        tree1.value
          .root()
          .downNodeForNodeId(new NodeKey({shareId: shareB, nodeId: shareB.id}))
          .getOrThrow()
          .downNodeForNodeId(new NodeKey({shareId: shareC, nodeId: shareC.id}))
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

// Makes a desired-heads map from a nested structure that is more
// convenient for writing out.
function desiredHeads(
  heads: HashMap<
    ShareId,
    HashMap<
      DeviceId,
      HashMap<"share data" | "shared node", "open" | OpList<AppliedOp>>
    >
  >,
): HashMap<StreamId, "open" | OpList<AppliedOp>> {
  return heads.flatMap((shareId, map) =>
    map.flatMap((deviceId, map) =>
      map.flatMap((type, status) => [
        [new StreamId({shareId, type, deviceId}), status],
      ]),
    ),
  );
}
*/
