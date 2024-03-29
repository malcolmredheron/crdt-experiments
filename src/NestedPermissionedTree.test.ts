import {
  AppliedOp,
  createPermissionedTree,
  DeviceId,
  NodeId,
  NodeInfo,
  PriorityStatus,
  ShareId,
  StreamId,
} from "./NestedPermissionedTree";
import {CountingClock} from "./helper/Clock.testing";
import {
  expectDeepEqual,
  expectIdentical,
  expectPreludeEqual,
} from "./helper/Shared.testing";
import {HashMap, LinkedList} from "prelude-ts";

describe("NestedPermissionedTree", () => {
  const clock = new CountingClock();
  const deviceA = DeviceId.create("A");
  const deviceB = DeviceId.create("B");
  const shareId = new ShareId({creator: deviceA, id: "share"});
  const deviceAStreamId = new StreamId({deviceId: deviceA, shareId});
  const deviceBStreamId = new StreamId({deviceId: deviceB, shareId});
  const tree = createPermissionedTree(shareId);
  const rootNodeId = NodeId.create("root");

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
  const opA0 = LinkedList.of<AppliedOp["op"]>(
    openWriterOp(deviceA, deviceB, -1),
  );

  describe("permissions", () => {
    it("adds a lower-ranked writer", () => {
      const tree1 = tree.update(
        HashMap.of([new StreamId(deviceAStreamId), opA0]),
      );
      expectDeepEqual(
        tree1.value.sharedNodes
          .get(shareId)
          .getOrThrow()
          .writers.get(deviceB)
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
        tree1.value.sharedNodes
          .get(shareId)
          .getOrThrow()
          .writers.get(deviceB)
          .getOrUndefined(),
        undefined,
      );
    });

    it("ignores a SetWriter to modify an equal-priority writer", () => {
      const tree1 = tree.update(
        HashMap.of([
          deviceAStreamId,
          opA0.prepend(openWriterOp(deviceB, deviceB, -2)),
        ]),
      );
      expectDeepEqual(
        tree1.value.sharedNodes
          .get(shareId)
          .getOrThrow()
          .writers.get(deviceB)
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

    const opA1 = opA0.prepend({
      timestamp: clock.now(),
      device: deviceA,
      type: "create node",
      node: nodeA,
      parent: rootNodeId,
      position: 1,
      shareId: undefined,
    });
    const opA2 = opA1.prepend({
      timestamp: clock.now(),
      device: deviceA,
      type: "create node",
      node: nodeB,
      parent: rootNodeId,
      position: 2,
      shareId: undefined,
    });
    const opB0 = LinkedList.of<AppliedOp["op"]>({
      timestamp: clock.now(),
      device: deviceB,
      type: "set parent",
      node: nodeA,
      parent: nodeB,
      position: 0,
    });
    const opA3 = opA2.prepend({
      timestamp: clock.now(),
      device: deviceA,
      type: "set parent",
      node: nodeB,
      parent: nodeA,
      position: 0,
    });

    describe("create node", () => {
      it("creates a node if not present", () => {
        const tree1 = tree.update(HashMap.of([deviceAStreamId, opA1]));
        expectPreludeEqual(
          tree1.value.sharedNodes.get(shareId).getOrThrow().nodes,
          HashMap.of([
            nodeA,
            new NodeInfo({
              parent: rootNodeId,
              position: 1,
              shareId: undefined,
            }),
          ]),
        );
      });

      it("does nothing if already preesnt", () => {
        const tree1 = tree.update(
          HashMap.of(
            [deviceAStreamId, opA1],
            [
              deviceBStreamId,
              LinkedList.of({
                timestamp: clock.now(),
                device: deviceB,
                type: "create node",
                node: nodeA,
                parent: rootNodeId,
                position: 2,
                shareId: undefined,
              }),
            ],
          ),
        );
        expectPreludeEqual(
          tree1.value.sharedNodes.get(shareId).getOrThrow().nodes,
          HashMap.of([
            nodeA,
            new NodeInfo({
              parent: rootNodeId,
              position: 1,
              shareId: undefined,
            }),
          ]),
        );
      });
    });

    describe("set parent", () => {
      it("moves a node if present", () => {
        const tree1 = tree.update(HashMap.of([deviceAStreamId, opA3]));
        expectPreludeEqual(
          tree1.value.sharedNodes.get(shareId).getOrThrow().nodes,
          HashMap.of(
            [
              nodeA,
              new NodeInfo({
                parent: rootNodeId,
                position: 1,
                shareId: undefined,
              }),
            ],
            [
              nodeB,
              new NodeInfo({
                parent: nodeA,
                position: 0,
                shareId: undefined,
              }),
            ],
          ),
        );
      });

      it("does nothing if not preseent", () => {
        const tree1 = tree.update(
          HashMap.of([
            deviceAStreamId,
            LinkedList.of({
              timestamp: clock.now(),
              device: deviceA,
              type: "set parent",
              node: nodeA,
              parent: rootNodeId,
              position: 2,
            }),
          ]),
        );
        expectPreludeEqual(
          tree1.value.sharedNodes.get(shareId).getOrThrow().nodes,
          HashMap.of(),
        );
      });

      it("avoids a cycle", () => {
        const tree1 = tree.update(
          HashMap.of([deviceAStreamId, opA3], [deviceBStreamId, opB0]),
        );
        expectPreludeEqual(
          tree1.value.sharedNodes.get(shareId).getOrThrow().nodes,
          HashMap.of(
            [
              nodeA,
              new NodeInfo({
                parent: nodeB,
                position: 0,
                shareId: undefined,
              }),
            ],
            [
              nodeB,
              new NodeInfo({
                parent: rootNodeId,
                position: 2,
                shareId: undefined,
              }),
            ],
          ),
        );
      });
    });
  });

  it("nested shares", () => {
    const shareA = new ShareId({creator: deviceA, id: "a root"});
    const shareShared = new ShareId({creator: deviceB, id: "shared"});
    const nodeA = NodeId.create("nodeA");
    const treeA = createPermissionedTree(shareA);
    const nodeShared = NodeId.create("nodeShared");

    // Make the op in the shared node first, so that the tree has to handle
    // getting an op before it knows where to put it.
    const opBShared0 = LinkedList.of<AppliedOp["op"]>({
      timestamp: clock.now(),
      device: deviceB,
      type: "create node",
      node: nodeA,
      parent: rootNodeId,
      position: 0,
      shareId: undefined,
    });
    const opAA0 = LinkedList.of<AppliedOp["op"]>({
      timestamp: clock.now(),
      device: deviceA,
      type: "create node",
      node: nodeShared,
      parent: rootNodeId,
      position: 0,
      shareId: shareShared,
    });

    const treeA1 = treeA.update(
      HashMap.of(
        [new StreamId({deviceId: deviceA, shareId: shareA}), opAA0],
        [new StreamId({deviceId: deviceB, shareId: shareShared}), opBShared0],
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
    expectPreludeEqual(
      treeA1.value.sharedNodes.get(shareA).getOrThrow().nodes,
      HashMap.of([
        nodeShared,
        new NodeInfo({
          parent: rootNodeId,
          position: 0,
          shareId: shareShared,
        }),
      ]),
    );
    expectPreludeEqual(
      treeA1.value.sharedNodes.get(shareShared).getOrThrow().nodes,
      HashMap.of([
        nodeA,
        new NodeInfo({
          parent: rootNodeId,
          position: 0,
          shareId: undefined,
        }),
      ]),
    );
  });
});
