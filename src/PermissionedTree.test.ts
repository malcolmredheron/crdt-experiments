import {
  AppliedOp,
  createPermissionedTree,
  NodeId,
  ParentPos,
  PriorityStatus,
} from "./PermissionedTree";
import {DeviceId, OpList} from "./ControlledOpSet";
import {CountingClock} from "./helper/Clock.testing";
import {
  expectDeepEqual,
  expectIdentical,
  expectPreludeEqual,
} from "./helper/Shared.testing";
import {HashMap} from "prelude-ts";

describe("PermissionedTree", () => {
  const clock = new CountingClock();
  const deviceA = DeviceId.create("A");
  const deviceB = DeviceId.create("B");
  const tree = createPermissionedTree(deviceA);
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
  const opA0 = new OpList<AppliedOp>({
    op: openWriterOp(deviceA, deviceB, -1),
    prev: undefined,
  });

  describe("permissions", () => {
    it("adds a lower-ranked writer", () => {
      const tree1 = tree.update(HashMap.of([deviceA, opA0]));
      expectDeepEqual(
        tree1.value.writers.get(deviceB).getOrUndefined(),
        new PriorityStatus({
          priority: -1,
          status: "open",
        }),
      );
    });

    it("ignores a SetWriter to add an equal-priority writer", () => {
      const tree1 = tree.update(
        HashMap.of([
          deviceA,
          new OpList<AppliedOp>({
            op: openWriterOp(deviceA, deviceB, 0),
            prev: undefined,
          }),
        ]),
      );
      expectIdentical(
        tree1.value.writers.get(deviceB).getOrUndefined(),
        undefined,
      );
    });

    it("ignores a SetWriter to modify an equal-priority writer", () => {
      const tree1 = tree.update(
        HashMap.of([
          deviceA,
          new OpList<AppliedOp>({
            op: openWriterOp(deviceB, deviceB, -2),
            prev: opA0,
          }),
        ]),
      );
      expectDeepEqual(
        tree1.value.writers.get(deviceB).getOrThrow(),
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

    const opA1 = new OpList<AppliedOp>({
      op: {
        timestamp: clock.now(),
        type: "set parent",
        node: nodeA,
        parent: rootNodeId,
        position: 1,
      },
      prev: opA0,
    });
    const opA2 = new OpList<AppliedOp>({
      op: {
        timestamp: clock.now(),
        type: "set parent",
        node: nodeB,
        parent: rootNodeId,
        position: 2,
      },
      prev: opA1,
    });
    const opB0 = new OpList<AppliedOp>({
      op: {
        timestamp: clock.now(),
        type: "set parent",
        node: nodeA,
        parent: nodeB,
        position: 0,
      },
      prev: undefined,
    });
    const opA3 = new OpList<AppliedOp>({
      op: {
        timestamp: clock.now(),
        type: "set parent",
        node: nodeB,
        parent: nodeA,
        position: 0,
      },
      prev: opA2,
    });

    it("adds a node", () => {
      const tree1 = tree.update(
        HashMap.of<DeviceId, OpList<AppliedOp>>([deviceA, opA3]),
      );
      expectPreludeEqual(
        tree1.value.nodes,
        HashMap.of(
          [nodeA, new ParentPos({parent: rootNodeId, position: 1})],
          [nodeB, new ParentPos({parent: nodeA, position: 0})],
        ),
      );
    });

    it("moves a node", () => {
      const tree1 = tree.update(
        HashMap.of<DeviceId, OpList<AppliedOp>>([deviceA, opA3]),
      );
      expectPreludeEqual(
        tree1.value.nodes,
        HashMap.of(
          [nodeA, new ParentPos({parent: rootNodeId, position: 1})],
          [nodeB, new ParentPos({parent: nodeA, position: 0})],
        ),
      );
    });

    it("avoids a cycle", () => {
      const tree1 = tree.update(
        HashMap.of<DeviceId, OpList<AppliedOp>>(
          [deviceA, opA3],
          [deviceB, opB0],
        ),
      );
      expectPreludeEqual(
        tree1.value.nodes,
        HashMap.of(
          [nodeA, new ParentPos({parent: nodeB, position: 0})],
          [nodeB, new ParentPos({parent: rootNodeId, position: 2})],
        ),
      );
    });
  });
});
