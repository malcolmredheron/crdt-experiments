import {AppliedOp, createPermissionedTree} from "./PermissionedTree";
import {DeviceId, OpList} from "./ControlledOpSet";
import {RoMap} from "./helper/Collection";
import {CountingClock} from "./helper/Clock.testing";
import {expectDeepEqual} from "./helper/Shared.testing";

describe("PermissionedTree", () => {
  const clock = new CountingClock();
  const deviceA = DeviceId.create("A");
  const deviceB = DeviceId.create("B");
  const tree = createPermissionedTree(deviceA);

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
  const opA0: OpList<AppliedOp> = {
    op: openWriterOp(deviceA, deviceB, -1),
    prev: undefined,
  };

  describe("permissions", () => {
    it("adds a lower-ranked writer", () => {
      const tree1 = tree.update(RoMap([[deviceA, opA0]]));
      expectDeepEqual(tree1.value.writers.get(deviceB), {
        priority: -1,
        status: "open",
      });
    });

    it("ignores a SetWriter to add an equal-priority writer", () => {
      const tree1 = tree.update(
        RoMap([
          [
            deviceA,
            {
              op: openWriterOp(deviceA, deviceB, 0),
              prev: undefined,
              timestamp: clock.now(),
            },
          ],
        ]),
      );
      expectDeepEqual(tree1.value.writers.get(deviceB), undefined);
    });

    it("ignores a SetWriter to modify an equal-priority writer", () => {
      const tree1 = tree.update(
        RoMap([
          [
            deviceA,
            {
              op: openWriterOp(deviceB, deviceB, -2),
              prev: opA0,
              timestamp: clock.now(),
            },
          ],
        ]),
      );
      expectDeepEqual(tree1.value.writers.get(deviceB), {
        status: "open",
        priority: -1,
      });
    });
  });
});
