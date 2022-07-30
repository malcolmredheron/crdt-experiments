import {ControlledOpSet, OpList} from "./ControlledOpSet";
import {CountingClock} from "./helper/Clock.testing";
import {expectIdentical, expectPreludeEqual} from "./helper/Shared.testing";
import {AssertFailed} from "./helper/Assert";
import {Timestamp} from "./helper/Timestamp";
import {
  PersistentAppliedOp,
  persistentDoOpFactory,
  persistentUndoOp,
} from "./PersistentUndoHelper";
import {HashMap, LinkedList, Vector} from "prelude-ts";
import {TypedValue} from "./helper/TypedValue";

describe("ControlledOpSet", () => {
  class DeviceId extends TypedValue<"DeviceId", string> {}

  const deviceA = DeviceId.create("a");
  const deviceB = DeviceId.create("b");

  describe("basic", () => {
    type Value = Vector<string>;
    type AppliedOp = PersistentAppliedOp<
      Value,
      {token: string; timestamp: Timestamp}
    >;

    const clock = new CountingClock();
    const cos = ControlledOpSet.create<Value, AppliedOp, DeviceId>(
      persistentDoOpFactory((value, op) => {
        return value.append(op.token);
      }),
      persistentUndoOp,
      (value) => HashMap.of([deviceA, "open"], [deviceB, "open"]),
      Vector<string>.of(),
    );
    const opA0 = LinkedList.of<AppliedOp["op"]>({
      token: "a0",
      timestamp: clock.now(),
    });
    const opA1 = opA0.prepend({
      token: "a1",
      timestamp: clock.now(),
    });
    const opB0 = LinkedList.of<AppliedOp["op"]>({
      token: "b0",
      timestamp: clock.now(),
    });

    it("update merges single new op", () => {
      const cos1 = cos.update(HashMap.of([deviceA, opA0]));
      expectPreludeEqual(cos1.value, Vector.of("a0"));
      expectIdentical(
        ControlledOpSet.headsEqual(cos1.heads, HashMap.of([deviceA, opA0])),
        true,
      );
    });

    it("update merges multiple new ops", () => {
      const cos1 = cos.update(HashMap.of([deviceA, opA1]));
      expectPreludeEqual(cos1.value, Vector.of("a0", "a1"));
      expectIdentical(
        ControlledOpSet.headsEqual(cos1.heads, HashMap.of([deviceA, opA1])),
        true,
      );
    });

    it("update undoes before applying new ops", () => {
      const cos1 = cos.update(HashMap.of([deviceA, opA0]));
      const cos2 = cos1.update(HashMap.of([deviceA, opA0], [deviceB, opB0]));
      const cos3 = cos2.update(HashMap.of([deviceA, opA1], [deviceB, opB0]));
      expectPreludeEqual(cos3.value, Vector.of("a0", "a1", "b0"));
    });

    it("update purges newer ops from a device if needed", () => {
      const cos1 = cos.update(HashMap.of([deviceA, opA1]));
      const cos2 = cos1.update(HashMap.of([deviceA, opA0]));
      expectPreludeEqual(cos2.value, Vector.of("a0"));
      expectIdentical(
        ControlledOpSet.headsEqual(cos2.heads, HashMap.of([deviceA, opA0])),
        true,
      );
    });
  });

  // Also demonstrates using custom undo code.
  describe("write permissions", () => {
    type AddToken = {
      op: {
        timestamp: Timestamp;
        type: "add";
        token: string;
      };
      undoInfo: undefined;
    };
    type AddWriter = {
      op: {
        timestamp: Timestamp;
        deviceId: DeviceId;
        type: "add writer";
      };
      undoInfo: undefined | "open" | OpList<AppliedOp>;
    };
    type RemoveWriter = {
      op: {
        timestamp: Timestamp;
        deviceId: DeviceId;
        type: "remove writer";
        finalOp: OpList<AppliedOp>;
      };
      undoInfo: undefined | "open" | OpList<AppliedOp>;
    };
    type AppliedOp = AddToken | AddWriter | RemoveWriter;
    type Value = {
      tokens: Vector<string>;
      desiredWriters: HashMap<DeviceId, "open" | OpList<AppliedOp>>;
    };

    const clock = new CountingClock();
    const cos = ControlledOpSet.create<Value, AppliedOp, DeviceId>(
      (value, op) => {
        if (op.type === "add")
          return {
            value: {
              ...value,
              tokens: value.tokens.append(op.token),
            },
            appliedOp: {op, undoInfo: undefined},
          };
        else if (op.type === "add writer") {
          return {
            value: {
              ...value,
              desiredWriters: value.desiredWriters.put(op.deviceId, "open"),
            },
            appliedOp: {
              op,
              undoInfo: value.desiredWriters.get(op.deviceId).getOrUndefined(),
            },
          };
        } else {
          const deviceId = op.deviceId;
          return {
            value: {
              ...value,
              desiredWriters: value.desiredWriters.put(deviceId, op.finalOp),
            },
            appliedOp: {
              op,
              undoInfo: value.desiredWriters.get(deviceId).getOrUndefined(),
            },
          };
        }
      },
      (value, {op, undoInfo}) => {
        if (op.type === "add") {
          return {
            ...value,
            tokens: value.tokens.dropRight(1),
          };
        } else if (op.type === "add writer") {
          undoInfo = undoInfo as AddWriter["undoInfo"];
          return {
            ...value,
            desiredWriters:
              undoInfo === undefined
                ? value.desiredWriters.remove(op.deviceId)
                : value.desiredWriters.put(op.deviceId, undoInfo),
          };
        } else if (op.type === "remove writer") {
          undoInfo = undoInfo as RemoveWriter["undoInfo"];
          const deviceId = op.deviceId;
          return {
            ...value,
            desiredWriters:
              undoInfo === undefined
                ? value.desiredWriters.remove(deviceId)
                : value.desiredWriters.put(deviceId, undoInfo),
          };
        } else throw new AssertFailed("Unknown op type");
      },
      (value) => HashMap.ofIterable(value.desiredWriters),
      {
        tokens: Vector.of(),
        desiredWriters: HashMap.of([deviceB, "open"]),
      },
    );
    const opA0 = LinkedList.of<AppliedOp["op"]>({
      type: "add",
      token: "a0",
      timestamp: clock.now(),
    });
    const opA1 = opA0.prepend({
      type: "add",
      token: "a1",
      timestamp: clock.now(),
    });
    const opB0 = LinkedList.of<AppliedOp["op"]>({
      type: "add writer",
      deviceId: deviceA,
      timestamp: clock.now(),
    });
    const opB1 = opB0.prepend({
      type: "remove writer",
      deviceId: deviceA,
      finalOp: opA0,
      timestamp: clock.now(),
    });
    const opB2 = opB1.prepend({
      type: "add writer",
      deviceId: deviceA,
      timestamp: clock.now(),
    });
    const opB0Alternate = LinkedList.of<AppliedOp["op"]>({
      type: "add",
      token: "b0Alternate",
      timestamp: clock.now(),
    });
    const opA2 = opA1.prepend({
      type: "add",
      token: "a2",
      timestamp: clock.now(),
    });

    it("ignores ops from a non-writer", () => {
      const cos1 = cos.update(HashMap.of([deviceA, opA1]));
      expectIdentical(cos1.appliedOps.isEmpty(), true);
    });

    it("includes ops from an added writer", () => {
      const cos1 = cos.update(HashMap.of([deviceA, opA1], [deviceB, opB0]));
      expectPreludeEqual(cos1.value.tokens, Vector.of("a0", "a1"));
      expectIdentical(
        ControlledOpSet.headsEqual(
          cos1.heads,
          HashMap.of([deviceA, opA1], [deviceB, opB0]),
        ),
        true,
      );
    });

    it("closes a writer", () => {
      const cos1 = cos.update(HashMap.of([deviceA, opA1], [deviceB, opB0]));
      expectPreludeEqual(cos1.value.tokens, Vector.of("a0", "a1"));

      // Close deviceA after opA0.
      const cos2 = cos.update(HashMap.of([deviceA, opA1], [deviceB, opB1]));
      // a1 was excluded because b0 closed that device.
      expectPreludeEqual(cos2.value.tokens, Vector.of("a0"));
      expectIdentical(
        ControlledOpSet.headsEqual(
          cos2.heads,
          HashMap.of([deviceA, opA0], [deviceB, opB1]),
        ),
        true,
      );
    });

    it("reopens a closed writer", () => {
      const cos1 = cos.update(HashMap.of([deviceA, opA1], [deviceB, opB1]));
      expectPreludeEqual(cos1.value.tokens, Vector.of("a0"));

      // Reopen deviceA.
      const cos2 = cos.update(HashMap.of([deviceA, opA1], [deviceB, opB2]));
      expectPreludeEqual(cos2.value.tokens, Vector.of("a0", "a1"));
      expectIdentical(
        ControlledOpSet.headsEqual(
          cos2.heads,
          HashMap.of([deviceA, opA1], [deviceB, opB2]),
        ),
        true,
      );
    });

    it("avoids redoing later ops from a removed writer", () => {
      const cos1 = cos.update(HashMap.of([deviceA, opA2], [deviceB, opB0]));
      expectPreludeEqual(cos1.value.tokens, Vector.of("a0", "a1", "a2"));

      const cos2 = cos.update(HashMap.of([deviceA, opA2], [deviceB, opB1]));
      // Even a2, which came after b1, which closed device A, is gone.
      expectPreludeEqual(cos2.value.tokens, Vector.of("a0"));
      expectIdentical(
        ControlledOpSet.headsEqual(
          cos2.heads,
          HashMap.of([deviceA, opA0], [deviceB, opB1]),
        ),
        true,
      );
    });

    it("undo Add/Remove Writer", () => {
      const cos1 = cos.update(HashMap.of([deviceA, opA1], [deviceB, opB1]));
      expectPreludeEqual(cos1.value.tokens, Vector.of("a0"));
      // This forces b0 and b1 to be undone.
      const cos2 = cos1.update(
        HashMap.of([deviceA, opA1], [deviceB, opB0Alternate]),
      );

      expectPreludeEqual(cos2.value.tokens, Vector.of("b0Alternate"));
      expectIdentical(
        ControlledOpSet.headsEqual(
          cos2.heads,
          HashMap.of([deviceB, opB0Alternate]),
        ),
        true,
      );
    });
  });
});
