import {ControlledOpSet, DeviceId, Op} from "./ControlledOpSet";
import {asType, mapWith, mapWithout, RoArray, RoMap} from "./helper/Collection";
import {CountingClock} from "./helper/Clock.testing";
import {expectDeepEqual, expectIdentical} from "./helper/Shared.testing";
import {AssertFailed} from "./helper/Assert";

describe("ControlledOpSet", () => {
  const deviceA = DeviceId.create("a");
  const deviceB = DeviceId.create("b");

  describe("basic", () => {
    type Value = RoArray<string>;
    type OpPayloads = {
      forward: string;
      backward: undefined;
    };

    const clock = new CountingClock();
    const cos = ControlledOpSet.create<Value, OpPayloads>(
      (value, op) => {
        return {value: [...value, op.forward], backward: undefined};
      },
      (value, p) => {
        return value.slice(0, -1);
      },
      (value) =>
        RoMap([
          [deviceA, "open"],
          [deviceB, "open"],
        ]),
      RoArray<string>(),
    );
    const opA0 = asType<Op<OpPayloads>>({
      forward: "a0",
      timestamp: clock.now(),
      prev: undefined,
    });
    const opA1 = asType<Op<OpPayloads>>({
      forward: "a1",
      timestamp: clock.now(),
      prev: opA0,
    });
    const opB0 = asType<Op<OpPayloads>>({
      forward: "b0",
      timestamp: clock.now(),
      prev: undefined,
    });

    it("update merges single new op", () => {
      const cos1 = cos.update(RoMap([[deviceA, opA0]]));
      expectDeepEqual(cos1.value, RoArray([opA0.forward]));
      expectDeepEqual(
        cos1.heads,
        RoMap<DeviceId, Op<OpPayloads>>([[deviceA, opA0]]),
      );
    });

    it("update merges multiple new ops", () => {
      const cos1 = cos.update(RoMap([[deviceA, opA1]]));
      expectDeepEqual(cos1.value, RoArray([opA0.forward, opA1.forward]));
      expectDeepEqual(
        cos1.heads,
        RoMap<DeviceId, Op<OpPayloads>>([[deviceA, opA1]]),
      );
    });

    it("update undoes before applying new ops", () => {
      const cos1 = cos.update(RoMap([[deviceA, opA0]]));
      const cos2 = cos1.update(
        RoMap([
          [deviceA, opA0],
          [deviceB, opB0],
        ]),
      );
      const cos3 = cos2.update(
        RoMap([
          [deviceA, opA1],
          [deviceB, opB0],
        ]),
      );
      expectDeepEqual(
        cos3.value,
        RoArray([opA0.forward, opA1.forward, opB0.forward]),
      );
    });

    it("update purges newer ops from a device if needed", () => {
      const cos1 = cos.update(RoMap([[deviceA, opA1]]));
      const cos2 = cos1.update(RoMap([[deviceA, opA0]]));
      expectDeepEqual(cos2.value, RoArray([opA0.forward]));
      expectDeepEqual(
        cos2.heads,
        RoMap<DeviceId, Op<OpPayloads>>([[deviceA, opA0]]),
      );
    });
  });

  describe("write permissions", () => {
    type AddToken = {
      forward: {
        type: "add";
        token: string;
      };
      backward: undefined;
    };
    type AddWriter = {
      forward: {
        type: "add writer";
        deviceId: DeviceId;
      };
      backward: undefined | "open" | Op<OpPayloads>;
    };
    type RemoveWriter = {
      forward: {
        type: "remove writer";
        deviceId: DeviceId;
        finalOp: Op<OpPayloads>;
      };
      backward: undefined | "open" | Op<OpPayloads>;
    };
    type OpPayloads = AddToken | AddWriter | RemoveWriter;
    type Value = {
      tokens: RoArray<string>;
      desiredWriters: RoMap<DeviceId, "open" | Op<OpPayloads>>;
    };

    const clock = new CountingClock();
    const cos = ControlledOpSet.create<Value, OpPayloads>(
      (value, op) => {
        if (op.forward.type === "add")
          return {
            value: {
              ...value,
              tokens: [...value.tokens, op.forward.token],
            },
            backward: undefined,
          };
        else if (op.forward.type === "add writer") {
          return {
            value: {
              ...value,
              desiredWriters: mapWith(
                value.desiredWriters,
                op.forward.deviceId,
                "open",
              ),
            },
            backward: value.desiredWriters.get(op.forward.deviceId),
          };
        } else {
          const deviceId = op.forward.deviceId;
          return {
            value: {
              ...value,
              desiredWriters: mapWith(
                value.desiredWriters,
                deviceId,
                op.forward.finalOp,
              ),
            },
            backward: value.desiredWriters.get(deviceId),
          };
        }
      },
      (value, op, backward) => {
        if (op.forward.type === "add") {
          return {
            ...value,
            tokens: value.tokens.slice(0, -1),
          };
        } else if (op.forward.type === "add writer") {
          backward = backward as AddWriter["backward"];
          return {
            ...value,
            desiredWriters:
              backward === undefined
                ? mapWithout(value.desiredWriters, op.forward.deviceId)
                : mapWith(value.desiredWriters, op.forward.deviceId, backward),
          };
        } else if (op.forward.type === "remove writer") {
          backward = backward as RemoveWriter["backward"];
          const deviceId = op.forward.deviceId;
          return {
            ...value,
            desiredWriters:
              backward === undefined
                ? mapWithout(value.desiredWriters, deviceId)
                : mapWith(value.desiredWriters, deviceId, backward),
          };
        } else throw new AssertFailed("Unknown op type");
      },
      (value) => value.desiredWriters,
      {tokens: RoArray<string>(), desiredWriters: RoMap([[deviceB, "open"]])},
    );
    const opA0 = asType<Op<OpPayloads>>({
      forward: {type: "add", token: "a0"},
      timestamp: clock.now(),
      prev: undefined,
    });
    const opA1 = asType<Op<OpPayloads>>({
      forward: {type: "add", token: "a1"},
      timestamp: clock.now(),
      prev: opA0,
    });
    const opB0 = asType<Op<OpPayloads>>({
      forward: {type: "add writer", deviceId: deviceA},
      timestamp: clock.now(),
      prev: undefined,
    });
    const opB1 = asType<Op<OpPayloads>>({
      forward: {type: "remove writer", deviceId: deviceA, finalOp: opA0},
      timestamp: clock.now(),
      prev: opB0,
    });
    const opB2 = asType<Op<OpPayloads>>({
      forward: {type: "add writer", deviceId: deviceA},
      timestamp: clock.now(),
      prev: opB1,
    });
    const opB0Alternate = asType<Op<OpPayloads>>({
      forward: {type: "add", token: "b0Alternate"},
      timestamp: clock.now(),
      // TODO: would be nice to be able to chain ops of different types.
      prev: undefined,
    });
    const opA2 = asType<Op<OpPayloads>>({
      forward: {type: "add", token: "a2"},
      timestamp: clock.now(),
      prev: opA1,
    });

    it("ignores ops from a non-writer", () => {
      const cos1 = cos.update(RoMap([[deviceA, opA1]]));
      expectIdentical(cos1.appliedHead, undefined);
    });

    it("includes ops from an added writer", () => {
      const cos1 = cos.update(
        RoMap<DeviceId, Op<OpPayloads>>([
          [deviceA, opA1],
          [deviceB, opB0],
        ]),
      );
      expectDeepEqual(cos1.value.tokens, RoArray(["a0", "a1"]));
      expectDeepEqual(
        cos1.heads,
        RoMap<DeviceId, Op<OpPayloads>>([
          [deviceA, opA1],
          [deviceB, opB0],
        ]),
      );
    });

    it("closes a writer", () => {
      const cos1 = cos.update(
        RoMap<DeviceId, Op<OpPayloads>>([
          [deviceA, opA1],
          [deviceB, opB0],
        ]),
      );
      expectDeepEqual(cos1.value.tokens, RoArray(["a0", "a1"]));

      // Close deviceA after opA0.
      const cos2 = cos.update(
        RoMap<DeviceId, Op<OpPayloads>>([
          [deviceA, opA1],
          [deviceB, opB1],
        ]),
      );
      // a1 was excluded because b0 closed that device.
      expectDeepEqual(cos2.value.tokens, RoArray(["a0"]));
      expectDeepEqual(
        cos2.heads,
        RoMap<DeviceId, Op<OpPayloads>>([
          [deviceA, opA0],
          [deviceB, opB1],
        ]),
      );
    });

    it("reopens a closed writer", () => {
      const cos1 = cos.update(
        RoMap<DeviceId, Op<OpPayloads>>([
          [deviceA, opA1],
          [deviceB, opB1],
        ]),
      );
      expectDeepEqual(cos1.value.tokens, RoArray(["a0"]));

      // Reopen deviceA.
      const cos2 = cos.update(
        RoMap<DeviceId, Op<OpPayloads>>([
          [deviceA, opA1],
          [deviceB, opB2],
        ]),
      );
      expectDeepEqual(cos2.value.tokens, RoArray(["a0", "a1"]));
      expectDeepEqual(
        cos2.heads,
        RoMap<DeviceId, Op<OpPayloads>>([
          [deviceA, opA1],
          [deviceB, opB2],
        ]),
      );
    });

    it("avoids redoing later ops from a removed writer", () => {
      const cos1 = cos.update(
        RoMap([
          [deviceA, opA2],
          [deviceB, opB0],
        ]),
      );
      expectDeepEqual(cos1.value.tokens, RoArray(["a0", "a1", "a2"]));

      const cos2 = cos.update(
        RoMap([
          [deviceA, opA2],
          [deviceB, opB1],
        ]),
      );
      // Even a2, which came after b1, which closed device A, is gone.
      expectDeepEqual(cos2.value.tokens, RoArray(["a0"]));
      expectDeepEqual(
        cos2.heads,
        RoMap<DeviceId, Op<OpPayloads>>([
          [deviceA, opA0],
          [deviceB, opB1],
        ]),
      );
    });

    it("undo Add/Remove Writer", () => {
      const cos1 = cos.update(
        RoMap<DeviceId, Op<OpPayloads>>([
          [deviceA, opA1],
          [deviceB, opB1],
        ]),
      );
      expectDeepEqual(cos1.value.tokens, RoArray(["a0"]));
      // This forces b0 and b1 to be undone.
      const cos2 = cos1.update(
        RoMap<DeviceId, Op<OpPayloads>>([
          [deviceA, opA1],
          [deviceB, opB0Alternate],
        ]),
      );

      expectDeepEqual(cos2.value.tokens, RoArray(["b0Alternate"]));
      expectDeepEqual(
        cos2.heads,
        RoMap<DeviceId, Op<OpPayloads>>([[deviceB, opB0Alternate]]),
      );
    });
  });
});
