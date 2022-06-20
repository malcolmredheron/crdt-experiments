import {DeviceId, Op, SyncState} from "./SyncState";
import {asType, mapWith, mapWithout, RoArray, RoMap} from "./helper/Collection";
import {CountingClock} from "./helper/Clock.testing";
import {expectDeepEqual, expectIdentical} from "./helper/Shared.testing";
import {AssertFailed} from "./helper/Assert";

describe("SyncState", () => {
  const deviceA = DeviceId.create("a");
  const deviceB = DeviceId.create("b");

  describe("basic", () => {
    type AppState = RoArray<string>;
    type OpPayloads = {
      forward: string;
      backward: undefined;
    };

    const clock = new CountingClock();
    const state = SyncState.create<AppState, OpPayloads>(
      (state, op) => {
        return {state: [...state, op.forward], backward: undefined};
      },
      (state, p) => {
        return state.slice(0, -1);
      },
      (state) =>
        RoMap([
          [deviceA, "open"],
          [deviceB, "open"],
        ]),
      RoArray<string>(),
    );
    const opA0 = asType<Op<OpPayloads>>({
      deviceId: deviceA,
      forward: "a0",
      timestamp: clock.now(),
      prev: undefined,
    });
    const opA1 = asType<Op<OpPayloads>>({
      deviceId: deviceA,
      forward: "a1",
      timestamp: clock.now(),
      prev: opA0,
    });
    const opB0 = asType<Op<OpPayloads>>({
      deviceId: deviceB,
      forward: "b0",
      timestamp: clock.now(),
      prev: undefined,
    });

    it("update merges single new op", () => {
      const state1 = state.update(RoMap([[deviceA, opA0]]));
      expectDeepEqual(state1.appState, RoArray([opA0.forward]));
      expectDeepEqual(
        state1.heads,
        RoMap<DeviceId, Op<OpPayloads>>([[deviceA, opA0]]),
      );
    });

    it("update merges multiple new ops", () => {
      const state1 = state.update(RoMap([[deviceA, opA1]]));
      expectDeepEqual(state1.appState, RoArray([opA0.forward, opA1.forward]));
      expectDeepEqual(
        state1.heads,
        RoMap<DeviceId, Op<OpPayloads>>([[deviceA, opA1]]),
      );
    });

    it("update undoes before applying new ops", () => {
      const state1 = state.update(RoMap([[deviceA, opA0]]));
      const state2 = state1.update(
        RoMap([
          [deviceA, opA0],
          [deviceB, opB0],
        ]),
      );
      const state3 = state2.update(
        RoMap([
          [deviceA, opA1],
          [deviceB, opB0],
        ]),
      );
      expectDeepEqual(
        state3.appState,
        RoArray([opA0.forward, opA1.forward, opB0.forward]),
      );
    });

    it("update purges newer ops from a device if needed", () => {
      const state1 = state.update(RoMap([[deviceA, opA1]]));
      const state2 = state1.update(RoMap([[deviceA, opA0]]));
      expectDeepEqual(state2.appState, RoArray([opA0.forward]));
      expectDeepEqual(
        state2.heads,
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
        finalOp: Op<OpPayloads>;
      };
      backward: undefined | "open" | Op<OpPayloads>;
    };
    type OpPayloads = AddToken | AddWriter | RemoveWriter;
    type AppState = {
      tokens: RoArray<string>;
      desiredWriters: RoMap<DeviceId, "open" | Op<OpPayloads>>;
    };

    const clock = new CountingClock();
    const state = SyncState.create<AppState, OpPayloads>(
      (state, op) => {
        if (op.forward.type === "add")
          return {
            state: {
              ...state,
              tokens: [...state.tokens, op.forward.token],
            },
            backward: undefined,
          };
        else if (op.forward.type === "add writer") {
          return {
            state: {
              ...state,
              desiredWriters: mapWith(
                state.desiredWriters,
                op.forward.deviceId,
                "open",
              ),
            },
            backward: state.desiredWriters.get(op.forward.deviceId),
          };
        } else {
          const deviceId = op.forward.finalOp.deviceId;
          return {
            state: {
              ...state,
              desiredWriters: mapWith(
                state.desiredWriters,
                deviceId,
                op.forward.finalOp,
              ),
            },
            backward: state.desiredWriters.get(deviceId),
          };
        }
      },
      (state, op, backward) => {
        if (op.forward.type === "add") {
          return {
            ...state,
            tokens: state.tokens.slice(0, -1),
          };
        } else if (op.forward.type === "add writer") {
          backward = backward as AddWriter["backward"];
          return {
            ...state,
            desiredWriters:
              backward === undefined
                ? mapWithout(state.desiredWriters, op.forward.deviceId)
                : mapWith(state.desiredWriters, op.forward.deviceId, backward),
          };
        } else if (op.forward.type === "remove writer") {
          backward = backward as RemoveWriter["backward"];
          const deviceId = op.forward.finalOp.deviceId;
          return {
            ...state,
            desiredWriters:
              backward === undefined
                ? mapWithout(state.desiredWriters, deviceId)
                : mapWith(state.desiredWriters, deviceId, backward),
          };
        } else throw new AssertFailed("Unknown op type");
      },
      (state) => state.desiredWriters,
      {tokens: RoArray<string>(), desiredWriters: RoMap([[deviceB, "open"]])},
    );
    const opA0 = asType<Op<OpPayloads>>({
      deviceId: deviceA,
      forward: {type: "add", token: "a0"},
      timestamp: clock.now(),
      prev: undefined,
    });
    const opA1 = asType<Op<OpPayloads>>({
      deviceId: deviceA,
      forward: {type: "add", token: "a1"},
      timestamp: clock.now(),
      prev: opA0,
    });
    const opB0 = asType<Op<OpPayloads>>({
      deviceId: deviceB,
      forward: {type: "add writer", deviceId: deviceA},
      timestamp: clock.now(),
      prev: undefined,
    });
    const opB1 = asType<Op<OpPayloads>>({
      deviceId: deviceB,
      forward: {type: "remove writer", finalOp: opA0},
      timestamp: clock.now(),
      prev: opB0,
    });
    const opB2 = asType<Op<OpPayloads>>({
      deviceId: deviceB,
      forward: {type: "add writer", deviceId: deviceA},
      timestamp: clock.now(),
      prev: opB1,
    });
    const opB0Alternate = asType<Op<OpPayloads>>({
      deviceId: deviceB,
      forward: {type: "add", token: "b0Alternate"},
      timestamp: clock.now(),
      // TODO: would be nice to be able to chain ops of different types.
      prev: undefined,
    });
    const opA2 = asType<Op<OpPayloads>>({
      deviceId: deviceA,
      forward: {type: "add", token: "a2"},
      timestamp: clock.now(),
      prev: opA1,
    });

    it("ignores ops from a non-writer", () => {
      const state1 = state.update(RoMap([[deviceA, opA1]]));
      expectIdentical(state1.appliedHead, undefined);
    });

    it("includes ops from an added writer", () => {
      const state1 = state.update(
        RoMap<DeviceId, Op<OpPayloads>>([
          [deviceA, opA1],
          [deviceB, opB0],
        ]),
      );
      expectDeepEqual(state1.appState.tokens, RoArray(["a0", "a1"]));
      expectDeepEqual(
        state1.heads,
        RoMap<DeviceId, Op<OpPayloads>>([
          [deviceA, opA1],
          [deviceB, opB0],
        ]),
      );
    });

    it("closes a writer", () => {
      const state1 = state.update(
        RoMap<DeviceId, Op<OpPayloads>>([
          [deviceA, opA1],
          [deviceB, opB0],
        ]),
      );
      expectDeepEqual(state1.appState.tokens, RoArray(["a0", "a1"]));

      // Close deviceA after opA0.
      const state2 = state.update(
        RoMap<DeviceId, Op<OpPayloads>>([
          [deviceA, opA1],
          [deviceB, opB1],
        ]),
      );
      // a1 was excluded because b0 closed that device.
      expectDeepEqual(state2.appState.tokens, RoArray(["a0"]));
      expectDeepEqual(
        state2.heads,
        RoMap<DeviceId, Op<OpPayloads>>([
          [deviceA, opA0],
          [deviceB, opB1],
        ]),
      );
    });

    it("reopens a closed writer", () => {
      const state1 = state.update(
        RoMap<DeviceId, Op<OpPayloads>>([
          [deviceA, opA1],
          [deviceB, opB1],
        ]),
      );
      expectDeepEqual(state1.appState.tokens, RoArray(["a0"]));

      // Reopen deviceA.
      const state2 = state.update(
        RoMap<DeviceId, Op<OpPayloads>>([
          [deviceA, opA1],
          [deviceB, opB2],
        ]),
      );
      expectDeepEqual(state2.appState.tokens, RoArray(["a0", "a1"]));
      expectDeepEqual(
        state2.heads,
        RoMap<DeviceId, Op<OpPayloads>>([
          [deviceA, opA1],
          [deviceB, opB2],
        ]),
      );
    });

    it("avoids redoing later ops from a removed writer", () => {
      const state1 = state.update(
        RoMap([
          [deviceA, opA2],
          [deviceB, opB0],
        ]),
      );
      expectDeepEqual(state1.appState.tokens, RoArray(["a0", "a1", "a2"]));

      const state2 = state.update(
        RoMap([
          [deviceA, opA2],
          [deviceB, opB1],
        ]),
      );
      // Even a2, which came after b1, which closed device A, is gone.
      expectDeepEqual(state2.appState.tokens, RoArray(["a0"]));
      expectDeepEqual(
        state2.heads,
        RoMap<DeviceId, Op<OpPayloads>>([
          [deviceA, opA0],
          [deviceB, opB1],
        ]),
      );
    });

    it("undo Add/Remove Writer", () => {
      const state1 = state.update(
        RoMap<DeviceId, Op<OpPayloads>>([
          [deviceA, opA1],
          [deviceB, opB1],
        ]),
      );
      expectDeepEqual(state1.appState.tokens, RoArray(["a0"]));
      // This forces b0 and b1 to be undone.
      const state2 = state1.update(
        RoMap<DeviceId, Op<OpPayloads>>([
          [deviceA, opA1],
          [deviceB, opB0Alternate],
        ]),
      );

      expectDeepEqual(state2.appState.tokens, RoArray(["b0Alternate"]));
      expectDeepEqual(
        state2.heads,
        RoMap<DeviceId, Op<OpPayloads>>([[deviceB, opB0Alternate]]),
      );
    });
  });
});
