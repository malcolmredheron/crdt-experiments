import {DeviceId, Op, SyncState} from "./PlfDemo";
import {asType, mapWith, mapWithout, RoArray, RoMap} from "./helper/Collection";
import {CountingClock} from "./helper/Clock.testing";
import {expectDeepEqual, expectIdentical} from "./helper/Shared.testing";
import {AssertFailed} from "./helper/Assert";

describe("PlfDemo", () => {
  const deviceA = DeviceId.create("a");
  const deviceB = DeviceId.create("b");

  describe("basic", () => {
    type AppState = RoArray<string>;
    type Payloads = {
      forward: string;
      backward: undefined;
    };

    const clock = new CountingClock();
    const state = SyncState.create<AppState, Payloads>(
      (state, deviceId, forward) => {
        return {state: [...state, forward], backward: undefined};
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
    const opA0 = asType<Op<Payloads>>({
      deviceId: deviceA,
      forward: "a0",
      timestamp: clock.now(),
      prev: undefined,
    });
    const opA1 = asType<Op<Payloads>>({
      deviceId: deviceA,
      forward: "a1",
      timestamp: clock.now(),
      prev: opA0,
    });
    const opB0 = asType<Op<Payloads>>({
      deviceId: deviceB,
      forward: "b0",
      timestamp: clock.now(),
      prev: undefined,
    });

    it("update merges single new op", () => {
      const state1 = state.update(RoMap([[deviceA, opA0]]));
      expectDeepEqual(state1.appState, RoArray([opA0.forward]));
      expectDeepEqual(
        state1.deviceHeads,
        RoMap<DeviceId, Op<Payloads>>([[deviceA, opA0]]),
      );
    });

    it("update merges multiple new ops", () => {
      const state1 = state.update(RoMap([[deviceA, opA1]]));
      expectDeepEqual(state1.appState, RoArray([opA0.forward, opA1.forward]));
      expectDeepEqual(
        state1.deviceHeads,
        RoMap<DeviceId, Op<Payloads>>([[deviceA, opA1]]),
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
        state2.deviceHeads,
        RoMap<DeviceId, Op<Payloads>>([[deviceA, opA0]]),
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
      backward: undefined | "open" | Op<Payloads>;
    };
    type RemoveWriter = {
      forward: {
        type: "remove writer";
        finalOp: Op<Payloads>;
      };
      backward: undefined | "open" | Op<Payloads>;
    };
    type Payloads = AddToken | AddWriter | RemoveWriter;
    type AppState = {
      tokens: RoArray<string>;
      desiredWriters: RoMap<DeviceId, "open" | Op<Payloads>>;
    };

    const clock = new CountingClock();
    const state = SyncState.create<AppState, Payloads>(
      (state, deviceId, forward) => {
        if (forward.type === "add")
          return {
            state: {
              ...state,
              tokens: [...state.tokens, forward.token],
            },
            backward: undefined,
          };
        else if (forward.type === "add writer") {
          return {
            state: {
              ...state,
              desiredWriters: mapWith(
                state.desiredWriters,
                forward.deviceId,
                "open",
              ),
            },
            backward: state.desiredWriters.get(forward.deviceId),
          };
        } else {
          const deviceId = forward.finalOp.deviceId;
          return {
            state: {
              ...state,
              desiredWriters: mapWith(
                state.desiredWriters,
                deviceId,
                forward.finalOp,
              ),
            },
            backward: state.desiredWriters.get(deviceId),
          };
        }
      },
      (state, p) => {
        if (p.forward.type === "add") {
          return {
            ...state,
            tokens: state.tokens.slice(0, -1),
          };
        } else if (p.forward.type === "add writer") {
          p = p as AddWriter;
          return {
            ...state,
            desiredWriters:
              p.backward === undefined
                ? mapWithout(state.desiredWriters, p.forward.deviceId)
                : mapWith(state.desiredWriters, p.forward.deviceId, p.backward),
          };
        } else if (p.forward.type === "remove writer") {
          p = p as RemoveWriter;
          const deviceId = p.forward.finalOp.deviceId;
          return {
            ...state,
            desiredWriters:
              p.backward === undefined
                ? mapWithout(state.desiredWriters, deviceId)
                : mapWith(state.desiredWriters, deviceId, p.backward),
          };
        } else throw new AssertFailed("Unknown op type");
      },
      (state) => state.desiredWriters,
      {tokens: RoArray<string>(), desiredWriters: RoMap([[deviceB, "open"]])},
    );
    const opA0 = asType<Op<Payloads>>({
      deviceId: deviceA,
      forward: {type: "add", token: "a0"},
      timestamp: clock.now(),
      prev: undefined,
    });
    const opA1 = asType<Op<Payloads>>({
      deviceId: deviceA,
      forward: {type: "add", token: "a1"},
      timestamp: clock.now(),
      prev: opA0,
    });
    const opB0 = asType<Op<Payloads>>({
      deviceId: deviceB,
      forward: {type: "add writer", deviceId: deviceA},
      timestamp: clock.now(),
      prev: undefined,
    });
    const opB1 = asType<Op<Payloads>>({
      deviceId: deviceB,
      forward: {type: "remove writer", finalOp: opA0},
      timestamp: clock.now(),
      prev: opB0,
    });
    const opB2 = asType<Op<Payloads>>({
      deviceId: deviceB,
      forward: {type: "add writer", deviceId: deviceA},
      timestamp: clock.now(),
      prev: opB1,
    });
    const opB0Alternate = asType<Op<Payloads>>({
      deviceId: deviceB,
      forward: {type: "add", token: "b0Alternate"},
      timestamp: clock.now(),
      // TODO: would be nice to be able to chain ops of different types.
      prev: undefined,
    });
    const opA2 = asType<Op<Payloads>>({
      deviceId: deviceA,
      forward: {type: "add", token: "a2"},
      timestamp: clock.now(),
      prev: opA1,
    });

    it("ignores ops from a non-writer", () => {
      const state1 = state.update(RoMap([[deviceA, opA1]]));
      expectIdentical(state1.headAppliedOp, undefined);
    });

    it("includes ops from an added writer", () => {
      const state1 = state.update(
        RoMap<DeviceId, Op<Payloads>>([
          [deviceA, opA1],
          [deviceB, opB0],
        ]),
      );
      expectDeepEqual(state1.appState.tokens, RoArray(["a0", "a1"]));
      expectDeepEqual(
        state1.deviceHeads,
        RoMap<DeviceId, Op<Payloads>>([
          [deviceA, opA1],
          [deviceB, opB0],
        ]),
      );
    });

    it("closes a writer", () => {
      const state1 = state.update(
        RoMap<DeviceId, Op<Payloads>>([
          [deviceA, opA1],
          [deviceB, opB0],
        ]),
      );
      expectDeepEqual(state1.appState.tokens, RoArray(["a0", "a1"]));

      // Close deviceA after opA0.
      const state2 = state.update(
        RoMap<DeviceId, Op<Payloads>>([
          [deviceA, opA1],
          [deviceB, opB1],
        ]),
      );
      // a1 was excluded because b0 closed that device.
      expectDeepEqual(state2.appState.tokens, RoArray(["a0"]));
      expectDeepEqual(
        state2.deviceHeads,
        RoMap<DeviceId, Op<Payloads>>([
          [deviceA, opA0],
          [deviceB, opB1],
        ]),
      );
    });

    it("reopens a closed writer", () => {
      const state1 = state.update(
        RoMap<DeviceId, Op<Payloads>>([
          [deviceA, opA1],
          [deviceB, opB1],
        ]),
      );
      expectDeepEqual(state1.appState.tokens, RoArray(["a0"]));

      // Reopen deviceA.
      const state2 = state.update(
        RoMap<DeviceId, Op<Payloads>>([
          [deviceA, opA1],
          [deviceB, opB2],
        ]),
      );
      expectDeepEqual(state2.appState.tokens, RoArray(["a0", "a1"]));
      expectDeepEqual(
        state2.deviceHeads,
        RoMap<DeviceId, Op<Payloads>>([
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
        state2.deviceHeads,
        RoMap<DeviceId, Op<Payloads>>([
          [deviceA, opA0],
          [deviceB, opB1],
        ]),
      );
    });

    it("undo Add/Remove Writer", () => {
      const state1 = state.update(
        RoMap<DeviceId, Op<Payloads>>([
          [deviceA, opA1],
          [deviceB, opB1],
        ]),
      );
      expectDeepEqual(state1.appState.tokens, RoArray(["a0"]));
      // This forces b0 and b1 to be undone.
      const state2 = state1.update(
        RoMap<DeviceId, Op<Payloads>>([
          [deviceA, opA1],
          [deviceB, opB0Alternate],
        ]),
      );

      expectDeepEqual(state2.appState.tokens, RoArray(["b0Alternate"]));
      expectDeepEqual(
        state2.deviceHeads,
        RoMap<DeviceId, Op<Payloads>>([[deviceB, opB0Alternate]]),
      );
    });
  });
});
