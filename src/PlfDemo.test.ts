import {DeviceId, Op, SyncState} from "./PlfDemo";
import {asType, RoArray, RoMap, RoSet, setWithout} from "./helper/Collection";
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
      (state, deviceId, forward, ensureMerged) => {
        return {state: [...state, forward], backward: undefined};
      },
      (state, p) => {
        return state.slice(0, -1);
      },
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

    it("mergeFrom merges single new op", () => {
      const state1 = state.mergeFrom(opA0);
      expectDeepEqual(state1.appState, RoArray([opA0.forward]));
      expectDeepEqual(
        state1.deviceOps,
        RoMap<DeviceId, Op<Payloads>>([[deviceA, opA0]]),
      );
    });

    it("mergeFrom merges multiple new ops", () => {
      const state1 = state.mergeFrom(opA1);
      expectDeepEqual(state1.appState, RoArray([opA0.forward, opA1.forward]));
      expectDeepEqual(
        state1.deviceOps,
        RoMap<DeviceId, Op<Payloads>>([[deviceA, opA1]]),
      );
    });

    it("mergeFrom undoes before applying new ops", () => {
      const state1 = state.mergeFrom(opA1);
      const state2 = state1.mergeFrom(opB0);
      const state3 = state2.mergeFrom(opA1);
      expectDeepEqual(
        state3.appState,
        RoArray([opA0.forward, opA1.forward, opB0.forward]),
      );
    });

    it("mergeFrom purges newer ops from a device if needed", () => {
      const state1 = state.mergeFrom(opA1);
      const state2 = state1.mergeFrom(opA0);
      expectDeepEqual(state2.appState, RoArray([opA0.forward]));
      expectDeepEqual(
        state2.deviceOps,
        RoMap<DeviceId, Op<Payloads>>([[deviceA, opA0]]),
      );
    });
  });

  describe("write permissions", () => {
    type AppState = {tokens: RoArray<string>; removedWriters: RoSet<DeviceId>};
    type Add = {
      forward: {
        type: "add";
        token: string;
      };
      backward: undefined;
    };
    type RemoveWriter = {
      forward: {
        type: "remove writer";
        finalOp: Op<Payloads>;
      };
      backward: boolean; // true if the writer was already removed
    };
    type Payloads = Add | RemoveWriter;

    const clock = new CountingClock();
    const state = SyncState.create<AppState, Payloads>(
      (state, deviceId, forward, ensureMerged) => {
        if (state.removedWriters.has(deviceId)) return "skip";
        if (forward.type === "add")
          return {
            state: {
              ...state,
              tokens: [...state.tokens, forward.token],
            },
            backward: undefined,
          };
        else {
          ensureMerged(forward.finalOp);
          const deviceId = forward.finalOp.deviceId;
          return {
            state: {
              ...state,
              removedWriters: RoSet([...state.removedWriters, deviceId]),
            },
            backward: state.removedWriters.has(deviceId),
          };
        }
      },
      (state, p) => {
        if (p.forward.type === "add") {
          return {
            ...state,
            tokens: state.tokens.slice(0, -1),
          };
        } else if (p.forward.type === "remove writer") {
          p = p as RemoveWriter;
          return {
            ...state,
            removedWriters: p.backward
              ? state.removedWriters
              : setWithout(state.removedWriters, p.forward.finalOp.deviceId),
          };
        } else throw new AssertFailed("Unknown op type");
      },
      {tokens: RoArray<string>(), removedWriters: RoSet()},
    );
    const opA0 = asType<Op<Add>>({
      deviceId: deviceA,
      forward: {type: "add", token: "a0"},
      timestamp: clock.now(),
      prev: undefined,
    });
    const opA1 = asType<Op<Add>>({
      deviceId: deviceA,
      forward: {type: "add", token: "a1"},
      timestamp: clock.now(),
      prev: opA0,
    });
    const opB0 = asType<Op<RemoveWriter>>({
      deviceId: deviceB,
      forward: {type: "remove writer", finalOp: opA0},
      timestamp: clock.now(),
      // TODO: would be nice to be able to chain ops of different types.
      prev: undefined,
    });
    const opB0Alternate = asType<Op<Add>>({
      deviceId: deviceB,
      forward: {type: "add", token: "b0Alternate"},
      timestamp: clock.now(),
      // TODO: would be nice to be able to chain ops of different types.
      prev: undefined,
    });
    const opA2 = asType<Op<Add>>({
      deviceId: deviceA,
      forward: {type: "add", token: "a2"},
      timestamp: clock.now(),
      prev: opA1,
    });

    it("basic", () => {
      const state1 = state.mergeFrom(opA1);
      expectDeepEqual(
        state1.appState.tokens,
        RoArray([opA0.forward.token, opA1.forward.token]),
      );
      const state2 = state1.mergeFrom(opB0);

      expectDeepEqual(state2.appState.tokens, RoArray([opA0.forward.token]));
      expectIdentical(state2.deviceOps.get(deviceA), opA0);
      expectDeepEqual(
        state2.deviceOps,
        RoMap<DeviceId, Op<Payloads>>([
          [deviceA, opA0],
          [deviceB, opB0],
        ]),
      );
    });

    it("avoids redoing ops from a removed writer", () => {
      const state1 = state.mergeFrom(opA2);
      expectDeepEqual(
        state1.appState.tokens,
        RoArray([opA0.forward.token, opA1.forward.token, opA2.forward.token]),
      );
      const state2 = state1.mergeFrom(opB0);

      expectDeepEqual(state2.appState.tokens, RoArray([opA0.forward.token]));
      expectIdentical(state2.deviceOps.get(deviceA), opA0);
      expectDeepEqual(
        state2.deviceOps,
        RoMap<DeviceId, Op<Payloads>>([
          [deviceA, opA0],
          [deviceB, opB0],
        ]),
      );
    });

    it("undo RemoveWriter", () => {
      const state1 = state.mergeFrom(opA1);
      expectDeepEqual(
        state1.appState.tokens,
        RoArray([opA0.forward.token, opA1.forward.token]),
      );
      const state2 = state1.mergeFrom(opB0);
      expectDeepEqual(state2.appState.tokens, RoArray([opA0.forward.token]));
      // This forces b0 to be undone.
      const state3 = state2.mergeFrom(opB0Alternate);

      expectDeepEqual(
        state3.appState.tokens,
        RoArray([
          opA0.forward.token,
          opA1.forward.token,
          opB0Alternate.forward.token,
        ]),
      );
      expectDeepEqual(
        state3.deviceOps,
        RoMap<DeviceId, Op<Payloads>>([
          [deviceA, opA1],
          [deviceB, opB0Alternate],
        ]),
      );
    });
  });
});
