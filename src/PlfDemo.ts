import {Timestamp} from "./helper/Timestamp";
import {
  arraySortedByKey,
  asType,
  mapWith,
  mapWithout,
  RoArray,
  RoMap,
} from "./helper/Collection";
import {TypedValue, value} from "./helper/TypedValue";
import {AssertFailed} from "./helper/Assert";

export class DeviceId extends TypedValue<"DeviceId", string> {}

type PayloadsBase = Readonly<{
  forward: unknown;
  backward: unknown;
}>;

export type Op<Payloads extends PayloadsBase> = Readonly<{
  deviceId: DeviceId;
  timestamp: Timestamp;
  // Previous op from the same device.
  prev: Op<Payloads> | undefined;

  forward: Payloads["forward"];
}>;

type AppliedOp<Payloads extends PayloadsBase> = Readonly<{
  // Previous applied op on this device, regardless of the author.
  prev: AppliedOp<Payloads> | undefined;

  op: Op<Payloads>;
  backward: Payloads["backward"];
  backwardRemoteHeadOps: RoArray<Op<Payloads>>;
}>;

type DoOp<AppState, Payloads extends PayloadsBase> = (
  state: AppState,
  deviceId: DeviceId,
  forward: Payloads["forward"],
  ensureMerged: (headRemoteOp: Op<Payloads>) => void,
) =>
  | {
      state: AppState;
      backward: Payloads["backward"];
    }
  | "skip";

type UndoOp<AppState, Payloads extends PayloadsBase> = (
  state: AppState,
  forward: Payloads["forward"],
  backward: Payloads["backward"],
) => AppState;

export class SyncState<AppState, Payloads extends PayloadsBase> {
  static create<AppState, Payloads extends PayloadsBase>(
    doOp: DoOp<AppState, Payloads>,
    undoOp: UndoOp<AppState, Payloads>,
    appState: AppState,
  ): SyncState<AppState, Payloads> {
    return new SyncState(doOp, undoOp, appState, undefined, RoMap());
  }

  private constructor(
    readonly doOp: DoOp<AppState, Payloads>,
    readonly undoOp: UndoOp<AppState, Payloads>,
    readonly appState: AppState,
    readonly headAppliedOp: AppliedOp<Payloads> | undefined,
    readonly deviceOps: RoMap<DeviceId, Op<Payloads>>,
  ) {}

  // TODO: We'd like to be able to remove all ops from another device, but this
  // method can't do that because it needs the device id from the op.
  mergeFrom(headRemoteOp: Op<Payloads>): SyncState<AppState, Payloads> {
    const timestampToUndoPast = SyncState.earliestDivergentTimestamp(
      headRemoteOp,
      this.deviceOps.get(headRemoteOp.deviceId),
    );

    const {state: state1, undoneOps} =
      this.undoToBeforeTimestamp(timestampToUndoPast);
    const newRemoteOps = new Array<Op<Payloads>>();
    for (
      let remoteOp: Op<Payloads> | undefined = headRemoteOp;
      remoteOp && remoteOp.timestamp >= timestampToUndoPast;
      remoteOp = remoteOp.prev
    ) {
      newRemoteOps.push(remoteOp);
    }
    const opsToApply = arraySortedByKey(
      [
        ...undoneOps.filter((op) => op.deviceId !== headRemoteOp.deviceId),
        ...newRemoteOps,
      ],
      (op) => value(op.timestamp),
    );

    return opsToApply.reduce((state, op) => state.doOnce(op), state1);
  }

  static earliestDivergentTimestamp<Payloads extends PayloadsBase>(
    left: Op<Payloads> | undefined,
    right: Op<Payloads> | undefined,
  ): Timestamp {
    let earliestLeft = undefined;
    let earliestRight = undefined;
    while (true) {
      if (left === right)
        // Either both undefined or both pointing to the same op
        break;
      if (left && (!right || left!.timestamp > right.timestamp)) {
        earliestLeft = left;
        left = left!.prev;
        continue;
      }
      if (right && (!left || right.timestamp > left.timestamp)) {
        earliestRight = right;
        right = right.prev;
        continue;
      }
    }
    return Timestamp.create(
      // Returns infinity if the array is empty, which is perfect -- no need to
      // undo anything.
      Math.min(
        ...[earliestLeft, earliestRight]
          .filter((op) => op !== undefined)
          .map((op) => value(op!.timestamp)),
      ),
    );
  }

  private undoToBeforeTimestamp(timestampToUndoPast: Timestamp): {
    state: SyncState<AppState, Payloads>;
    undoneOps: RoArray<Op<Payloads>>;
  } {
    const undoneOps = asType<Array<Op<Payloads>>>([]);
    let state: SyncState<AppState, Payloads> = this;
    while (
      state.headAppliedOp &&
      state.headAppliedOp.op.timestamp >= timestampToUndoPast
    ) {
      undoneOps.push(state.headAppliedOp.op);
      state = state.undoOnce();
    }
    return {
      state,
      undoneOps: undoneOps,
    };
  }

  private doOnce(op: Op<Payloads>): SyncState<AppState, Payloads> {
    class ReplaceStartingState extends Error {
      constructor(readonly state: SyncState<AppState, Payloads>) {
        super("ReplaceStartingState");
      }
    }
    const oldRemoteHeadOps = new Array<Op<Payloads>>();
    for (let state: SyncState<AppState, Payloads> = this; ; ) {
      try {
        const ensureMerged = (headRemoteOp: Op<Payloads>): void => {
          const exisingRemoteOp = state.deviceOps.get(headRemoteOp.deviceId);
          if (exisingRemoteOp !== headRemoteOp) {
            oldRemoteHeadOps.push(exisingRemoteOp!);
            throw new ReplaceStartingState(state.mergeFrom(headRemoteOp));
          }
        };
        const doOpReturnValue = state.doOp(
          state.appState,
          op.deviceId,
          op.forward,
          ensureMerged,
        );
        // `this`, not `state`, so that doOp can't call ensureMerged and then
        // skip the op. If this happens, we'll drop the results of ensureMerged.
        if (doOpReturnValue === "skip") return this;
        const {state: appState1, backward} = doOpReturnValue;
        if (state.deviceOps.get(op.deviceId) !== op.prev) {
          throw new AssertFailed(
            "Attempt to apply an op without its predecessor",
          );
        }
        return new SyncState(
          state.doOp,
          state.undoOp,
          appState1,
          {
            op,
            backward,
            prev: state.headAppliedOp,
            backwardRemoteHeadOps: oldRemoteHeadOps,
          },
          mapWith(state.deviceOps, op.deviceId, op),
        );
      } catch (e) {
        if (e instanceof ReplaceStartingState) {
          state = e.state;
          continue;
        }
        throw e;
      }
    }
  }

  private undoOnce(): SyncState<AppState, Payloads> {
    const appliedOp = this.headAppliedOp;
    if (appliedOp === undefined)
      throw new AssertFailed("Attempt to undo but no ops");
    const appState1 = this.undoOp(
      this.appState,
      appliedOp.op.forward,
      appliedOp.backward,
    );
    const previousHeadForDevice = appliedOp.op.prev;
    const deviceOps1 = previousHeadForDevice
      ? mapWith(this.deviceOps, appliedOp.op.deviceId, previousHeadForDevice)
      : mapWithout(this.deviceOps, appliedOp.op.deviceId);
    const state1 = new SyncState(
      this.doOp,
      this.undoOp,
      appState1,
      appliedOp.prev,
      deviceOps1,
    );
    const state2 = appliedOp.backwardRemoteHeadOps.reduce(
      (state, remoteHeadOp) => state.mergeFrom(remoteHeadOp),
      state1,
    );
    return state2;
  }
}
