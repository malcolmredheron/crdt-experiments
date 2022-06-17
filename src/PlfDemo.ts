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
}>;

type DoOp<AppState, Payloads extends PayloadsBase> = (
  state: AppState,
  deviceId: DeviceId,
  forward: Payloads["forward"],
) => {
  state: AppState;
  backward: Payloads["backward"];
};

type UndoOp<AppState, Payloads extends PayloadsBase> = (
  state: AppState,
  payloads: Payloads,
) => AppState;

type DesiredDeviceHeads<AppState, Payloads extends PayloadsBase> = (
  state: AppState,
) => RoMap<DeviceId, "open" | Op<Payloads>>;

export class SyncState<AppState, Payloads extends PayloadsBase> {
  static create<AppState, Payloads extends PayloadsBase>(
    doOp: DoOp<AppState, Payloads>,
    undoOp: UndoOp<AppState, Payloads>,
    desiredDeviceHeads: DesiredDeviceHeads<AppState, Payloads>,
    appState: AppState,
  ): SyncState<AppState, Payloads> {
    return new SyncState(
      doOp,
      undoOp,
      desiredDeviceHeads,
      appState,
      undefined,
      RoMap(),
    );
  }

  private constructor(
    readonly doOp: DoOp<AppState, Payloads>,
    readonly undoOp: UndoOp<AppState, Payloads>,
    readonly desiredDeviceHeads: DesiredDeviceHeads<AppState, Payloads>,

    readonly appState: AppState,
    readonly headAppliedOp: AppliedOp<Payloads> | undefined,
    readonly deviceHeads: RoMap<DeviceId, Op<Payloads>>,
  ) {}

  update(
    remoteHeads: RoMap<DeviceId, Op<Payloads>>,
  ): SyncState<AppState, Payloads> {
    const desiredHeads = this.desiredDeviceHeads(this.appState);

    // Add/update heads.
    for (const [deviceId, desiredHead] of desiredHeads.entries()) {
      if (desiredHead === "open") {
        const remoteHead = remoteHeads.get(deviceId);
        if (remoteHead && this.deviceHeads.get(deviceId) !== remoteHead) {
          const state1 = this.mergeFrom(remoteHead.deviceId, remoteHead);
          return state1.update(remoteHeads);
        }
      } else {
        if (this.deviceHeads.get(deviceId) !== desiredHead) {
          const state1 = this.mergeFrom(desiredHead.deviceId, desiredHead);
          return state1.update(remoteHeads);
        }
      }
    }

    // Remove heads.
    for (const [deviceId] of this.deviceHeads) {
      if (!desiredHeads.has(deviceId)) {
        const state1 = this.mergeFrom(deviceId, undefined);
        return state1.update(remoteHeads);
      }
    }

    return this;
  }

  private mergeFrom(
    deviceId: DeviceId,
    headRemoteOp: undefined | Op<Payloads>,
  ): SyncState<AppState, Payloads> {
    const timestampToUndoPast = SyncState.earliestDivergentTimestamp(
      headRemoteOp,
      this.deviceHeads.get(deviceId),
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
      [...undoneOps.filter((op) => op.deviceId !== deviceId), ...newRemoteOps],
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
    const doOpReturnValue = this.doOp(this.appState, op.deviceId, op.forward);

    const {state: appState1, backward} = doOpReturnValue;
    if (this.deviceHeads.get(op.deviceId) !== op.prev) {
      throw new AssertFailed("Attempt to apply an op without its predecessor");
    }

    return new SyncState(
      this.doOp,
      this.undoOp,
      this.desiredDeviceHeads,
      appState1,
      {
        op,
        backward,
        prev: this.headAppliedOp,
      },
      mapWith(this.deviceHeads, op.deviceId, op),
    );
  }

  private undoOnce(): SyncState<AppState, Payloads> {
    const appliedOp = this.headAppliedOp;
    if (appliedOp === undefined)
      throw new AssertFailed("Attempt to undo but no ops");
    const appState1 = this.undoOp(this.appState, {
      forward: appliedOp.op.forward,
      backward: appliedOp.backward,
    } as Payloads);
    const previousHeadForDevice = appliedOp.op.prev;
    const deviceOps1 = previousHeadForDevice
      ? mapWith(this.deviceHeads, appliedOp.op.deviceId, previousHeadForDevice)
      : mapWithout(this.deviceHeads, appliedOp.op.deviceId);
    return new SyncState(
      this.doOp,
      this.undoOp,
      this.desiredDeviceHeads,
      appState1,
      appliedOp.prev,
      deviceOps1,
    );
  }
}
