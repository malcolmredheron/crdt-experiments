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

type OpPayloadsBase = Readonly<{
  forward: unknown;
  backward: unknown;
}>;

export type Op<OpPayloads extends OpPayloadsBase> = Readonly<{
  deviceId: DeviceId;
  timestamp: Timestamp;
  // Previous op from the same device.
  prev: Op<OpPayloads> | undefined;

  forward: OpPayloads["forward"];
}>;

type AppliedOp<OpPayloads extends OpPayloadsBase> = Readonly<{
  // Previous applied op on this device, regardless of the author.
  prev: AppliedOp<OpPayloads> | undefined;

  op: Op<OpPayloads>;
  backward: OpPayloads["backward"];
}>;

type DoOp<AppState, OpPayloads extends OpPayloadsBase> = (
  state: AppState,
  deviceId: DeviceId,
  forward: OpPayloads["forward"],
) => {
  state: AppState;
  backward: OpPayloads["backward"];
};

type UndoOp<AppState, OpPayloads extends OpPayloadsBase> = (
  state: AppState,
  payloads: OpPayloads,
) => AppState;

type DesiredDeviceHeads<AppState, OpPayloads extends OpPayloadsBase> = (
  state: AppState,
) => RoMap<DeviceId, "open" | Op<OpPayloads>>;

export class SyncState<AppState, OpPayloads extends OpPayloadsBase> {
  static create<AppState, OpPayloads extends OpPayloadsBase>(
    doOp: DoOp<AppState, OpPayloads>,
    undoOp: UndoOp<AppState, OpPayloads>,
    desiredDeviceHeads: DesiredDeviceHeads<AppState, OpPayloads>,
    appState: AppState,
  ): SyncState<AppState, OpPayloads> {
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
    readonly doOp: DoOp<AppState, OpPayloads>,
    readonly undoOp: UndoOp<AppState, OpPayloads>,
    readonly desiredDeviceHeads: DesiredDeviceHeads<AppState, OpPayloads>,

    readonly appState: AppState,
    readonly headAppliedOp: AppliedOp<OpPayloads> | undefined,
    readonly deviceHeads: RoMap<DeviceId, Op<OpPayloads>>,
  ) {}

  update(
    remoteHeads: RoMap<DeviceId, Op<OpPayloads>>,
  ): SyncState<AppState, OpPayloads> {
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
    headRemoteOp: undefined | Op<OpPayloads>,
  ): SyncState<AppState, OpPayloads> {
    const timestampToUndoPast = SyncState.earliestDivergentTimestamp(
      headRemoteOp,
      this.deviceHeads.get(deviceId),
    );

    const {state: state1, undoneOps} =
      this.undoToBeforeTimestamp(timestampToUndoPast);
    const newRemoteOps = new Array<Op<OpPayloads>>();
    for (
      let remoteOp: Op<OpPayloads> | undefined = headRemoteOp;
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

  static earliestDivergentTimestamp<OpPayloads extends OpPayloadsBase>(
    left: Op<OpPayloads> | undefined,
    right: Op<OpPayloads> | undefined,
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
    state: SyncState<AppState, OpPayloads>;
    undoneOps: RoArray<Op<OpPayloads>>;
  } {
    const undoneOps = asType<Array<Op<OpPayloads>>>([]);
    let state: SyncState<AppState, OpPayloads> = this;
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

  private doOnce(op: Op<OpPayloads>): SyncState<AppState, OpPayloads> {
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

  private undoOnce(): SyncState<AppState, OpPayloads> {
    const appliedOp = this.headAppliedOp;
    if (appliedOp === undefined)
      throw new AssertFailed("Attempt to undo but no ops");
    const appState1 = this.undoOp(this.appState, {
      forward: appliedOp.op.forward,
      backward: appliedOp.backward,
    } as OpPayloads);
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
