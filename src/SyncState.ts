import {Timestamp} from "./helper/Timestamp";
import {
  asType,
  mapMapToMap,
  mapWith,
  mapWithout,
  RoMap,
} from "./helper/Collection";
import {TypedValue} from "./helper/TypedValue";
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
  op: Op<OpPayloads>,
) => {
  state: AppState;
  backward: OpPayloads["backward"];
};

type UndoOp<AppState, OpPayloads extends OpPayloadsBase> = (
  state: AppState,
  op: Op<OpPayloads>,
  backward: OpPayloads["backward"],
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
    const abstractDesiredHeads = this.desiredDeviceHeads(this.appState);
    const filteredAbstractDesiredHeads = RoMap(
      Array.from(abstractDesiredHeads.entries()).filter(([deviceId]) =>
        remoteHeads.has(deviceId),
      ),
    );
    const desiredHeads = mapMapToMap(
      filteredAbstractDesiredHeads,
      (deviceId, openOrOp) => [
        deviceId,
        asType<Op<OpPayloads>>(
          openOrOp === "open" ? remoteHeads.get(deviceId)! : openOrOp,
        ),
      ],
    );
    if (SyncState.headsEqual(desiredHeads, this.deviceHeads)) return this;

    let actualState: SyncState<AppState, OpPayloads> = this;
    const ops = new Array<Op<OpPayloads>>();

    let remainingDesiredHeads = desiredHeads;
    let remainingActualHeads = this.deviceHeads;
    while (!SyncState.headsEqual(remainingDesiredHeads, remainingActualHeads)) {
      const {prev: prevRemainingDesiredHeads, op: desiredOp} =
        SyncState.previousHeads(remainingDesiredHeads);
      const {prev: prevActualHeads, op: actualOp} =
        SyncState.previousHeads(remainingActualHeads);

      if (
        desiredOp &&
        (!actualOp || desiredOp.timestamp > actualOp.timestamp)
      ) {
        remainingDesiredHeads = prevRemainingDesiredHeads;
        ops.push(desiredOp);
      } else if (
        actualOp &&
        (!desiredOp || actualOp.timestamp > desiredOp.timestamp)
      ) {
        remainingActualHeads = prevActualHeads;
        actualState = actualState.undoOnce();
      } else if (
        desiredOp &&
        actualOp &&
        desiredOp.timestamp === actualOp.timestamp
      ) {
        remainingDesiredHeads = prevRemainingDesiredHeads;
        ops.push(desiredOp);
        remainingActualHeads = prevActualHeads;
        actualState = actualState.undoOnce();
      } else {
        throw new AssertFailed(
          "If neither op exists then the heads should be equal",
        );
      }
    }

    const {appState: appState1, headAppliedOp: headAppliedOp1} =
      ops.reduceRight(
        ({appState, headAppliedOp}, op) =>
          SyncState.doOnce(this.doOp, appState, headAppliedOp, op),
        {
          appState: actualState.appState,
          headAppliedOp: actualState.headAppliedOp,
        },
      );
    const this1 = new SyncState(
      this.doOp,
      this.undoOp,
      this.desiredDeviceHeads,
      appState1,
      headAppliedOp1,
      desiredHeads,
    );

    // Update again, in case we caused any changes in the desired heads.
    return this1.update(remoteHeads);
  }

  private static headsEqual<OpPayloads extends OpPayloadsBase>(
    left: RoMap<DeviceId, Op<OpPayloads>>,
    right: RoMap<DeviceId, Op<OpPayloads>>,
  ): boolean {
    if (left.size !== right.size) return false;
    for (const [deviceId, leftHead] of left) {
      const rightHead = right.get(deviceId);
      if (leftHead !== rightHead) return false;
    }
    return true;
  }

  private static previousHeads<OpPayloads extends OpPayloadsBase>(
    heads: RoMap<DeviceId, Op<OpPayloads>>,
  ): {
    op: undefined | Op<OpPayloads>;
    prev: RoMap<DeviceId, Op<OpPayloads>>;
  } {
    if (heads.size === 0) return {op: undefined, prev: heads};

    const [newestDeviceId, newestOp] = Array.from(heads.entries()).reduce(
      (winner, current) => {
        return winner[1].timestamp > current[1].timestamp ? winner : current;
      },
    );
    return {
      op: newestOp,
      prev: newestOp.prev
        ? mapWith(heads, newestDeviceId, newestOp.prev)
        : mapWithout(heads, newestDeviceId),
    };
  }

  private static doOnce<AppState, OpPayloads extends OpPayloadsBase>(
    doOp: DoOp<AppState, OpPayloads>,
    appState: AppState,
    headAppliedOp: undefined | AppliedOp<OpPayloads>,
    op: Op<OpPayloads>,
  ): {appState: AppState; headAppliedOp: AppliedOp<OpPayloads>} {
    const {state: appState1, backward} = doOp(appState, op);
    return {
      appState: appState1,
      headAppliedOp: {
        op,
        backward,
        prev: headAppliedOp,
      },
    };
  }

  private undoOnce(): SyncState<AppState, OpPayloads> {
    const appliedOp = this.headAppliedOp;
    if (appliedOp === undefined)
      throw new AssertFailed("Attempt to undo but no ops");
    const appState1 = this.undoOp(
      this.appState,
      appliedOp.op,
      appliedOp.backward,
    );
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
