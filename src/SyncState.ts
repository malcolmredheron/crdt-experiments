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
    const abstractDesiredHeads = this.desiredDeviceHeads(this.appState);
    const filteredAbstractDesiredHeads = RoMap(
      Array.from(abstractDesiredHeads.entries()).filter(([deviceId, head]) =>
        remoteHeads.has(deviceId),
      ),
    );
    let desiredHeads = mapMapToMap(
      filteredAbstractDesiredHeads,
      (deviceId, openOrOp) => [
        deviceId,
        asType<Op<OpPayloads>>(
          openOrOp === "open" ? remoteHeads.get(deviceId)! : openOrOp,
        ),
      ],
    );
    let actualHeads = this.deviceHeads;
    let actualState: SyncState<AppState, OpPayloads> = this;
    if (this.headsEqual(desiredHeads, actualHeads)) return this;

    const ops = new Array<Op<OpPayloads>>();
    while (!this.headsEqual(desiredHeads, actualHeads)) {
      const {prev: prevDesiredHeads, op: desiredOp} =
        this.previousHeads(desiredHeads);
      const {prev: prevActualHeads, op: actualOp} =
        this.previousHeads(actualHeads);

      if (
        desiredOp &&
        (!actualOp || desiredOp.timestamp > actualOp.timestamp)
      ) {
        desiredHeads = prevDesiredHeads;
        ops.push(desiredOp);
      } else if (
        actualOp &&
        (!desiredOp || actualOp.timestamp > desiredOp.timestamp)
      ) {
        actualHeads = prevActualHeads;
        actualState = actualState.undoOnce();
      } else if (
        desiredOp &&
        actualOp &&
        desiredOp.timestamp === actualOp.timestamp
      ) {
        desiredHeads = prevDesiredHeads;
        ops.push(desiredOp);
        actualHeads = prevActualHeads;
        actualState = actualState.undoOnce();
      } else {
        throw new AssertFailed(
          "If neither op exists then the heads should be equal",
        );
      }
    }

    const actualState1 = ops.reduceRight(
      (state, op) => state.doOnce(op),
      actualState,
    );

    return actualState1.update(remoteHeads);
  }

  headsEqual(
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

  previousHeads(heads: RoMap<DeviceId, Op<OpPayloads>>): {
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
