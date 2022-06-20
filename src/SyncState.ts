import {Timestamp} from "./helper/Timestamp";
import {
  asType,
  mapMapToMap,
  mapWith,
  mapWithout,
  RoArray,
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

type DesiredHeads<AppState, OpPayloads extends OpPayloadsBase> = (
  state: AppState,
) => RoMap<DeviceId, "open" | Op<OpPayloads>>;

export class SyncState<AppState, OpPayloads extends OpPayloadsBase> {
  static create<AppState, OpPayloads extends OpPayloadsBase>(
    doOp: DoOp<AppState, OpPayloads>,
    undoOp: UndoOp<AppState, OpPayloads>,
    desiredHeads: DesiredHeads<AppState, OpPayloads>,
    appState: AppState,
  ): SyncState<AppState, OpPayloads> {
    return new SyncState(
      doOp,
      undoOp,
      desiredHeads,
      appState,
      undefined,
      RoMap(),
    );
  }

  private constructor(
    readonly doOp: DoOp<AppState, OpPayloads>,
    readonly undoOp: UndoOp<AppState, OpPayloads>,
    readonly desiredHeads: DesiredHeads<AppState, OpPayloads>,

    readonly appState: AppState,
    readonly appliedHead: AppliedOp<OpPayloads> | undefined,
    readonly heads: RoMap<DeviceId, Op<OpPayloads>>,
  ) {}

  update(
    remoteHeads: RoMap<DeviceId, Op<OpPayloads>>,
  ): SyncState<AppState, OpPayloads> {
    const abstractDesiredHeads = this.desiredHeads(this.appState);
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
    if (SyncState.headsEqual(desiredHeads, this.heads)) return this;

    const {appState, appliedHead, ops} = SyncState.commonStateAndDesiredOps(
      this.undoOp,
      desiredHeads,
      this.heads,
      this.appState,
      this.appliedHead,
    );
    const {appState: appState1, appliedHead: appliedHead1} = ops.reduce(
      ({appState, appliedHead}, op) =>
        SyncState.doOnce(this.doOp, appState, appliedHead, op),
      {appState, appliedHead},
    );
    const this1 = new SyncState(
      this.doOp,
      this.undoOp,
      this.desiredHeads,
      appState1,
      appliedHead1,
      desiredHeads,
    );

    // Update again, in case we caused any changes in the desired heads.
    return this1.update(remoteHeads);
  }

  private static commonStateAndDesiredOps<
    AppState,
    OpPayloads extends OpPayloadsBase,
  >(
    undoOp: UndoOp<AppState, OpPayloads>,
    desiredHeads: RoMap<DeviceId, Op<OpPayloads>>,
    actualHeads: RoMap<DeviceId, Op<OpPayloads>>,
    appState: AppState,
    appliedHead: undefined | AppliedOp<OpPayloads>,
  ): {
    appState: AppState;
    appliedHead: undefined | AppliedOp<OpPayloads>;
    ops: RoArray<Op<OpPayloads>>;
  } {
    const ops = new Array<Op<OpPayloads>>();

    while (!SyncState.headsEqual(desiredHeads, actualHeads)) {
      const {heads: nextRemainingDesiredHeads, op: desiredOp} =
        SyncState.undoHeadsOnce(desiredHeads);
      const {heads: nextActualHeads, op: actualOp} =
        SyncState.undoHeadsOnce(actualHeads);

      if (
        desiredOp &&
        (!actualOp || desiredOp.timestamp > actualOp.timestamp)
      ) {
        desiredHeads = nextRemainingDesiredHeads;
        ops.push(desiredOp);
      } else if (
        actualOp &&
        (!desiredOp || actualOp.timestamp > desiredOp.timestamp)
      ) {
        actualHeads = nextActualHeads;
        ({appState, appliedHead} = SyncState.undoOnce(
          undoOp,
          appState,
          appliedHead!,
        ));
      } else if (
        desiredOp &&
        actualOp &&
        desiredOp.timestamp === actualOp.timestamp
      ) {
        desiredHeads = nextRemainingDesiredHeads;
        ops.push(desiredOp);
        actualHeads = nextActualHeads;
        ({appState, appliedHead} = SyncState.undoOnce(
          undoOp,
          appState,
          appliedHead!,
        ));
      } else {
        throw new AssertFailed(
          "If neither op exists then the heads should be equal",
        );
      }
    }

    ops.reverse();
    return {
      appState: appState,
      appliedHead: appliedHead,
      ops,
    };
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

  private static undoHeadsOnce<OpPayloads extends OpPayloadsBase>(
    heads: RoMap<DeviceId, Op<OpPayloads>>,
  ): {
    op: undefined | Op<OpPayloads>;
    heads: RoMap<DeviceId, Op<OpPayloads>>;
  } {
    if (heads.size === 0) return {op: undefined, heads};

    const [newestDeviceId, newestOp] = Array.from(heads.entries()).reduce(
      (winner, current) => {
        return winner[1].timestamp > current[1].timestamp ? winner : current;
      },
    );
    return {
      op: newestOp,
      heads: newestOp.prev
        ? mapWith(heads, newestDeviceId, newestOp.prev)
        : mapWithout(heads, newestDeviceId),
    };
  }

  private static doOnce<AppState, OpPayloads extends OpPayloadsBase>(
    doOp: DoOp<AppState, OpPayloads>,
    appState: AppState,
    appliedHead: undefined | AppliedOp<OpPayloads>,
    op: Op<OpPayloads>,
  ): {appState: AppState; appliedHead: AppliedOp<OpPayloads>} {
    const {state: appState1, backward} = doOp(appState, op);
    return {
      appState: appState1,
      appliedHead: {
        op,
        backward,
        prev: appliedHead,
      },
    };
  }

  private static undoOnce<AppState, OpPayloads extends OpPayloadsBase>(
    undoOp: UndoOp<AppState, OpPayloads>,
    appState: AppState,
    appliedHead: AppliedOp<OpPayloads>,
  ): {
    appState: AppState;
    appliedHead: undefined | AppliedOp<OpPayloads>;
  } {
    const appliedOp = appliedHead;
    if (appliedOp === undefined)
      throw new AssertFailed("Attempt to undo but no ops");
    const appState1 = undoOp(appState, appliedOp.op, appliedOp.backward);
    return {
      appState: appState1,
      appliedHead: appliedOp.prev,
    };
  }
}
