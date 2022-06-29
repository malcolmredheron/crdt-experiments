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

export type OpBase = {timestamp: Timestamp};
type AppliedOpBase = Readonly<{
  op: OpBase;
  undoInfo: unknown;
}>;

export type OpList<AppliedOp extends AppliedOpBase> = Readonly<{
  // Previous op from the same device.
  prev: OpList<AppliedOp> | undefined;
  op: AppliedOp["op"];
}>;

type AppliedOpList<AppliedOp extends AppliedOpBase> = Readonly<{
  // Previous applied op on this device, regardless of the author.
  prev: AppliedOpList<AppliedOp> | undefined;
  appliedOp: AppliedOp;
}>;

export type DoOp<Value, AppliedOp extends AppliedOpBase> = (
  value: Value,
  op: AppliedOp["op"],
) => {
  value: Value;
  appliedOp: AppliedOp;
};

export type UndoOp<Value, AppliedOp extends AppliedOpBase> = (
  value: Value,
  appliedOp: AppliedOp,
) => Value;

type DesiredHeads<Value, AppliedOp extends AppliedOpBase> = (
  value: Value,
) => RoMap<DeviceId, "open" | OpList<AppliedOp>>;

export class ControlledOpSet<Value, AppliedOp extends AppliedOpBase> {
  static create<Value, AppliedOp extends AppliedOpBase>(
    doOp: DoOp<Value, AppliedOp>,
    undoOp: UndoOp<Value, AppliedOp>,
    desiredHeads: DesiredHeads<Value, AppliedOp>,
    value: Value,
  ): ControlledOpSet<Value, AppliedOp> {
    return new ControlledOpSet(
      doOp,
      undoOp,
      desiredHeads,
      value,
      undefined,
      RoMap(),
    );
  }

  private constructor(
    readonly doOp: DoOp<Value, AppliedOp>,
    readonly undoOp: UndoOp<Value, AppliedOp>,
    readonly desiredHeads: DesiredHeads<Value, AppliedOp>,

    readonly value: Value,
    readonly appliedHead: AppliedOpList<AppliedOp> | undefined,
    readonly heads: RoMap<DeviceId, OpList<AppliedOp>>,
  ) {}

  update(
    remoteHeads: RoMap<DeviceId, undefined | OpList<AppliedOp>>,
  ): ControlledOpSet<Value, AppliedOp> {
    const abstractDesiredHeads = this.desiredHeads(this.value);
    const filteredAbstractDesiredHeads = RoMap(
      Array.from(abstractDesiredHeads.entries()).filter(
        ([deviceId]) => remoteHeads.get(deviceId) !== undefined,
      ),
    );
    const desiredHeads = mapMapToMap(
      filteredAbstractDesiredHeads,
      (deviceId, openOrOp) => [
        deviceId,
        asType<OpList<AppliedOp>>(
          openOrOp === "open" ? remoteHeads.get(deviceId)! : openOrOp,
        ),
      ],
    );
    if (ControlledOpSet.headsEqual(desiredHeads, this.heads)) return this;

    const {value, appliedHead, ops} = ControlledOpSet.commonStateAndDesiredOps(
      this.undoOp,
      desiredHeads,
      this.heads,
      this.value,
      this.appliedHead,
    );

    // const undidBackTo = appliedHead?.appliedOp.op.timestamp;
    // const undidFrom = this.appliedHead?.appliedOp.op.timestamp;
    // if (undidBackTo !== undidFrom)
    //   console.log("Undid back to ", undidBackTo, "from", undidFrom);
    // console.log("doing ops", ops.length);

    const {value: appState1, appliedHead: appliedHead1} = ops.reduce(
      ({value, appliedHead}, op) =>
        ControlledOpSet.doOnce(this.doOp, value, appliedHead, op),
      {value, appliedHead},
    );
    const this1 = new ControlledOpSet(
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
    Value,
    AppliedOp extends AppliedOpBase,
  >(
    undoOp: UndoOp<Value, AppliedOp>,
    desiredHeads: RoMap<DeviceId, OpList<AppliedOp>>,
    actualHeads: RoMap<DeviceId, OpList<AppliedOp>>,
    value: Value,
    appliedHead: undefined | AppliedOpList<AppliedOp>,
  ): {
    value: Value;
    appliedHead: undefined | AppliedOpList<AppliedOp>;
    ops: RoArray<AppliedOp["op"]>;
  } {
    const ops = new Array<AppliedOp["op"]>();

    while (!ControlledOpSet.headsEqual(desiredHeads, actualHeads)) {
      const {heads: nextRemainingDesiredHeads, op: desiredOp} =
        ControlledOpSet.undoHeadsOnce(desiredHeads);
      const {heads: nextActualHeads, op: actualOp} =
        ControlledOpSet.undoHeadsOnce(actualHeads);

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
        ({value, appliedHead} = ControlledOpSet.undoOnce(
          undoOp,
          value,
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
        ({value, appliedHead} = ControlledOpSet.undoOnce(
          undoOp,
          value,
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
      value: value,
      appliedHead: appliedHead,
      ops,
    };
  }

  private static headsEqual<AppliedOp extends AppliedOpBase>(
    left: RoMap<DeviceId, OpList<AppliedOp>>,
    right: RoMap<DeviceId, OpList<AppliedOp>>,
  ): boolean {
    if (left.size !== right.size) return false;
    for (const [deviceId, leftHead] of left) {
      const rightHead = right.get(deviceId);
      if (leftHead !== rightHead) return false;
    }
    return true;
  }

  private static undoHeadsOnce<AppliedOp extends AppliedOpBase>(
    heads: RoMap<DeviceId, OpList<AppliedOp>>,
  ): {
    op: undefined | AppliedOp["op"];
    heads: RoMap<DeviceId, OpList<AppliedOp>>;
  } {
    if (heads.size === 0) return {op: undefined, heads};

    const [newestDeviceId, newestOpList] = Array.from(heads.entries()).reduce(
      (winner, current) => {
        return winner[1].op.timestamp > current[1].op.timestamp
          ? winner
          : current;
      },
    );
    return {
      op: newestOpList.op,
      heads: newestOpList.prev
        ? mapWith(heads, newestDeviceId, newestOpList.prev)
        : mapWithout(heads, newestDeviceId),
    };
  }

  private static doOnce<Value, AppliedOp extends AppliedOpBase>(
    doOp: DoOp<Value, AppliedOp>,
    value: Value,
    appliedHead: undefined | AppliedOpList<AppliedOp>,
    op: AppliedOp["op"],
  ): {value: Value; appliedHead: AppliedOpList<AppliedOp>} {
    const {value: appState1, appliedOp} = doOp(value, op);
    return {
      value: appState1,
      appliedHead: {
        appliedOp,
        prev: appliedHead,
      },
    };
  }

  private static undoOnce<Value, AppliedOp extends AppliedOpBase>(
    undoOp: UndoOp<Value, AppliedOp>,
    value: Value,
    appliedHead: AppliedOpList<AppliedOp>,
  ): {
    value: Value;
    appliedHead: undefined | AppliedOpList<AppliedOp>;
  } {
    if (appliedHead === undefined)
      throw new AssertFailed("Attempt to undo but no ops");
    const appState1 = undoOp(value, appliedHead.appliedOp);
    return {
      value: appState1,
      appliedHead: appliedHead.prev,
    };
  }
}
