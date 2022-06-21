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

type DoOp<Value, OpPayloads extends OpPayloadsBase> = (
  value: Value,
  op: Op<OpPayloads>,
) => {
  value: Value;
  backward: OpPayloads["backward"];
};

type UndoOp<Value, OpPayloads extends OpPayloadsBase> = (
  value: Value,
  op: Op<OpPayloads>,
  backward: OpPayloads["backward"],
) => Value;

type DesiredHeads<Value, OpPayloads extends OpPayloadsBase> = (
  value: Value,
) => RoMap<DeviceId, "open" | Op<OpPayloads>>;

export class ControlledOpSet<Value, OpPayloads extends OpPayloadsBase> {
  static create<Value, OpPayloads extends OpPayloadsBase>(
    doOp: DoOp<Value, OpPayloads>,
    undoOp: UndoOp<Value, OpPayloads>,
    desiredHeads: DesiredHeads<Value, OpPayloads>,
    value: Value,
  ): ControlledOpSet<Value, OpPayloads> {
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
    readonly doOp: DoOp<Value, OpPayloads>,
    readonly undoOp: UndoOp<Value, OpPayloads>,
    readonly desiredHeads: DesiredHeads<Value, OpPayloads>,

    readonly value: Value,
    readonly appliedHead: AppliedOp<OpPayloads> | undefined,
    readonly heads: RoMap<DeviceId, Op<OpPayloads>>,
  ) {}

  update(
    remoteHeads: RoMap<DeviceId, Op<OpPayloads>>,
  ): ControlledOpSet<Value, OpPayloads> {
    const abstractDesiredHeads = this.desiredHeads(this.value);
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
    if (ControlledOpSet.headsEqual(desiredHeads, this.heads)) return this;

    const {value, appliedHead, ops} = ControlledOpSet.commonStateAndDesiredOps(
      this.undoOp,
      desiredHeads,
      this.heads,
      this.value,
      this.appliedHead,
    );
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
    OpPayloads extends OpPayloadsBase,
  >(
    undoOp: UndoOp<Value, OpPayloads>,
    desiredHeads: RoMap<DeviceId, Op<OpPayloads>>,
    actualHeads: RoMap<DeviceId, Op<OpPayloads>>,
    value: Value,
    appliedHead: undefined | AppliedOp<OpPayloads>,
  ): {
    value: Value;
    appliedHead: undefined | AppliedOp<OpPayloads>;
    ops: RoArray<Op<OpPayloads>>;
  } {
    const ops = new Array<Op<OpPayloads>>();

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

  private static doOnce<Value, OpPayloads extends OpPayloadsBase>(
    doOp: DoOp<Value, OpPayloads>,
    value: Value,
    appliedHead: undefined | AppliedOp<OpPayloads>,
    op: Op<OpPayloads>,
  ): {value: Value; appliedHead: AppliedOp<OpPayloads>} {
    const {value: appState1, backward} = doOp(value, op);
    return {
      value: appState1,
      appliedHead: {
        op,
        backward,
        prev: appliedHead,
      },
    };
  }

  private static undoOnce<Value, OpPayloads extends OpPayloadsBase>(
    undoOp: UndoOp<Value, OpPayloads>,
    value: Value,
    appliedHead: AppliedOp<OpPayloads>,
  ): {
    value: Value;
    appliedHead: undefined | AppliedOp<OpPayloads>;
  } {
    const appliedOp = appliedHead;
    if (appliedOp === undefined)
      throw new AssertFailed("Attempt to undo but no ops");
    const appState1 = undoOp(value, appliedOp.op, appliedOp.backward);
    return {
      value: appState1,
      appliedHead: appliedOp.prev,
    };
  }
}
