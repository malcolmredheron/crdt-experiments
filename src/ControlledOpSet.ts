import {Timestamp} from "./helper/Timestamp";
import {asType} from "./helper/Collection";
import {TypedValue} from "./helper/TypedValue";
import {AssertFailed} from "./helper/Assert";
import {HashMap, LinkedList, Option} from "prelude-ts";
import {CaseClass} from "./helper/CaseClass";
import {Seq} from "prelude-ts/dist/src/Seq";

export class DeviceId extends TypedValue<"DeviceId", string> {}

export type OpBase = {timestamp: Timestamp};
type AppliedOpBase = Readonly<{
  op: OpBase;
  undoInfo: unknown;
}>;

export class OpList<AppliedOp extends AppliedOpBase> extends CaseClass<{
  // Previous op from the same device.
  prev: OpList<AppliedOp> | undefined;
  op: AppliedOp["op"];
}> {}

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
) => HashMap<DeviceId, "open" | OpList<AppliedOp>>;

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
      HashMap.of(),
    );
  }

  private constructor(
    readonly doOp: DoOp<Value, AppliedOp>,
    readonly undoOp: UndoOp<Value, AppliedOp>,
    readonly desiredHeads: DesiredHeads<Value, AppliedOp>,

    readonly value: Value,
    readonly appliedHead: AppliedOpList<AppliedOp> | undefined,
    readonly heads: HashMap<DeviceId, OpList<AppliedOp>>,
  ) {}

  update(
    remoteHeads: HashMap<DeviceId, OpList<AppliedOp>>,
  ): ControlledOpSet<Value, AppliedOp> {
    const abstractDesiredHeads = this.desiredHeads(this.value);
    const desiredHeads = abstractDesiredHeads.flatMap((deviceId, openOrOp) =>
      openOrOp === "open"
        ? asType<Option<[DeviceId, OpList<AppliedOp>][]>>(
            remoteHeads
              .get(deviceId)
              .map((remoteHead) => [[deviceId, remoteHead]]),
          ).getOrElse([])
        : [[deviceId, openOrOp]],
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

    const {value: appState1, appliedHead: appliedHead1} = ops.foldLeft(
      {value, appliedHead},
      ({value, appliedHead}, op) =>
        ControlledOpSet.doOnce(this.doOp, value, appliedHead, op),
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
    desiredHeads: HashMap<DeviceId, OpList<AppliedOp>>,
    actualHeads: HashMap<DeviceId, OpList<AppliedOp>>,
    value: Value,
    appliedHead: undefined | AppliedOpList<AppliedOp>,
  ): {
    value: Value;
    appliedHead: undefined | AppliedOpList<AppliedOp>;
    ops: Seq<AppliedOp["op"]>;
  } {
    let ops = LinkedList.of<AppliedOp["op"]>();

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
        ops = ops.prepend(desiredOp);
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
        ops = ops.prepend(desiredOp);
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

    return {
      value: value,
      appliedHead: appliedHead,
      ops,
    };
  }

  private static headsEqual<AppliedOp extends AppliedOpBase>(
    left: HashMap<DeviceId, OpList<AppliedOp>>,
    right: HashMap<DeviceId, OpList<AppliedOp>>,
  ): boolean {
    if (left.length() !== right.length()) return false;
    for (const [deviceId, leftHead] of left) {
      const rightHead = right.get(deviceId);
      if (leftHead !== rightHead.getOrUndefined()) return false;
    }
    return true;
  }

  private static undoHeadsOnce<AppliedOp extends AppliedOpBase>(
    heads: HashMap<DeviceId, OpList<AppliedOp>>,
  ): {
    op: undefined | AppliedOp["op"];
    heads: HashMap<DeviceId, OpList<AppliedOp>>;
  } {
    if (heads.isEmpty()) return {op: undefined, heads};

    const newestDeviceAndOpList = heads.reduce((winner, current) => {
      return winner[1].p.op.timestamp > current[1].p.op.timestamp
        ? winner
        : current;
    });
    if (Option.isNone(newestDeviceAndOpList)) return {op: undefined, heads};
    const [newestDeviceId, newestOpList] = newestDeviceAndOpList.get();
    return {
      op: newestOpList.p.op,
      heads: newestOpList.p.prev
        ? heads.put(newestDeviceId, newestOpList.p.prev)
        : heads.remove(newestDeviceId),
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
