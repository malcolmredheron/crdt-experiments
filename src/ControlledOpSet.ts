import {Timestamp} from "./helper/Timestamp";
import {asType} from "./helper/Collection";
import {AssertFailed} from "./helper/Assert";
import {
  ConsLinkedList,
  HashMap,
  LinkedList,
  Option,
  WithEquality,
} from "prelude-ts";
import {Seq} from "prelude-ts/dist/src/Seq";

export type OpBase = {timestamp: Timestamp};
type AppliedOpBase = Readonly<{
  op: OpBase;
  undoInfo: unknown;
}>;

export type OpList<AppliedOp extends AppliedOpBase> = ConsLinkedList<
  AppliedOp["op"]
>;

export type DoOp<
  Value,
  AppliedOp extends AppliedOpBase,
  DeviceId extends WithEquality,
> = (
  value: Value,
  op: AppliedOp["op"],
  // This is the id of the device that published the operation that we are now
  // doing.
  deviceId: DeviceId,
) => {
  value: Value;
  appliedOp: AppliedOp;
};

export type UndoOp<Value, AppliedOp extends AppliedOpBase> = (
  value: Value,
  appliedOp: AppliedOp,
) => Value;

type DesiredHeads<Value, AppliedOp extends AppliedOpBase, DeviceId> = (
  value: Value,
) => HashMap<DeviceId, "open" | OpList<AppliedOp>>;

export class ControlledOpSet<
  Value,
  AppliedOp extends AppliedOpBase,
  DeviceId extends WithEquality,
> {
  static create<
    Value,
    AppliedOp extends AppliedOpBase,
    DeviceId extends WithEquality,
  >(
    doOp: DoOp<Value, AppliedOp, DeviceId>,
    undoOp: UndoOp<Value, AppliedOp>,
    desiredHeads: DesiredHeads<Value, AppliedOp, DeviceId>,
    value: Value,
  ): ControlledOpSet<Value, AppliedOp, DeviceId> {
    return new ControlledOpSet(
      doOp,
      undoOp,
      desiredHeads,
      value,
      LinkedList.of(),
      HashMap.of(),
    );
  }

  private constructor(
    readonly doOp: DoOp<Value, AppliedOp, DeviceId>,
    readonly undoOp: UndoOp<Value, AppliedOp>,
    readonly desiredHeads: DesiredHeads<Value, AppliedOp, DeviceId>,

    readonly value: Value,
    // The most recent op is at the head of the list.
    readonly appliedOps: LinkedList<AppliedOp>,
    readonly heads: HashMap<DeviceId, OpList<AppliedOp>>,
  ) {}

  update(
    remoteHeads: HashMap<DeviceId, OpList<AppliedOp>>,
  ): ControlledOpSet<Value, AppliedOp, DeviceId> {
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

    const {value, appliedOps, ops} = ControlledOpSet.commonStateAndDesiredOps(
      this.undoOp,
      desiredHeads,
      this.heads,
      this.value,
      this.appliedOps,
    );

    // const undidBackTo = appliedHead?.appliedOp.op.timestamp;
    // const undidFrom = this.appliedHead?.appliedOp.op.timestamp;
    // if (undidBackTo !== undidFrom)
    //   console.log("Undid back to ", undidBackTo, "from", undidFrom);
    // console.log("doing ops", ops.length);

    const {value: appState1, appliedOps: appliedOps1} = ops.foldLeft(
      {value, appliedOps},
      ({value, appliedOps}, {op, deviceId}) =>
        ControlledOpSet.doOnce(this.doOp, value, appliedOps, op, deviceId),
    );
    const this1 = new ControlledOpSet(
      this.doOp,
      this.undoOp,
      this.desiredHeads,
      appState1,
      appliedOps1,
      desiredHeads,
    );

    // Update again, in case we caused any changes in the desired heads.
    return this1.update(remoteHeads);
  }

  private static commonStateAndDesiredOps<
    Value,
    AppliedOp extends AppliedOpBase,
    DeviceId extends WithEquality,
  >(
    undoOp: UndoOp<Value, AppliedOp>,
    desiredHeads: HashMap<DeviceId, OpList<AppliedOp>>,
    actualHeads: HashMap<DeviceId, OpList<AppliedOp>>,
    value: Value,
    appliedOps: LinkedList<AppliedOp>,
  ): {
    value: Value;
    appliedOps: LinkedList<AppliedOp>;
    ops: Seq<{op: AppliedOp["op"]; deviceId: DeviceId}>;
  } {
    let ops =
      LinkedList.of<Readonly<{op: AppliedOp["op"]; deviceId: DeviceId}>>();

    while (!ControlledOpSet.headsEqual(desiredHeads, actualHeads)) {
      const desired = ControlledOpSet.undoHeadsOnce(desiredHeads);
      const actual = ControlledOpSet.undoHeadsOnce(actualHeads);

      if (
        desired.isSome() &&
        (actual.isNone() ||
          desired.get().op.timestamp > actual.get().op.timestamp)
      ) {
        const {heads, op, deviceId} = desired.get();
        desiredHeads = heads;
        ops = ops.prepend({op, deviceId});
      } else if (
        actual.isSome() &&
        (desired.isNone() ||
          actual.get().op.timestamp > desired.get().op.timestamp)
      ) {
        const {heads} = actual.get();
        actualHeads = heads;
        ({value, appliedOps} = ControlledOpSet.undoOnce(
          undoOp,
          value,
          appliedOps!,
        ));
      } else if (
        desired.isSome() &&
        actual.isSome() &&
        desired.get().op.timestamp === actual.get().op.timestamp
      ) {
        const {heads: desiredHeads1, op, deviceId} = desired.get();
        desiredHeads = desiredHeads1;
        ops = ops.prepend({op, deviceId});
        actualHeads = actual.get().heads;
        ({value, appliedOps} = ControlledOpSet.undoOnce(
          undoOp,
          value,
          appliedOps!,
        ));
      } else {
        throw new AssertFailed(
          "If neither op exists then the heads should be equal",
        );
      }
    }

    return {
      value,
      appliedOps,
      ops,
    };
  }

  static headsEqual<
    AppliedOp extends AppliedOpBase,
    DeviceId extends WithEquality,
  >(
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

  private static undoHeadsOnce<
    AppliedOp extends AppliedOpBase,
    DeviceId extends WithEquality,
  >(
    heads: HashMap<DeviceId, OpList<AppliedOp>>,
  ): Option<{
    op: AppliedOp["op"];
    deviceId: DeviceId;
    heads: HashMap<DeviceId, OpList<AppliedOp>>;
  }> {
    if (heads.isEmpty()) return Option.none();

    const newestDeviceAndOpList = heads.reduce((winner, current) => {
      return winner[1].head().get().timestamp >
        current[1].head().get().timestamp
        ? winner
        : current;
    });
    if (Option.isNone(newestDeviceAndOpList)) return Option.none();
    const [newestDeviceId, newestOpList] = newestDeviceAndOpList.get();
    const heads1 = newestOpList.tail().getOrElse(LinkedList.of());
    return Option.some({
      op: newestOpList.head().get(),
      deviceId: newestDeviceId,
      heads: LinkedList.isNotEmpty(heads1)
        ? heads.put(newestDeviceId, heads1)
        : heads.remove(newestDeviceId),
    });
  }

  private static doOnce<
    Value,
    AppliedOp extends AppliedOpBase,
    DeviceId extends WithEquality,
  >(
    doOp: DoOp<Value, AppliedOp, DeviceId>,
    value: Value,
    appliedOps: LinkedList<AppliedOp>,
    op: AppliedOp["op"],
    deviceId: DeviceId,
  ): {value: Value; appliedOps: LinkedList<AppliedOp>} {
    const {value: appState1, appliedOp} = doOp(value, op, deviceId);
    return {
      value: appState1,
      appliedOps: appliedOps.prepend(appliedOp),
    };
  }

  private static undoOnce<Value, AppliedOp extends AppliedOpBase>(
    undoOp: UndoOp<Value, AppliedOp>,
    value: Value,
    appliedOps: LinkedList<AppliedOp>,
  ): {
    value: Value;
    appliedOps: LinkedList<AppliedOp>;
  } {
    if (LinkedList.isNotEmpty(appliedOps)) {
      const appState1 = undoOp(value, appliedOps.head().get());
      return {
        value: appState1,
        appliedOps: appliedOps.tail().get(),
      };
    } else {
      throw new AssertFailed("Attempt to undo but no applied ops");
    }
  }
}
