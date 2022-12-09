import {Timestamp} from "./helper/Timestamp";
import {asType, throwError} from "./helper/Collection";
import {AssertFailed} from "./helper/Assert";
import {
  ConsLinkedList,
  HashMap,
  HashSet,
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
  StreamId extends WithEquality,
> = (
  value: Value,
  op: AppliedOp["op"],
  // These are the streams and op lists that we found this op in.
  opHeads: HashMap<StreamId, OpList<AppliedOp>>,
) => {
  value: Value;
  appliedOp: AppliedOp;
};

export type UndoOp<Value, AppliedOp extends AppliedOpBase> = (
  value: Value,
  appliedOp: AppliedOp,
) => Value;

type DesiredHeads<Value, AppliedOp extends AppliedOpBase, StreamId> = (
  value: Value,
) => HashMap<StreamId, "open" | OpList<AppliedOp>>;

export class ControlledOpSet<
  Value,
  AppliedOp extends AppliedOpBase,
  StreamId extends WithEquality,
> {
  static create<
    Value,
    AppliedOp extends AppliedOpBase,
    StreamId extends WithEquality,
  >(
    doOp: DoOp<Value, AppliedOp, StreamId>,
    undoOp: UndoOp<Value, AppliedOp>,
    desiredHeads: DesiredHeads<Value, AppliedOp, StreamId>,
    value: Value,
  ): ControlledOpSet<Value, AppliedOp, StreamId> {
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
    readonly doOp: DoOp<Value, AppliedOp, StreamId>,
    readonly undoOp: UndoOp<Value, AppliedOp>,
    readonly desiredHeads: DesiredHeads<Value, AppliedOp, StreamId>,

    readonly value: Value,
    // The most recent op is at the head of the list.
    readonly appliedOps: LinkedList<AppliedOp>,
    readonly heads: HashMap<StreamId, OpList<AppliedOp>>,
  ) {}

  update(
    remoteHeads: HashMap<StreamId, OpList<AppliedOp>>,
  ): ControlledOpSet<Value, AppliedOp, StreamId> {
    const abstractDesiredHeads = this.desiredHeads(this.value);
    const desiredHeads = abstractDesiredHeads.flatMap((streamId, openOrOp) =>
      openOrOp === "open"
        ? asType<Option<[StreamId, OpList<AppliedOp>][]>>(
            remoteHeads
              .get(streamId)
              .map((remoteHead) => [[streamId, remoteHead]]),
          ).getOrElse([])
        : [[streamId, openOrOp]],
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
      ({value, appliedOps}, {op, heads}) =>
        ControlledOpSet.doOnce(this.doOp, value, appliedOps, op, heads),
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

  // This should only be used for testing. It doesn't ensure that ops are
  // applied in order. However, it is sometimes useful to force ops into an
  // opset in a test without having to arrange for the opset to subscribe to
  // various streams.
  updateWithOneOp(
    op: AppliedOp["op"],
    streamIds: HashSet<StreamId>,
  ): ControlledOpSet<Value, AppliedOp, StreamId> {
    const opHeads = HashMap.ofIterable(
      streamIds.toVector().map((streamId) => [
        streamId,
        this.heads
          .get(streamId)
          .map((head) => head.prepend(op))
          .getOrCall(() => LinkedList.of(op)),
      ]),
    );

    const {value: value1, appliedOp} = this.doOp(this.value, op, opHeads);
    const heads1 = opHeads.foldLeft(this.heads, (heads, [streamId, head]) =>
      heads.put(streamId, head),
    );
    return new ControlledOpSet(
      this.doOp,
      this.undoOp,
      this.desiredHeads,
      value1,
      this.appliedOps.prepend(appliedOp),
      heads1,
    );
  }

  private static commonStateAndDesiredOps<
    Value,
    AppliedOp extends AppliedOpBase,
    StreamId extends WithEquality,
  >(
    undoOp: UndoOp<Value, AppliedOp>,
    desiredHeads: HashMap<StreamId, OpList<AppliedOp>>,
    actualHeads: HashMap<StreamId, OpList<AppliedOp>>,
    value: Value,
    appliedOps: LinkedList<AppliedOp>,
  ): {
    value: Value;
    appliedOps: LinkedList<AppliedOp>;
    ops: Seq<{
      op: AppliedOp["op"];
      heads: HashMap<StreamId, OpList<AppliedOp>>;
    }>;
  } {
    let ops = LinkedList.of<
      Readonly<{
        op: AppliedOp["op"];
        heads: HashMap<StreamId, OpList<AppliedOp>>;
      }>
    >();

    while (!ControlledOpSet.headsEqual(desiredHeads, actualHeads)) {
      const desired = ControlledOpSet.undoHeadsOnce(desiredHeads);
      const actual = ControlledOpSet.undoHeadsOnce(actualHeads);

      if (
        desired.isSome() &&
        (actual.isNone() ||
          desired.get().op.timestamp > actual.get().op.timestamp)
      ) {
        const {remainingHeads, opHeads, op} = desired.get();
        desiredHeads = remainingHeads;
        ops = ops.prepend({op, heads: opHeads});
      } else if (
        actual.isSome() &&
        (desired.isNone() ||
          actual.get().op.timestamp > desired.get().op.timestamp)
      ) {
        const {remainingHeads} = actual.get();
        actualHeads = remainingHeads;
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
        const {remainingHeads, opHeads, op} = desired.get();
        desiredHeads = remainingHeads;
        ops = ops.prepend({op, heads: opHeads});
        actualHeads = actual.get().remainingHeads;
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

  // ##CantCompareHeads
  //
  // We can't compare head maps because ops are simple JS structures. Also, we
  // don't want to walk the entire list -- because ops are unique, we can just
  // compare the identity of the head ops.
  static headsEqual<
    AppliedOp extends AppliedOpBase,
    StreamId extends WithEquality,
  >(
    left: HashMap<StreamId, OpList<AppliedOp>>,
    right: HashMap<StreamId, OpList<AppliedOp>>,
  ): boolean {
    if (left.length() !== right.length()) return false;
    for (const [streamId, leftHead] of left) {
      const rightHead = right.get(streamId);
      if (leftHead !== rightHead.getOrUndefined()) return false;
    }
    return true;
  }

  private static undoHeadsOnce<
    AppliedOp extends AppliedOpBase,
    StreamId extends WithEquality,
  >(
    heads: HashMap<StreamId, OpList<AppliedOp>>,
  ): Option<{
    op: AppliedOp["op"];
    opHeads: HashMap<StreamId, OpList<AppliedOp>>;
    remainingHeads: HashMap<StreamId, OpList<AppliedOp>>;
  }> {
    if (heads.isEmpty()) return Option.none();

    const newestStreamAndOpList = heads.foldLeft<
      Option<{
        op: AppliedOp["op"];
        opHeads: HashMap<StreamId, OpList<AppliedOp>>;
      }>
    >(Option.none(), (winner, current) => {
      return winner.isNone()
        ? Option.some({
            op: current[1].head().get(),
            opHeads: HashMap.of(current),
          })
        : winner.get().op.timestamp > current[1].head().get().timestamp
        ? winner
        : winner.get().op === current[1].head().get()
        ? Option.some({
            op: winner.get().op,
            opHeads: winner.get().opHeads.put(current[0], current[1]),
          })
        : winner.get().op.timestamp === current[1].head().get().timestamp
        ? throwError("If ops have the same timestamp, they must be identical")
        : Option.some({
            op: current[1].head().get(),
            opHeads: HashMap.of(current),
          });
    });
    if (newestStreamAndOpList.isNone()) return Option.none();
    const {op, opHeads} = newestStreamAndOpList.get();
    const remainingHeads1 = opHeads.foldLeft(
      heads,
      (heads, [streamId, head]) => {
        const head1 = head.tail().getOrElse(LinkedList.of());
        return LinkedList.isNotEmpty(head1)
          ? heads.put(streamId, head1)
          : heads.remove(streamId);
      },
    );
    return Option.some({
      op,
      opHeads,
      remainingHeads: remainingHeads1,
    });
  }

  private static doOnce<
    Value,
    AppliedOp extends AppliedOpBase,
    StreamId extends WithEquality,
  >(
    doOp: DoOp<Value, AppliedOp, StreamId>,
    value: Value,
    appliedOps: LinkedList<AppliedOp>,
    op: AppliedOp["op"],
    heads: HashMap<StreamId, OpList<AppliedOp>>,
  ): {value: Value; appliedOps: LinkedList<AppliedOp>} {
    const {value: appState1, appliedOp} = doOp(value, op, heads);
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
