import {
  ConsLinkedList,
  HashMap,
  LinkedList,
  Option,
  WithEquality,
} from "prelude-ts";
import {asType, throwError} from "./helper/Collection";
import {Timestamp} from "./helper/Timestamp";

// ##CantCompareHeads
//
// We can't compare head maps because ops are simple JS structures. Also, we
// don't want to walk the entire list -- because ops are unique, we can just
// compare the identity of the head ops.
export function headsEqual<
  Op extends {timestamp: Timestamp},
  StreamId extends WithEquality,
>(
  left: HashMap<StreamId, "open" | ConsLinkedList<Op>>,
  right: HashMap<StreamId, "open" | ConsLinkedList<Op>>,
): boolean {
  if (left.length() !== right.length()) return false;
  for (const [streamId, leftHead] of left) {
    const rightHead = right.get(streamId);
    if (leftHead !== rightHead.getOrUndefined()) return false;
  }
  return true;
}

export function undoHeadsOnce<
  Op extends {timestamp: Timestamp},
  StreamId extends WithEquality,
>(
  heads: HashMap<StreamId, ConsLinkedList<Op>>,
): Option<{
  op: Op;
  opHeads: HashMap<StreamId, ConsLinkedList<Op>>;
  remainingHeads: HashMap<StreamId, ConsLinkedList<Op>>;
}> {
  if (heads.isEmpty()) return Option.none();

  const newestStreamAndOpList = heads.foldLeft<
    Option<{
      op: Op;
      opHeads: HashMap<StreamId, ConsLinkedList<Op>>;
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
  const remainingHeads1 = opHeads.foldLeft(heads, (heads, [streamId, head]) => {
    const head1 = head.tail().getOrElse(LinkedList.of());
    return LinkedList.isNotEmpty(head1)
      ? heads.put(streamId, head1)
      : heads.remove(streamId);
  });
  return Option.some({
    op,
    opHeads,
    remainingHeads: remainingHeads1,
  });
}

export function concreteHeadsForAbstractHeads<
  Op extends {timestamp: Timestamp},
  StreamId extends WithEquality,
>(
  universe: HashMap<StreamId, LinkedList<Op>>,
  abstractHeads: HashMap<StreamId, "open" | LinkedList<Op>>,
): HashMap<StreamId, LinkedList<Op>> {
  return abstractHeads.flatMap((streamId, openOrOp) =>
    openOrOp === "open"
      ? asType<Option<[StreamId, LinkedList<Op>][]>>(
          universe.get(streamId).map((remoteHead) => [[streamId, remoteHead]]),
        ).getOrElse([])
      : [[streamId, openOrOp]],
  );
}
