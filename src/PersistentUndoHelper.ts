import {DoOp, OpBase} from "./ControlledOpSet";
import {ConsLinkedList, HashMap, WithEquality} from "prelude-ts";

// If you use ControlledOpSet with a value that is entirely readonly/persistent
// then this will keep prior values around and relieve you of writing undoOp
// functions.

export type PersistentAppliedOp<Value, Op extends OpBase> = Readonly<{
  op: Op;
  undoInfo: Value;
}>;

export function persistentDoOpFactory<
  Value,
  Op extends OpBase,
  StreamId extends WithEquality,
>(
  doOp: (
    value: Value,
    op: Op,
    opHeads: HashMap<StreamId, ConsLinkedList<Op>>,
  ) => Value,
): DoOp<Value, PersistentAppliedOp<Value, Op>, StreamId> {
  return (value, op, opHeads) => ({
    value: doOp(value, op, opHeads),
    appliedOp: {op, undoInfo: value},
  });
}

export function persistentUndoOp<Value, Op extends OpBase>(
  value: Value,
  appliedOp: PersistentAppliedOp<Value, Op>,
): Value {
  return appliedOp.undoInfo;
}
