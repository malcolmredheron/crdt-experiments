import {DoOp, OpBase} from "./ControlledOpSet";
import {HashSet, WithEquality} from "prelude-ts";

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
  doOp: (value: Value, op: Op, streamIds: HashSet<StreamId>) => Value,
): DoOp<Value, PersistentAppliedOp<Value, Op>, StreamId> {
  return (value, op, streamIds) => ({
    value: doOp(value, op, streamIds),
    appliedOp: {op, undoInfo: value},
  });
}

export function persistentUndoOp<Value, Op extends OpBase>(
  value: Value,
  appliedOp: PersistentAppliedOp<Value, Op>,
): Value {
  return appliedOp.undoInfo;
}
