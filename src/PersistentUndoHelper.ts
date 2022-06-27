import {DoOp, OpBase} from "./ControlledOpSet";

// If you use ControlledOpSet with a value that is entirely readonly/persistent
// then this will keep prior values around and relieve you of writing undoOp
// functions.

export type PersistentAppliedOp<Value, Op extends OpBase> = Readonly<{
  op: Op;
  undoInfo: Value;
}>;

export function persistentDoOpFactory<Value, Op extends OpBase>(
  doOp: (value: Value, op: Op) => Value,
): DoOp<Value, PersistentAppliedOp<Value, Op>> {
  return (value, op) => ({
    value: doOp(value, op),
    appliedOp: {op, undoInfo: value},
  });
}

export function persistentUndoOp<Value, Op extends OpBase>(
  value: Value,
  appliedOp: PersistentAppliedOp<Value, Op>,
): Value {
  return appliedOp.undoInfo;
}
