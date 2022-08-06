import {DoOp, OpBase} from "./ControlledOpSet";
import {WithEquality} from "prelude-ts";

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
  DeviceId extends WithEquality,
>(
  doOp: (value: Value, op: Op, deviceId: DeviceId) => Value,
): DoOp<Value, PersistentAppliedOp<Value, Op>, DeviceId> {
  return (value, op, deviceId) => ({
    value: doOp(value, op, deviceId),
    appliedOp: {op, undoInfo: value},
  });
}

export function persistentUndoOp<Value, Op extends OpBase>(
  value: Value,
  appliedOp: PersistentAppliedOp<Value, Op>,
): Value {
  return appliedOp.undoInfo;
}
