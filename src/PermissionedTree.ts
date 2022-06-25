import {
  asType,
  definedOrThrow,
  mapMapToMap,
  mapWith,
  RoMap,
} from "./helper/Collection";
import {ControlledOpSet, DeviceId, OpList} from "./ControlledOpSet";
import {Timestamp} from "./helper/Timestamp";

export type SetWriter = {
  op: {
    timestamp: Timestamp;
    device: DeviceId;
    type: "set writer";
    targetWriter: DeviceId;
    priority: number;
    status: "open" | OpList<AppliedOp>;
  };
  undoInfo: PermissionedTreeValue;
};

export type AppliedOp = SetWriter;

type PermissionedTreeValue = {
  writers: RoMap<
    DeviceId,
    {priority: number; status: "open" | OpList<AppliedOp>}
  >;
};

type PermissionedTree = ControlledOpSet<PermissionedTreeValue, AppliedOp>;

export function createPermissionedTree(owner: DeviceId): PermissionedTree {
  return ControlledOpSet<PermissionedTreeValue, AppliedOp>.create(
    (value, op) => {
      const devicePriority = definedOrThrow(
        value.writers.get(op.device),
        "Cannot find writer entry for op author",
      ).priority;
      const writerPriority = value.writers.get(op.targetWriter)?.priority;
      if (writerPriority !== undefined && writerPriority >= devicePriority)
        return {value, appliedOp: {op, undoInfo: value}};
      if (op.priority >= devicePriority)
        return {value, appliedOp: {op, undoInfo: value}};

      return {
        value: {
          ...value,
          writers: mapWith(value.writers, op.targetWriter, {
            priority: op.priority,
            status: op.status,
          }),
        },
        appliedOp: {op, undoInfo: value},
      };
    },
    (value, {op, undoInfo}) => undoInfo,
    (value) =>
      mapMapToMap(value.writers, (device, info) => [device, info.status]),
    asType<PermissionedTreeValue>({
      writers: RoMap([[owner, {priority: 0, status: "open"}]]),
    }),
  );
}
