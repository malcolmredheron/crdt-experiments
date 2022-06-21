import {asType, definedOrThrow, mapWith, RoMap} from "./helper/Collection";
import {ControlledOpSet, DeviceId, Op} from "./ControlledOpSet";

type SetWriter = {
  forward: {
    type: "set writer";
    deviceId: DeviceId;
    priority: number;
    status: "open" | Op<OpPayloads>;
  };
  backward: PermissionedTreeValue;
};

type OpPayloads = SetWriter;

type PermissionedTreeValue = {
  writers: RoMap<DeviceId, {priority: number; status: "open" | Op<OpPayloads>}>;
};

type PermissionedTree = ControlledOpSet<PermissionedTreeValue, OpPayloads>;

export function createPermissionedTree(owner: DeviceId): PermissionedTree {
  return ControlledOpSet<PermissionedTreeValue, OpPayloads>.create(
    (value, op) => {
      const authorPriority = definedOrThrow(
        value.writers.get(op.deviceId),
        "Cannot find writer entry for op author",
      ).priority;
      const targetPriority = value.writers.get(op.forward.deviceId)?.priority;
      if (targetPriority !== undefined && targetPriority <= authorPriority)
        return {value, backward: value};
      if (op.forward.priority <= authorPriority)
        return {value, backward: value};

      return {
        value: {
          ...value,
          writers: mapWith(value.writers, op.forward.deviceId, {
            priority: op.forward.priority,
            status: op.forward.status,
          }),
        },
        backward: value,
      };
    },
    (value, op, backward) => backward,
    (value) => RoMap(),
    asType<PermissionedTreeValue>({
      writers: RoMap([[owner, {priority: Number.MAX_VALUE, status: "open"}]]),
    }),
  );
}
