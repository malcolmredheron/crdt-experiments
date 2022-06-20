import {asType, definedOrThrow, mapWith, RoMap} from "./helper/Collection";
import {DeviceId, Op, SyncState} from "./SyncState";

type SetWriter = {
  forward: {
    type: "set writer";
    deviceId: DeviceId;
    priority: number;
    status: "open" | Op<OpPayloads>;
  };
  backward: AppState;
};

type OpPayloads = SetWriter;

type AppState = {
  writers: RoMap<DeviceId, {priority: number; status: "open" | Op<OpPayloads>}>;
};

export function createPermissionedTree(
  owner: DeviceId,
): SyncState<AppState, OpPayloads> {
  return SyncState<AppState, OpPayloads>.create(
    (state, op) => {
      const authorPriority = definedOrThrow(
        state.writers.get(op.deviceId),
        "Cannot find writer entry for op author",
      ).priority;
      const targetPriority = state.writers.get(op.forward.deviceId)?.priority;
      if (targetPriority !== undefined && targetPriority <= authorPriority)
        return {state, backward: state};
      if (op.forward.priority <= authorPriority)
        return {state, backward: state};

      return {
        state: {
          ...state,
          writers: mapWith(state.writers, op.forward.deviceId, {
            priority: op.forward.priority,
            status: op.forward.status,
          }),
        },
        backward: state,
      };
    },
    (state, op, backward) => backward,
    (state) => RoMap(),
    asType<AppState>({
      writers: RoMap([[owner, {priority: Number.MAX_VALUE, status: "open"}]]),
    }),
  );
}
