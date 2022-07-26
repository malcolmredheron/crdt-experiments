import seedRandom from "seed-random";
import {
  asType,
  definedOrThrow,
  mapMapToMap,
  mapWith,
  RoArray,
  RoMap,
} from "./helper/Collection";
import {
  AppliedOp,
  createPermissionedTree,
  NodeId,
  PermissionedTree,
} from "./PermissionedTree";
import {DeviceId} from "./ControlledOpSet";
import {Clock} from "./helper/Clock";
import {Timestamp} from "./helper/Timestamp";
import {expectDeepEqual, expectPreludeEqual} from "./helper/Shared.testing";

type OpType = "add" | "move" | "update";
const rootNodeId = NodeId.create("root");

//------------------------------------------------------------------------------
// Randomness
type RandomSource = {
  rand: () => number;
  seq: () => number;
};

function randomIndexInArray<T>(
  rand: RandomSource,
  array: ReadonlyArray<T>,
): number {
  return Math.floor(rand.rand() * array.length);
}

function randomInArray<T>(rand: RandomSource, array: RoArray<T>): T {
  return definedOrThrow(
    array[randomIndexInArray(rand, array)],
    "randomInArray called on empty array",
  );
}

// Makes random changes to two trees and merges them.
//
// THIS IS NOT A CORRECTNESS TEST
//
// That is, any fixes that we make because of what this test shows us must be
// tested somewhere else.
describe("PermissionedTree.monkey", function () {
  this.timeout(100000);

  const weights = new Map<OpType, number>(
    asType<Array<[OpType, number]>>([
      ["add", 10],
      ["move", 10],
      ["update", 2],
    ]),
  );

  function randomOpType(rand: RandomSource): OpType {
    const sumOfWeights = RoArray(weights.values()).reduce((a, b) => a + b, 0);
    const r = rand.rand() * sumOfWeights;
    let runningSum = 0;
    for (const [op, weight] of weights) {
      runningSum += weight;
      if (runningSum >= r) return op;
    }
    throw new Error("Should not get here");
  }

  it("randomly mutates and merges", async () => {
    let nextInSeq = 0;
    const rand: RandomSource = {
      rand: seedRandom(""),
      seq: () => nextInSeq++,
    };

    const device0 = DeviceId.create("device0");
    const initialTree = applyNewOp(createPermissionedTree(device0), device0, {
      timestamp: Timestamp.create(-1),
      type: "set writer",
      device: device0,
      targetWriter: DeviceId.create("device1"),
      priority: -1,
      status: "open",
    });
    let devices = RoMap<DeviceId, PermissionedTree>(
      Array.from(Array(4).keys()).map((index) => [
        DeviceId.create(`device${index}`),
        initialTree,
      ]),
    );

    for (let turn = 0; turn < 1000; turn++) {
      const clock: Clock = {
        now(): Timestamp {
          return Timestamp.create(turn);
        },
      };
      const log = false
        ? console.log
        : () => {
            /* do not log */
          };

      // Update a device.
      {
        const device = randomInArray(rand, Array.from(devices.keys()))!;
        const tree = devices.get(device)!;
        const opType = randomOpType(rand);

        log(`turn ${turn}, device ${device}, op type ${opType}`);
        if (opType === "update") {
          const remoteHeads = mapMapToMap(devices, (device, tree) => [
            device,
            tree.heads.get(device),
          ]);
          const tree1 = tree.update(remoteHeads);
          devices = mapWith(devices, device, tree1);
        } else {
          const op = opForOpType(clock, log, rand, opType, tree);
          const tree1 = applyNewOp(tree, device, op);
          devices = mapWith(devices, device, tree1);
        }
      }

      // Check the devices.
      if (turn % 10 === 0) {
        const remoteHeads = mapMapToMap(devices, (device, tree) => [
          device,
          tree.heads.get(device),
        ]);
        const devices1 = mapMapToMap(devices, (device, tree) => [
          device,
          tree.update(remoteHeads),
        ]);
        for (const [, tree] of devices1) {
          expectDeepEqual(
            tree.value.writers,
            Array.from(devices1.values())[0].value.writers,
          );
          expectPreludeEqual(
            tree.value.nodes,
            Array.from(devices1.values())[0].value.nodes,
          );
        }
      }
    }
  });
});

function opForOpType(
  clock: Clock,
  log: (s: string) => void,
  rand: RandomSource,
  optype: OpType,
  tree: PermissionedTree,
): AppliedOp["op"] {
  if (optype === "add") {
    const node = NodeId.create(`node${rand.seq()}`);
    const parent = randomInArray(rand, [
      rootNodeId,
      ...tree.value.nodes.keySet().toArray(),
    ]);
    return {
      timestamp: clock.now(),
      type: "set parent",
      node,
      parent,
      position: rand.rand(),
    };
  } else if (optype === "move") {
    const node = randomInArray(rand, [
      rootNodeId,
      ...tree.value.nodes.keySet().toArray(),
    ]);
    const parent = randomInArray(rand, [
      rootNodeId,
      ...tree.value.nodes.keySet().toArray(),
    ]);
    return {
      timestamp: clock.now(),
      type: "set parent",
      node,
      parent,
      position: rand.rand(),
    };
  } else {
    throw new Error("Unknown operation");
  }
}

function applyNewOp(
  tree: PermissionedTree,
  device: DeviceId,
  op: AppliedOp["op"],
): PermissionedTree {
  const opList1 = {prev: tree.heads.get(device), op};
  const heads1 = mapWith(tree.heads, device, opList1);
  return tree.update(heads1);
}
