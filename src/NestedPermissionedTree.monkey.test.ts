import seedRandom from "seed-random";
import {asType, definedOrThrow} from "./helper/Collection";
import {
  AppliedOp,
  createPermissionedTree,
  DeviceId,
  NestedPermissionedTree,
  NodeId,
  ShareId,
  StreamId,
} from "./NestedPermissionedTree";
import {OpList} from "./ControlledOpSet";
import {Clock} from "./helper/Clock";
import {Timestamp} from "./helper/Timestamp";
import {expectPreludeEqual} from "./helper/Shared.testing";
import {HashMap, LinkedList} from "prelude-ts";
import {expect} from "chai";

type OpType = "add" | "add shared" | "move" | "remove" | "update";
const rootNodeId = NodeId.create("root");
// A node that isn't part of the tree, and which we move nodes into in order to
// remove them from the tree
const trashNodeId = NodeId.create("trash");

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

function randomInArray<T>(rand: RandomSource, array: ReadonlyArray<T>): T {
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
describe("NestedPermissionedTree.monkey", function () {
  this.timeout(100000);

  const weights = HashMap.of<OpType, number>(
    ["add", 10],
    ["add shared", 2],
    ["move", 10],
    ["remove", 1],
    ["update", 2],
  );

  function randomOpType(rand: RandomSource): OpType {
    const sumOfWeights = weights.foldLeft(0, (sum, [, weight]) => sum + weight);
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
    const shareId = new ShareId({creator: device0, id: "root"});
    const initialTree = applyNewOp(
      createPermissionedTree(shareId),
      new StreamId({deviceId: device0, shareId}),
      {
        timestamp: Timestamp.create(-1),
        type: "set writer",
        device: device0,
        targetWriter: DeviceId.create("device1"),
        priority: -1,
        status: "open",
      },
    );
    let devices = HashMap<DeviceId, NestedPermissionedTree>.ofIterable(
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
        const deviceId = randomInArray(rand, Array.from(devices.keySet()))!;
        const tree = devices.get(deviceId).getOrThrow();
        const opType = randomOpType(rand);

        log(`turn ${turn}, device ${deviceId}, op type ${opType}`);
        if (opType === "update") {
          const tree1 = tree.update(localHeadsForDevices(shareId, devices));
          devices = devices.put(deviceId, tree1);
        } else {
          const {op, shareId} = opForOpType(
            clock,
            log,
            rand,
            opType,
            deviceId,
            tree,
          );
          const tree1 = applyNewOp(tree, new StreamId({deviceId, shareId}), op);
          devices = devices.put(deviceId, tree1);
        }
      }

      // Check the devices.
      if (turn % 10 === 0) {
        const localHeadsForEachDevice = localHeadsForDevices(shareId, devices);
        const devices1: HashMap<DeviceId, NestedPermissionedTree> = devices.map(
          (device, tree) => [device, tree.update(localHeadsForEachDevice)],
        );
        devices1.reduce((left, right) => {
          expectPreludeEqual(left[1].value, right[1].value);
          expectPreludeEqual(left[1].value, right[1].value);
          return left;
        });
      }
    }
    // Just make sure that we made some nodes.
    expect(
      devices
        .get(device0)
        .getOrThrow()
        .value.sharedNodes.get(shareId)
        .getOrThrow()
        .nodes.length(),
    ).greaterThan(0);
  });
});

function opForOpType(
  clock: Clock,
  log: (s: string) => void,
  rand: RandomSource,
  opType: Exclude<OpType, "update">,
  deviceId: DeviceId,
  tree: NestedPermissionedTree,
): {shareId: ShareId; op: AppliedOp["op"]} {
  const shareId = randomInArray(
    rand,
    tree.value
      .desiredHeads()
      .keySet()
      .map((streamId) => streamId.shareId)
      .toArray(),
  );
  const sharedNode = tree.value.sharedNodeForId(shareId);
  switch (opType) {
    case "add":
    case "add shared": {
      const node = NodeId.create(`node${rand.seq()}`);
      const parent = randomInArray(rand, [
        rootNodeId,
        ...sharedNode.nodes.keySet().toArray(),
      ]);
      return {
        shareId,
        op: {
          timestamp: clock.now(),
          device: deviceId,
          type: "create node",
          node,
          parent,
          position: rand.rand(),
          shareId:
            opType === "add shared"
              ? new ShareId({creator: deviceId, id: "" + rand.seq()})
              : undefined,
        },
      };
    }
    case "move": {
      const node = randomInArray(rand, [
        rootNodeId,
        ...sharedNode.nodes.keySet().toArray(),
      ]);
      const parent = randomInArray(rand, [
        rootNodeId,
        ...sharedNode.nodes.keySet().toArray(),
      ]);
      return {
        shareId,
        op: {
          timestamp: clock.now(),
          device: deviceId,
          type: "set parent",
          node,
          parent,
          position: rand.rand(),
        },
      };
    }
    case "remove": {
      const node = randomInArray(rand, [
        rootNodeId,
        ...sharedNode.nodes.keySet().toArray(),
      ]);
      return {
        shareId,
        op: {
          timestamp: clock.now(),
          device: deviceId,
          type: "set parent",
          node,
          parent: trashNodeId,
          position: 0,
        },
      };
    }
  }
}

// Returns the local stream-to-head mappings from all of the devices, "local"
// meaning the ones where the device id in the mapping matches the device's id.
//
// These are the ones published by each device, versus the remote ones that they
// have merged in from other devices.
function localHeadsForDevices(
  shareId: ShareId,
  devices: HashMap<DeviceId, NestedPermissionedTree>,
): HashMap<StreamId, OpList<AppliedOp>> {
  return devices.flatMap((deviceId, tree) =>
    tree.heads
      .get(new StreamId({deviceId, shareId}))
      .map((head) => [
        asType<[StreamId, OpList<AppliedOp>]>([
          new StreamId({deviceId, shareId}),
          head,
        ]),
      ])
      .getOrElse([]),
  );
}

function applyNewOp(
  tree: NestedPermissionedTree,
  streamId: StreamId,
  op: AppliedOp["op"],
): NestedPermissionedTree {
  const opList1 = tree.heads
    .get(streamId)
    .map((ops) => ops.prepend(op))
    .getOrCall(() => LinkedList.of(op));
  const heads1 = tree.heads.put(streamId, opList1);
  return tree.update(heads1);
}
