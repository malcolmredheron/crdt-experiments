import seedRandom from "seed-random";
import {definedOrThrow} from "./helper/Collection";
import {
  AppliedOp,
  createPermissionedTree,
  DeviceId,
  NestedPermissionedTree,
  NodeId,
  NodeKey,
  ShareData,
  SharedNode,
  ShareId,
  StreamId,
} from "./NestedPermissionedTree";
import {OpList} from "./ControlledOpSet";
import {Clock} from "./helper/Clock";
import {Timestamp} from "./helper/Timestamp";
import {expectIdentical, expectPreludeEqual} from "./helper/Shared.testing";
import {HashMap, LinkedList, Option, Vector} from "prelude-ts";
import {expect} from "chai";
import {value} from "./helper/TypedValue";
import {Seq} from "prelude-ts/dist/src/Seq";
import {AssertFailed} from "./helper/Assert";

type OpType =
  | "add"
  | "add shared"
  | "move"
  | "remove"
  | "add writer"
  | "remove writer"
  | "update";
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

function randomInSeq<T>(rand: RandomSource, seq: Seq<T>): Option<T> {
  return seq.get(Math.floor(rand.rand() * seq.length()));
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
    ["add", 20],
    ["add shared", 20],
    ["move", 100],
    ["remove", 10],
    ["add writer", 5],
    // Needs to be high because often we can't find a writer to remove.
    ["remove writer", 5],
    ["update", 20],
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

    let devices = HashMap<DeviceId, NestedPermissionedTree>.ofIterable(
      Array.from(Array(4).keys()).map((index) => {
        const deviceId = DeviceId.create(`device${index}`);
        const shareId = new ShareId({creator: deviceId, id: rootNodeId});
        return [deviceId, createPermissionedTree(shareId)];
      }),
    );

    for (let turn = 0; turn < 1000; turn++) {
      let lastNowTurn = -1;
      let lastNowCounter = 0;
      const clock: Clock = {
        now(): Timestamp {
          // Occasionally we call `now` multiple times in one turn, so we need
          // to be careful not to return the same timestamp.
          if (turn === lastNowTurn) lastNowCounter++;
          else {
            lastNowTurn = turn;
            lastNowCounter = 0;
          }
          if (lastNowCounter > 9) throw new AssertFailed("Counter too high");
          return Timestamp.create(lastNowTurn + lastNowCounter / 10);
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
          const tree1 = tree.update(localHeadsForDevices(devices));
          devices = devices.put(deviceId, tree1);
        } else {
          const opOption = opForOpType(
            clock,
            log,
            rand,
            opType,
            deviceId,
            tree,
            Vector.ofIterable(devices.keySet()).sortOn((e) => value(e)),
          );
          if (opOption.isSome()) {
            const {op, shareId} = opOption.get();
            const tree1 = applyNewOp(
              tree,
              new StreamId({deviceId, shareId}),
              op,
            );
            devices = devices.put(deviceId, tree1);

            if (op.type === "set writer") {
              // When we apply a "set writer" op we only add the writer to the
              // shared node. We also need to add the shared node to the new
              // writer's tree somewhere.

              const writerTree = devices.get(op.targetWriter).getOrThrow();
              const writerTree1 = applyNewOp(
                writerTree,
                new StreamId({
                  deviceId: op.targetWriter,
                  shareId: writerTree.value.rootKey.shareId,
                }),
                {
                  timestamp: clock.now(),
                  device: deviceId,
                  type: "set parent",

                  nodeId: shareId.id,
                  nodeShareId: Option.some(shareId),
                  parentNodeId: writerTree.value.rootKey.nodeId,
                  position: rand.rand(),
                },
              );
              devices = devices.put(op.targetWriter, writerTree1);
            }
          }
        }
      }
    }

    // Just make sure that we made some nodes, shared nodes, etc.
    const tree = devices.findAny(() => true).getOrThrow()[1];
    expect(tree.value.root().children.length()).greaterThan(0);

    // Check convergence.
    const localHeadsForEachDevice = localHeadsForDevices(devices);
    const devices1: HashMap<DeviceId, NestedPermissionedTree> = devices.map(
      (device, tree) => [device, tree.update(localHeadsForEachDevice)],
    );
    const referenceSharedNodes = devices1.foldLeft(
      HashMap.of<ShareId, SharedNode & {shareData: ShareData}>(),
      (refs, [, tree]) =>
        refs.mergeWith(
          shareRoots(tree.value.root()),
          (leftSharedNode, rightSharedNode) => leftSharedNode,
        ),
    );
    // Some shared nodes should have more than one writer, otherwise it's no
    // fun.
    expectIdentical(
      referenceSharedNodes
        .filterValues((node) => node.shareData.writers.length() > 1)
        .length() > 1,
      true,
    );
    // Check that every device that has a given shared node has the same
    // version of that shared node.
    devices1.forEach(([deviceId, tree]) => {
      for (const [shareId, sharedNode] of shareRoots(tree.value.root())) {
        const referenceSharedNode = referenceSharedNodes
          .get(shareId)
          .getOrThrow();
        expectPreludeEqual(referenceSharedNode, sharedNode);
      }
    });
  });
});

function opForOpType(
  clock: Clock,
  log: (s: string) => void,
  rand: RandomSource,
  opType: Exclude<OpType, "update">,
  deviceId: DeviceId,
  tree: NestedPermissionedTree,
  deviceIds: Seq<DeviceId>,
): Option<{shareId: ShareId; op: AppliedOp["op"]}> {
  // Share ids that we can write to.
  const shareIds = tree.value
    .desiredHeads()
    .keySet()
    .filter(
      (streamId) =>
        streamId.deviceId === deviceId &&
        // We avoid share cycles by never sharing the root and only adding
        // shared nodes to the root.
        (opType !== "add writer" ||
          streamId.shareId !== tree.value.rootKey.shareId),
    )
    .map((streamId) => streamId.shareId);
  if (shareIds.isEmpty()) return Option.none();
  const shareId = randomInArray(rand, shareIds.toArray());
  const sharedNode = tree.value.nodeForNodeKey(
    new NodeKey({shareId, nodeId: shareId.id}),
  );
  if (sharedNode.isNone()) return Option.none();
  switch (opType) {
    case "add":
    case "add shared": {
      const nodeId = NodeId.create(`node${rand.seq()}`);
      const parentNodeId = randomInArray(
        rand,
        Array.from(nodesWithSameShareId(sharedNode.get()).valueIterable()),
      ).id;
      return Option.of({
        shareId,
        op: {
          timestamp: clock.now(),
          device: deviceId,
          type: "set parent",
          nodeId,
          parentNodeId,
          position: rand.rand(),
          nodeShareId:
            opType === "add shared"
              ? Option.some(
                  new ShareId({
                    creator: deviceId,
                    id: nodeId,
                  }),
                )
              : Option.none(),
        },
      });
    }
    case "move":
    case "remove": {
      const sharedNodeChildren = Array.from(
        nodesWithSameShareId(sharedNode.get()).valueIterable(),
      );
      const node = randomInArray(rand, sharedNodeChildren);
      if (node.id === shareId.id) return Option.none();
      return Option.of({
        shareId,
        op: {
          timestamp: clock.now(),
          device: deviceId,
          type: "set parent",
          nodeId: node.id,
          nodeShareId: node.shareData
            ? Option.some(node.shareId)
            : Option.none(),
          parentNodeId:
            opType === "move"
              ? randomInArray(rand, sharedNodeChildren).id
              : trashNodeId,
          position: opType === "move" ? rand.rand() : 0,
        },
      });
    }
    case "add writer": {
      if (sharedNode.isNone()) return Option.none();
      const writers = sharedNode.get().shareData!.writers;
      const newWriterId = randomInSeq(
        rand,
        Vector.ofIterable(deviceIds.removeAll(writers.keySet())),
      );
      if (newWriterId.isNone()) return Option.none();
      const now = clock.now();
      return Option.of({
        shareId,
        op: {
          timestamp: now,
          device: deviceId,
          type: "set writer",

          targetWriter: newWriterId.get(),
          priority:
            writers.get(deviceId).getOrThrow().priority - (rand.rand() % 1),
          status: "open",
        },
      });
    }
    case "remove writer": {
      if (sharedNode.isNone()) return Option.none();
      const writers = sharedNode.get().shareData!.writers;
      const ourPriority = writers.get(deviceId).getOrThrow().priority;
      const writerToRemove = randomInSeq(
        rand,
        Vector.ofIterable(
          writers.filterValues((ps) => ps.priority < ourPriority),
        ),
      );
      if (writerToRemove.isNone()) return Option.none();

      const [writerId, {priority: writerPriority}] = writerToRemove.get();
      const writerHead = tree.heads.get(
        new StreamId({deviceId: writerId, shareId: shareId}),
      );
      if (writerHead.isNone()) return Option.none();

      return Option.of({
        shareId,
        op: {
          timestamp: clock.now(),
          device: deviceId,
          type: "set writer",

          targetWriter: writerId,
          priority: writerPriority,
          status: tree.heads
            .get(new StreamId({deviceId: writerId, shareId: shareId}))
            .getOrThrow(),
        },
      });
    }
  }
}

// Returns the local stream-to-head mappings from all of the devices, "local"
// meaning the ones where the device id in the mapping matches the device's id.
//
// These are the ones published by each device, versus the remote ones that they
// have merged in from other devices.
function localHeadsForDevices(
  devices: HashMap<DeviceId, NestedPermissionedTree>,
): HashMap<StreamId, OpList<AppliedOp>> {
  return devices.flatMap((deviceId, tree) =>
    tree.heads.filter((streamId) => streamId.deviceId === deviceId),
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

function nodesWithSameShareId(node: SharedNode): HashMap<NodeId, SharedNode> {
  return node.children.foldLeft(
    HashMap.of([node.id, node]),
    (nodes, [, child]) => {
      if (node.shareId !== child.node.shareId) return nodes;
      return nodesWithSameShareId(child.node).foldLeft(
        nodes,
        (nodes, [id, childNode]) => nodes.put(id, childNode),
      );
    },
  );
}

function shareRoots(
  node: SharedNode,
): HashMap<ShareId, SharedNode & {shareData: ShareData}> {
  return node.children.foldLeft(
    node.shareData
      ? HashMap.of([node.shareId, node as SharedNode & {shareData: ShareData}])
      : HashMap.of(),
    (roots, [, child]) => {
      return shareRoots(child.node).foldLeft(
        roots,
        (roots, [shareId, childNode]) => roots.put(shareId, childNode),
      );
    },
  );
}
