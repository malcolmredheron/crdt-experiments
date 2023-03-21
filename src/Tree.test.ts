import {
  AddWriter,
  advanceIteratorBeyond,
  buildDynamicPermGroup,
  buildTree,
  Device,
  DeviceId,
  DynamicPermGroup,
  DynamicPermGroupId,
  DynamicPermGroupStreamId,
  Op,
  OpStream,
  PermGroupId,
  RemoveWriter,
  StaticPermGroup,
  StaticPermGroupId,
  StreamId,
  TreeId,
  TreeParentStreamId,
  TreeValueStreamId,
} from "./Tree";
import {HashMap, HashSet, LinkedList} from "prelude-ts";
import {Timestamp} from "./helper/Timestamp";
import {expectIdentical, expectPreludeEqual} from "./helper/Shared.testing";
import {CountingClock} from "./helper/Clock.testing";
import {Clock} from "./helper/Clock";
import {headsEqual} from "./StreamHeads";

function opsList(...ops: Op[]): OpStream {
  return LinkedList.ofIterable(ops).reverse() as OpStream;
}

describe("DynamicPermGroup", () => {
  let clock: Clock;
  beforeEach(() => (clock = new CountingClock()));

  function addWriter(
    groupId: DynamicPermGroupId,
    writerId: PermGroupId,
    extras?: {},
  ): AddWriter {
    return {
      timestamp: clock.now(),
      type: "add writer",
      groupId: groupId,
      writerId: writerId,
    };
  }

  function removeWriter(
    groupId: DynamicPermGroupId,
    writerId: PermGroupId,
    extras?: {devices?: HashMap<DeviceId, Device>},
  ): RemoveWriter {
    return {
      timestamp: clock.now(),
      type: "remove writer",
      groupId: groupId,
      writerId: writerId,
      contributingDevices: extras?.devices || HashMap.of(),
    };
  }

  const deviceId = DeviceId.create("device");
  const adminId = new StaticPermGroupId({writers: HashSet.of(deviceId)});
  const rootId = new DynamicPermGroupId({admin: adminId, rest: undefined});
  const maxTimestamp = Timestamp.create(Number.MAX_SAFE_INTEGER);
  const admin = new StaticPermGroup({id: adminId, writers: adminId.writers});

  describe("desiredStreams", () => {
    it("writeable by admins but not writers", () => {
      const otherDeviceId = DeviceId.create("other device");
      const parentId = new StaticPermGroupId({
        writers: HashSet.of(otherDeviceId),
      });
      const root = new DynamicPermGroup({
        id: rootId,
        closedDevices: HashMap.of(),
        heads: HashMap.of(),
        admin: admin,
        writers: HashMap.of([
          parentId,
          new StaticPermGroup({
            id: parentId,
            writers: HashSet.of(otherDeviceId),
          }),
        ]),
      });
      expectPreludeEqual(
        root.desiredHeads(),
        HashMap.of([
          new DynamicPermGroupStreamId({
            deviceId: deviceId,
            permGroupId: rootId,
          }),
          "open" as const,
        ]),
      );
      expectPreludeEqual(
        root.writerDevices(),
        HashMap.of(
          [deviceId, "open" as const],
          [otherDeviceId, "open" as const],
        ),
      );
    });

    it("includes closed streams", () => {
      const otherDeviceId = DeviceId.create("other device");
      const parentId = new StaticPermGroupId({
        writers: HashSet.of(otherDeviceId),
      });
      const dummyId = new DynamicPermGroupId({admin: rootId, rest: "foo"});
      const otherDeviceOps = opsList(addWriter(rootId, parentId));
      const otherStreamId = new DynamicPermGroupStreamId({
        permGroupId: dummyId,
        deviceId: otherDeviceId,
      });
      const otherDevice = new Device({
        heads: HashMap.of([otherStreamId, otherDeviceOps]),
      });
      const root = new DynamicPermGroup({
        id: rootId,
        heads: HashMap.of(),
        closedDevices: HashMap.of([otherDeviceId, otherDevice]),
        admin,
        writers: HashMap.of(),
      });
      expectPreludeEqual(
        root.writerDevices(),
        HashMap.of<DeviceId, "open" | Device>(
          [deviceId, "open"],
          [otherDeviceId, otherDevice],
        ),
      );
      const child = new DynamicPermGroup({
        admin: root,
        heads: HashMap.of(),
        writers: HashMap.of(),
        closedDevices: HashMap.of(),
        id: dummyId,
      });
      expectIdentical(
        headsEqual(
          child.desiredHeads(),
          HashMap.of<StreamId, "open" | OpStream>(
            [
              new DynamicPermGroupStreamId({
                deviceId: deviceId,
                permGroupId: child.id,
              }),
              "open" as const,
            ],
            [otherStreamId, otherDeviceOps],
          ),
        ),
        true,
      );
    });
  });

  describe("buildDynamicPermGroup", () => {
    it("initial value", () => {
      const root = buildDynamicPermGroup(HashMap.of(), rootId).value;
      expectPreludeEqual(root.id, rootId);
    });

    it("applies one op", () => {
      const parentId = new DynamicPermGroupId({
        admin: adminId,
        rest: "parent",
      });
      const op = addWriter(rootId, parentId);
      const universe = HashMap.of([
        new DynamicPermGroupStreamId({
          permGroupId: rootId,
          deviceId: deviceId,
        }),
        opsList(op),
      ]);
      const root = advanceIteratorBeyond(
        buildDynamicPermGroup(universe, rootId),
        maxTimestamp,
      ).value;
      const parent = root.writers.single().getOrThrow()[1];
      expectPreludeEqual(parent.id, parentId);
    });

    it("ignores later op", () => {
      const parentId = new DynamicPermGroupId({
        admin: adminId,
        rest: "parent",
      });
      const op = addWriter(rootId, parentId);
      const universe = HashMap.of([
        new DynamicPermGroupStreamId({
          permGroupId: rootId,
          deviceId: deviceId,
        }),
        opsList(op),
      ]);
      expectIdentical(op.timestamp, Timestamp.create(0));
      const root = advanceIteratorBeyond(
        buildDynamicPermGroup(universe, rootId),
        Timestamp.create(-1),
      ).value;
      expectPreludeEqual(root.writers, HashMap.of());
    });

    it("applies one op, adds a parent and updates it with an earlier op", () => {
      const parentId = new DynamicPermGroupId({
        admin: adminId,
        rest: "parent",
      });
      const grandparentId = new DynamicPermGroupId({
        admin: adminId,
        rest: "grandparent",
      });
      const universe = HashMap.of(
        [
          new DynamicPermGroupStreamId({
            permGroupId: parentId,
            deviceId: deviceId,
          }),
          opsList(addWriter(parentId, grandparentId)),
        ],
        [
          new DynamicPermGroupStreamId({
            permGroupId: rootId,
            deviceId: deviceId,
          }),
          opsList(addWriter(rootId, parentId)),
        ],
      );
      const root = advanceIteratorBeyond(
        buildDynamicPermGroup(universe, rootId),
        maxTimestamp,
      ).value;
      expectIdentical(
        headsEqual(
          root.heads,
          universe.filterKeys((streamId) =>
            streamId.permGroupId.equals(rootId),
          ),
        ),
        true,
      );
      const parent = root.writers.single().getOrThrow()[1];
      expectPreludeEqual(parent.id, parentId);
      const grandparent = (parent as DynamicPermGroup).writers
        .single()
        .getOrThrow()[1];
      expectPreludeEqual(grandparent.id, grandparentId);
    });

    it("applies one op, adds a parent and updates it with a later op", () => {
      const parentId = new DynamicPermGroupId({
        admin: adminId,
        rest: "parent",
      });
      const grandparentId = new DynamicPermGroupId({
        admin: adminId,
        rest: "grandparent",
      });
      const universe = HashMap.of(
        [
          new DynamicPermGroupStreamId({
            permGroupId: rootId,
            deviceId: deviceId,
          }),
          opsList(addWriter(rootId, parentId)),
        ],
        [
          new DynamicPermGroupStreamId({
            permGroupId: parentId,
            deviceId: deviceId,
          }),
          opsList(addWriter(parentId, grandparentId)),
        ],
      );
      const root = advanceIteratorBeyond(
        buildDynamicPermGroup(universe, rootId),
        maxTimestamp,
      ).value;
      expectIdentical(
        headsEqual(
          root.heads,
          universe.filterKeys((streamId) =>
            streamId.permGroupId.equals(rootId),
          ),
        ),
        true,
      );
      const parent = root.writers.single().getOrThrow()[1];
      expectPreludeEqual(parent.id, parentId);
      const grandparent = (parent as DynamicPermGroup).writers
        .single()
        .getOrThrow()[1];
      expectPreludeEqual(grandparent.id, grandparentId);
    });

    // Doing all of this on a parent forces us to handle nested resets.
    it("(to a parent) applies one op, adds a parent and updates the root with an earlier op from that parent", () => {
      const deviceAId = DeviceId.create("device a");
      const deviceBId = DeviceId.create("device b");
      const childId = new DynamicPermGroupId({
        admin: rootId,
        rest: "child",
      });
      const parentAId = new StaticPermGroupId({
        writers: HashSet.of(deviceAId),
      });
      const parentBId = new StaticPermGroupId({
        writers: HashSet.of(deviceBId),
      });
      const universe = HashMap.of(
        // This op is earlier than the second one, but isn't part of the
        // original desired heads for the root.
        [
          new DynamicPermGroupStreamId({
            permGroupId: childId,
            deviceId: deviceAId,
          }),
          opsList(addWriter(childId, parentBId)),
        ],
        [
          new DynamicPermGroupStreamId({
            permGroupId: rootId,
            deviceId: deviceId,
          }),
          opsList(addWriter(rootId, parentAId)),
        ],
      );
      const iterator = advanceIteratorBeyond(
        buildDynamicPermGroup(universe, childId),
        maxTimestamp,
      );
      const root = iterator.value;
      expectIdentical(
        headsEqual(
          root.heads,
          universe.filterKeys((streamId) =>
            streamId.permGroupId.equals(root.id),
          ),
        ),
        true,
      );
      expectPreludeEqual(
        root.writerDevices(),
        HashMap.ofIterable(
          [deviceId, deviceAId, deviceBId].map((deviceId) => [
            deviceId,
            "open" as const,
          ]),
        ),
      );
    });

    it("closes devices for removed writers", () => {
      const deviceAId = DeviceId.create("device a");
      const deviceBId = DeviceId.create("device b");
      const parentAId = new StaticPermGroupId({
        writers: HashSet.of(deviceAId),
      });
      const parentBId = new StaticPermGroupId({
        writers: HashSet.of(deviceBId),
      });
      const adminId = new DynamicPermGroupId({
        admin: new StaticPermGroupId({writers: HashSet.of(deviceId)}),
        rest: undefined,
      });
      const rootId = new DynamicPermGroupId({admin: adminId, rest: undefined});

      const deviceA = new Device({
        heads: HashMap.of([
          new DynamicPermGroupStreamId({
            permGroupId: rootId,
            deviceId: deviceAId,
          }),
          opsList(addWriter(rootId, parentBId)),
        ]),
      });
      const root = advanceIteratorBeyond(
        buildDynamicPermGroup(
          HashMap.of([
            new DynamicPermGroupStreamId({
              permGroupId: adminId,
              deviceId: deviceId,
            }),
            opsList(
              // Add otherDeviceId as a writer.
              addWriter(adminId, parentAId),
              // Remove deviceAId as a writer.
              removeWriter(adminId, parentAId, {
                devices: HashMap.of([deviceAId, deviceA]),
              }),
            ),
          ]),
          rootId,
        ),
        maxTimestamp,
      ).value;
      // Only the stream for the removed writer gets closed.
      expectPreludeEqual(root.closedDevices, HashMap.of([deviceAId, deviceA]));
      // deviceB was added as a writer by the ops in the stream in the
      // remove-writer op. Seeing it here shows that we correctly applied the
      // closed stream.
      expectPreludeEqual(root.writers.keySet(), HashSet.of(parentBId));
    });

    it("avoids cycles", () => {
      const otherGroup = new DynamicPermGroupId({
        admin: adminId,
        rest: "otherGroup",
      });

      const root = advanceIteratorBeyond(
        buildDynamicPermGroup(
          HashMap.of(
            [
              new DynamicPermGroupStreamId({
                permGroupId: rootId,
                deviceId: deviceId,
              }),
              opsList(addWriter(rootId, otherGroup)),
            ],
            [
              new DynamicPermGroupStreamId({
                permGroupId: otherGroup,
                deviceId: deviceId,
              }),
              opsList(addWriter(otherGroup, rootId)),
            ],
          ),
          rootId,
        ),
        maxTimestamp,
      ).value;
      // The root should have otherGroup as a writer. otherGroup should not have
      // anyone as a writer, because the attempt to create that edge camge
      // second and was ignored.
      expectPreludeEqual(root.writers.keySet(), HashSet.of(otherGroup));
      expectPreludeEqual(
        (
          root.writers.single().getOrThrow()[1] as DynamicPermGroup
        ).writers.keySet(),
        HashSet.of(),
      );
    });
  });
});

describe("Tree", () => {
  let clock: Clock;
  beforeEach(() => (clock = new CountingClock()));

  const deviceId = DeviceId.create("device");
  const adminId = new StaticPermGroupId({writers: HashSet.of(deviceId)});
  const rootId = new TreeId({permGroupId: adminId, rest: "root"});
  const maxTimestamp = Timestamp.create(Number.MAX_SAFE_INTEGER);

  it("creates a single-node tree", () => {
    const root = advanceIteratorBeyond(
      buildTree(HashMap.of(), rootId, adminId),
      maxTimestamp,
    ).value;
    expectPreludeEqual(root.children, HashMap.of());
  });

  it("sets a parent", () => {
    const otherId = new TreeId({permGroupId: adminId, rest: "other"});
    const op = {
      type: "set parent",
      timestamp: clock.now(),
      parentId: otherId,
      childId: rootId,
    } as const;
    const root = advanceIteratorBeyond(
      buildTree(
        HashMap.of([
          new TreeParentStreamId({
            treeId: rootId,
            parentPermGroupId: adminId,
            deviceId,
          }) as StreamId,
          opsList(op),
        ]),
        rootId,
        adminId,
      ),
      maxTimestamp,
    ).value;
    expectPreludeEqual(root.parentId.getOrThrow(), otherId);
  });

  it("does not add a child if the child disagrees", () => {
    const childId = new TreeId({permGroupId: adminId, rest: "child"});
    const op = {
      type: "set parent",
      timestamp: clock.now(),
      parentId: rootId,
      childId: childId,
    } as const;
    const root = advanceIteratorBeyond(
      buildTree(
        HashMap.of([
          new TreeParentStreamId({
            treeId: childId,
            parentPermGroupId: adminId,
            deviceId,
          }) as StreamId,
          opsList(op),
        ]),
        rootId,
        adminId,
      ),
      maxTimestamp,
    ).value;
    expectPreludeEqual(root.children.keySet(), HashSet.of());
  });

  it("adds a child if the child agrees", () => {
    const childId = new TreeId({permGroupId: adminId, rest: "child"});
    const op = {
      type: "set parent",
      timestamp: clock.now(),
      parentId: rootId,
      childId: childId,
    } as const;
    const root = advanceIteratorBeyond(
      buildTree(
        HashMap.of(
          [
            new TreeValueStreamId({treeId: rootId, deviceId}) as StreamId,
            opsList(op),
          ],
          [
            new TreeParentStreamId({
              treeId: childId,
              parentPermGroupId: adminId,
              deviceId,
            }) as StreamId,
            opsList(op),
          ],
        ),
        rootId,
        adminId,
      ),
      maxTimestamp,
    ).value;
    expectPreludeEqual(root.children.keySet(), HashSet.of(childId));
  });

  // it("avoids cycles", () => {
  //   const otherId = new TreeId({permGroupId: adminId, rest: "other"});
  //   const otherInRoot = {
  //     type: "set parent",
  //     timestamp: clock.now(),
  //     parentId: rootId,
  //     childId: otherId,
  //   } as const;
  //   const rootInOther = {
  //     type: "set parent",
  //     timestamp: clock.now(),
  //     parentId: otherId,
  //     childId: rootId,
  //   } as const;
  //   const root = advanceIteratorBeyond(
  //     buildTree(
  //       HashMap.of(
  //         [
  //           new TreeValueStreamId({treeId: rootId, deviceId}) as StreamId,
  //           opsList(otherInRoot),
  //         ],
  //         [
  //           new TreeParentStreamId({
  //             treeId: otherId,
  //             parentPermGroupId: adminId,
  //             deviceId,
  //           }) as StreamId,
  //           opsList(otherInRoot),
  //         ],
  //         [
  //           new TreeValueStreamId({treeId: otherId, deviceId}) as StreamId,
  //           opsList(rootInOther),
  //         ],
  //         [
  //           new TreeParentStreamId({
  //             treeId: rootId,
  //             parentPermGroupId: adminId,
  //             deviceId,
  //           }) as StreamId,
  //           opsList(rootInOther),
  //         ],
  //       ),
  //       rootId,
  //       adminId,
  //     ),
  //     maxTimestamp,
  //   ).value;
  //   expectPreludeEqual(root.children.keySet(), HashSet.of(otherId));
  //   expectIdentical(root.parentId.isNone(), true);
  // });
});
