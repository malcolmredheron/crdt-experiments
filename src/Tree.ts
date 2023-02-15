import {ObjectValue} from "./helper/ObjectValue";
import {TypedValue} from "./helper/TypedValue";
import {Timestamp} from "./helper/Timestamp";
import {
  ConsLinkedList,
  HasEquals,
  HashMap,
  HashSet,
  LinkedList,
  Option,
  Vector,
} from "prelude-ts";
import {concreteHeadsForAbstractHeads} from "./StreamHeads";
import {
  asType,
  consTail,
  mapMapOption,
  mapValuesStable,
  throwError,
} from "./helper/Collection";
import {match, P} from "ts-pattern";
import {AssertFailed} from "./helper/Assert";

export class DeviceId extends TypedValue<"DeviceId", string> {}
export class TreeId extends TypedValue<"TreeId", string> {}
export class EdgeId extends TypedValue<"EdgeId", string> {}
export class PermGroupId extends ObjectValue<{
  writers: HashSet<DeviceId>;
}>() {}

type StreamId = {
  type: "tree" | "edge";
  deviceId: DeviceId;
} & HasEquals;
export class TreeStreamId
  extends ObjectValue<{
    type: "tree";
    deviceId: DeviceId;
    treeId: TreeId;
  }>()
  implements StreamId {}
export class EdgeStreamId
  extends ObjectValue<{
    type: "edge";
    deviceId: DeviceId;
    treeId: TreeId;
    edgeId: EdgeId;
  }>()
  implements StreamId {}

export type Op = SetEdge;
export type OpStream = ConsLinkedList<Op>;
export type AbstractHeads = HashMap<StreamId, "open" | OpStream>;
export type ConcreteHeads = HashMap<StreamId, OpStream>;

export type SetEdge = {
  timestamp: Timestamp;
  type: "set edge";

  edgeId: EdgeId;
  childId: TreeId;
  childPermGroupId: PermGroupId;

  parentId: TreeId;
};

interface InitialPersistentIterator<T> {
  value: T;
  next: () => Option<PersistentIterator<T>>;
  needsReset: boolean;
  reset: () => InitialPersistentIterator<T>;
}

interface PersistentIterator<T> {
  op: Op;
  result: InitialPersistentIterator<T>;
}

type TreeIteratorState = {
  readonly tree: Tree;
  // Unlike parentIterators, we map to PersistentIterators here because we
  // define a stream as containing an op, so we can't have a stream until after
  // the first op.
  readonly streamIterators: HashMap<StreamId, PersistentIterator<OpStream>>;
  readonly childIterators: HashMap<TreeId, InitialPersistentIterator<Tree>>;
};

export function buildUpTree(
  universe: ConcreteHeads,
  permGroupId: PermGroupId,
  treeId: TreeId,
): InitialPersistentIterator<Tree> {
  const streamIterators = concreteHeadsForAbstractHeads(
    universe,
    new Tree({
      permGroupId,
      treeId,
      edges: HashMap.of(),
    }).desiredHeads(),
  ).mapValues((stream) => streamIteratorForStream(stream));
  return buildUpTreeInternal(
    universe,
    permGroupId,
    treeId,
    streamIterators,
    HashMap.of(),
  );
}

function buildUpTreeInternal(
  universe: ConcreteHeads,
  permGroupId: PermGroupId,
  treeId: TreeId,
  streamIterators: HashMap<StreamId, PersistentIterator<OpStream>>,
  childIterators: HashMap<TreeId, InitialPersistentIterator<Tree>>,
): InitialPersistentIterator<Tree> {
  const tree = new Tree({
    permGroupId,
    treeId,
    edges: HashMap.of(),
  });
  const state = asType<TreeIteratorState>({
    tree,
    streamIterators,
    childIterators,
  });
  const iterator = {
    value: tree,
    next: () => nextIterator(universe, state),
    needsReset: false,
    reset: () => iterator,
    _state: state,
  };
  return iterator;
}

function nextIterator(
  universe: ConcreteHeads,
  state: TreeIteratorState,
): Option<PersistentIterator<Tree>> {
  const opOption = nextOp(state.streamIterators, state.childIterators);
  if (opOption.isNone()) return Option.none();
  const op = opOption.get();

  const childIterators1 = state.childIterators.mapValues((i) =>
    i
      .next()
      .map((next) => (next.op === op ? next.result : i))
      .getOrElse(i),
  );
  const childIterators2 =
    op.parentId === state.tree.treeId &&
    !childIterators1.containsKey(op.childId)
      ? childIterators1.put(
          // TODO: this should also be keyed on the child perm group id.
          op.childId,
          advanceIteratorUntil(
            buildUpTree(universe, op.childPermGroupId, op.childId),
            op.timestamp,
          ),
        )
      : childIterators1;
  const edges1 = mapValuesStable(state.tree.edges, (edge) =>
    childIterators2
      .get(edge.tree.treeId)
      .map((iterator) => edge.copy({tree: iterator.value}))
      .getOrElse(edge),
  );

  const streamIterators1 = mapMapOption(
    state.streamIterators,
    (streamId, streamIterator) => {
      return streamIterator.op === op
        ? streamIterator.result.next()
        : Option.of(streamIterator);
    },
  );

  const opHeads = mapMapOption(
    state.streamIterators,
    (streamId, streamIterator) => {
      return streamIterator.op === op
        ? Option.of(streamIterator.result.value)
        : Option.none<OpStream>();
    },
  );
  const tree1 = match({opHeads})
    .with(
      {},
      ({opHeads}) => !opHeads.isEmpty(),
      ({opHeads}) =>
        state.tree.copy({
          edges: edges1.put(
            op.edgeId,
            new Edge({
              parent: op.parentId,
              tree: childIterators2.get(op.childId).getOrThrow().value,
            }),
          ),
          // heads: state.tree.heads.mergeWith(opHeads, (v0, v1) => v1),
        }),
    )
    .with(P._, () => state.tree.copy({edges: edges1}))
    .exhaustive();

  // const addedStreamIds = tree1
  //   .openStreams()
  //   .removeAll(state.tree.openStreams());
  // const closedStreams1 = tree1.closedStreams.filterKeys(
  //   (streamId) => !addedStreamIds.contains(streamId),
  // );
  // const removedStreamIds = state.tree
  //   .openStreams()
  //   .removeAll(tree1.openStreams());
  // const closedStreams2 = closedStreams1.mergeWith(
  //   removedStreamIds.toVector().mapOption((streamId) => {
  //     return (
  //       op.contributingHeads
  //         .get(streamId)
  //         // Automatically include a closed stream for any stream that contained
  //         // this operation. Without this, it's impossible to add the first parent
  //         // of a node (which closes the bootstrap stream) since the op can't
  //         // list itself in `contributingHeads`.
  //         .orElse(opHeads.get(streamId))
  //         .map((ops) => [streamId, ops])
  //     );
  //   }),
  //   (ops0, ops1) => throwError("Should not have stream-id collision"),
  // );
  // const tree2 = tree1.copy({closedStreams: closedStreams2});

  const concreteHeads = concreteHeadsForAbstractHeads(
    universe,
    tree1.desiredHeads(),
  );
  // const headsNeedReset = !headsEqual(concreteHeads, tree1.heads);
  // const needsReset =
  //   headsNeedReset ||
  //   childIterators2.anyMatch(
  //     (nodeId, parentIterator) => parentIterator.needsReset,
  //   );
  const needsReset = false;

  const state1 = {
    tree: tree1,
    streamIterators: streamIterators1,
    childIterators: childIterators2,
  };
  return Option.of({
    op,
    result: {
      value: tree1,
      next: () => nextIterator(universe, state1),
      needsReset,
      reset: () => {
        return buildUpTreeInternal(
          universe,
          tree1.permGroupId,
          tree1.treeId,
          concreteHeads.mapValues((stream) => streamIteratorForStream(stream)),
          state1.childIterators.mapValues((iterator) => iterator.reset()),
        );
      },
      _state: state,
      _state1: state1,
      _concreteHeads: concreteHeads,
    },
  });
}

function nextOp(
  streamIterators: HashMap<StreamId, PersistentIterator<OpStream>>,
  parentIterators: HashMap<TreeId, InitialPersistentIterator<Tree>>,
): Option<Op> {
  const streamOps = streamIterators
    .mapValues((streamIterator) => streamIterator.op)
    .valueIterable();
  const parentOps = mapMapOption(parentIterators, (streamId, parentIterator) =>
    parentIterator.next().map((next) => next.op),
  ).valueIterable();
  const opOption = Vector.ofIterable([...streamOps, ...parentOps]).reduce(
    (leftOp: Op, right: Op): Op =>
      leftOp.timestamp < right.timestamp
        ? leftOp
        : right.timestamp < leftOp.timestamp
        ? right
        : leftOp === right
        ? leftOp
        : throwError("non-identical ops have the same timestamp"),
  );
  return opOption;
}

// Advances the iterator until the next value is greater than `after`.
export function advanceIteratorUntil<T>(
  iterator: InitialPersistentIterator<T>,
  after: Timestamp,
): InitialPersistentIterator<T> {
  // `iterators` and the limit of 10 times through the loop are for debugging.
  // We will have to find a more sophisticated way to handle this at some point.
  let iterators = LinkedList.of<{
    iterator: InitialPersistentIterator<T>;
    description: string;
  }>({iterator, description: "initial"});
  for (let i = 0; i < 10; i++) {
    while (true) {
      const next = iterator.next();
      if (next.isNone()) break;
      if (next.get().op.timestamp > after) break;
      iterator = next.get().result;
    }
    if (!iterator.needsReset) return iterator;
    iterators = iterators.prepend({iterator, description: "before reset"});
    iterator = iterator.reset();
    iterators = iterators.prepend({iterator, description: "after reset"});
  }
  throw new AssertFailed("Iterator did not stabilize");
}

export class Edge extends ObjectValue<{
  parent: TreeId;
  tree: Tree;
}>() {}

export class Tree extends ObjectValue<{
  readonly treeId: TreeId;
  readonly permGroupId: PermGroupId;
  // heads: ConcreteHeads;
  // closedStreams: ConcreteHeads;

  edges: HashMap<EdgeId, Edge>;
}>() {
  desiredHeads(): AbstractHeads {
    const ourHeads: AbstractHeads = HashMap.ofIterable<
      StreamId,
      "open" | OpStream
    >(
      this.permGroupId.writers.toVector().map((writer) => [
        new TreeStreamId({
          type: "tree",
          deviceId: writer,
          treeId: this.treeId,
        }),
        "open",
      ]),
    );
    return this.edges.foldLeft(ourHeads, (heads, [, edge]) =>
      HashMap.ofIterable<StreamId, "open" | OpStream>([
        ...heads,
        ...edge.tree.desiredHeads(),
      ]),
    );
  }
}

function streamIteratorForStream(
  stream: OpStream,
  next: () => Option<PersistentIterator<OpStream>> = () => Option.none(),
): PersistentIterator<OpStream> {
  const iterator: PersistentIterator<OpStream> = {
    op: stream.head().get(),
    result: {
      value: stream,
      next: next,
      needsReset: false,
      // Annoyingly, we don't have a way of resetting a stream iterator because
      // we'd need to return an InitialPersistentIterator, which we can't do
      // because the stream doesn't have a value until after the first op.
      reset: () => throwError("Not implemented"),
    },
  };
  return consTail(stream)
    .map((tail) => streamIteratorForStream(tail, () => Option.of(iterator)))
    .getOrElse(iterator);
}
