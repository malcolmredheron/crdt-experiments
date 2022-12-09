import {ControlledOpSet, OpList} from "./ControlledOpSet";
import {CountingClock} from "./helper/Clock.testing";
import {expectIdentical, expectPreludeEqual} from "./helper/Shared.testing";
import {AssertFailed} from "./helper/Assert";
import {Timestamp} from "./helper/Timestamp";
import {
  PersistentAppliedOp,
  persistentDoOpFactory,
  persistentUndoOp,
} from "./PersistentUndoHelper";
import {HashMap, LinkedList, Vector} from "prelude-ts";
import {TypedValue} from "./helper/TypedValue";

describe("ControlledOpSet", () => {
  class StreamId extends TypedValue<"StreamId", string> {}

  const streamA = StreamId.create("a");
  const streamB = StreamId.create("b");

  describe("basic", () => {
    type Value = Vector<string>;
    type AppliedOp = PersistentAppliedOp<
      Value,
      {token: string; timestamp: Timestamp}
    >;

    const clock = new CountingClock();
    const cos = ControlledOpSet.create<Value, AppliedOp, StreamId>(
      persistentDoOpFactory((value, op, opHeads) => {
        return value.append(
          `${opHeads
            .keySet()
            .toArray()
            .map((x) => "" + x)
            .sort()
            .join("/")}.${op.token}`,
        );
      }),
      persistentUndoOp,
      (value) => HashMap.of([streamA, "open"], [streamB, "open"]),
      Vector.of<string>(),
    );
    const opA0 = LinkedList.of<AppliedOp["op"]>({
      token: "a0",
      timestamp: clock.now(),
    });
    const opA1 = opA0.prepend({
      token: "a1",
      timestamp: clock.now(),
    });
    const opB0 = LinkedList.of<AppliedOp["op"]>({
      token: "b0",
      timestamp: clock.now(),
    });

    it("update merges single new op", () => {
      const cos1 = cos.update(HashMap.of([streamA, opA0]));
      expectPreludeEqual(cos1.value, Vector.of("a.a0"));
      expectIdentical(
        ControlledOpSet.headsEqual(cos1.heads, HashMap.of([streamA, opA0])),
        true,
      );
    });

    it("update handles same op in multiple streams", () => {
      const cos1 = cos.update(HashMap.of([streamA, opA0], [streamB, opA1]));
      expectPreludeEqual(cos1.value, Vector.of("a/b.a0", "b.a1"));
      expectIdentical(
        ControlledOpSet.headsEqual(
          cos1.heads,
          HashMap.of([streamA, opA0], [streamB, opA1]),
        ),
        true,
      );
    });

    it("update merges multiple new ops", () => {
      const cos1 = cos.update(HashMap.of([streamA, opA1]));
      expectPreludeEqual(cos1.value, Vector.of("a.a0", "a.a1"));
      expectIdentical(
        ControlledOpSet.headsEqual(cos1.heads, HashMap.of([streamA, opA1])),
        true,
      );
    });

    it("update undoes before applying new ops", () => {
      const cos1 = cos.update(HashMap.of([streamA, opA0]));
      const cos2 = cos1.update(HashMap.of([streamA, opA0], [streamB, opB0]));
      const cos3 = cos2.update(HashMap.of([streamA, opA1], [streamB, opB0]));
      expectPreludeEqual(cos3.value, Vector.of("a.a0", "a.a1", "b.b0"));
    });

    it("update purges newer ops from a stream if needed", () => {
      const cos1 = cos.update(HashMap.of([streamA, opA1]));
      const cos2 = cos1.update(HashMap.of([streamA, opA0]));
      expectPreludeEqual(cos2.value, Vector.of("a.a0"));
      expectIdentical(
        ControlledOpSet.headsEqual(cos2.heads, HashMap.of([streamA, opA0])),
        true,
      );
    });
  });

  // Also demonstrates using custom undo code.
  describe("write permissions", () => {
    type AddToken = {
      op: {
        timestamp: Timestamp;
        type: "add";
        token: string;
      };
      undoInfo: undefined;
    };
    type AddWriter = {
      op: {
        timestamp: Timestamp;
        streamId: StreamId;
        type: "add writer";
      };
      undoInfo: undefined | "open" | OpList<AppliedOp>;
    };
    type RemoveWriter = {
      op: {
        timestamp: Timestamp;
        streamId: StreamId;
        type: "remove writer";
        finalOp: OpList<AppliedOp>;
      };
      undoInfo: undefined | "open" | OpList<AppliedOp>;
    };
    type AppliedOp = AddToken | AddWriter | RemoveWriter;
    type Value = {
      tokens: Vector<string>;
      desiredWriters: HashMap<StreamId, "open" | OpList<AppliedOp>>;
    };

    const clock = new CountingClock();
    const cos = ControlledOpSet.create<Value, AppliedOp, StreamId>(
      (value, op) => {
        if (op.type === "add")
          return {
            value: {
              ...value,
              tokens: value.tokens.append(op.token),
            },
            appliedOp: {op, undoInfo: undefined},
          };
        else if (op.type === "add writer") {
          return {
            value: {
              ...value,
              desiredWriters: value.desiredWriters.put(op.streamId, "open"),
            },
            appliedOp: {
              op,
              undoInfo: value.desiredWriters.get(op.streamId).getOrUndefined(),
            },
          };
        } else {
          const streamId = op.streamId;
          return {
            value: {
              ...value,
              desiredWriters: value.desiredWriters.put(streamId, op.finalOp),
            },
            appliedOp: {
              op,
              undoInfo: value.desiredWriters.get(streamId).getOrUndefined(),
            },
          };
        }
      },
      (value, {op, undoInfo}) => {
        if (op.type === "add") {
          return {
            ...value,
            tokens: value.tokens.dropRight(1),
          };
        } else if (op.type === "add writer") {
          undoInfo = undoInfo as AddWriter["undoInfo"];
          return {
            ...value,
            desiredWriters:
              undoInfo === undefined
                ? value.desiredWriters.remove(op.streamId)
                : value.desiredWriters.put(op.streamId, undoInfo),
          };
        } else if (op.type === "remove writer") {
          undoInfo = undoInfo as RemoveWriter["undoInfo"];
          const streamId = op.streamId;
          return {
            ...value,
            desiredWriters:
              undoInfo === undefined
                ? value.desiredWriters.remove(streamId)
                : value.desiredWriters.put(streamId, undoInfo),
          };
        } else throw new AssertFailed("Unknown op type");
      },
      (value) => HashMap.ofIterable(value.desiredWriters),
      {
        tokens: Vector.of(),
        desiredWriters: HashMap.of([streamB, "open"]),
      },
    );
    const opA0 = LinkedList.of<AppliedOp["op"]>({
      type: "add",
      token: "a0",
      timestamp: clock.now(),
    });
    const opA1 = opA0.prepend({
      type: "add",
      token: "a1",
      timestamp: clock.now(),
    });
    const opB0 = LinkedList.of<AppliedOp["op"]>({
      type: "add writer",
      streamId: streamA,
      timestamp: clock.now(),
    });
    const opB1 = opB0.prepend({
      type: "remove writer",
      streamId: streamA,
      finalOp: opA0,
      timestamp: clock.now(),
    });
    const opB2 = opB1.prepend({
      type: "add writer",
      streamId: streamA,
      timestamp: clock.now(),
    });
    const opB0Alternate = LinkedList.of<AppliedOp["op"]>({
      type: "add",
      token: "b0Alternate",
      timestamp: clock.now(),
    });
    const opA2 = opA1.prepend({
      type: "add",
      token: "a2",
      timestamp: clock.now(),
    });

    it("ignores ops from a non-writer", () => {
      const cos1 = cos.update(HashMap.of([streamA, opA1]));
      expectIdentical(cos1.appliedOps.isEmpty(), true);
    });

    it("includes ops from an added writer", () => {
      const cos1 = cos.update(HashMap.of([streamA, opA1], [streamB, opB0]));
      expectPreludeEqual(cos1.value.tokens, Vector.of("a0", "a1"));
      expectIdentical(
        ControlledOpSet.headsEqual(
          cos1.heads,
          HashMap.of([streamA, opA1], [streamB, opB0]),
        ),
        true,
      );
    });

    it("closes a writer", () => {
      const cos1 = cos.update(HashMap.of([streamA, opA1], [streamB, opB0]));
      expectPreludeEqual(cos1.value.tokens, Vector.of("a0", "a1"));

      // Close streamA after opA0.
      const cos2 = cos.update(HashMap.of([streamA, opA1], [streamB, opB1]));
      // a1 was excluded because b0 closed that stream.
      expectPreludeEqual(cos2.value.tokens, Vector.of("a0"));
      expectIdentical(
        ControlledOpSet.headsEqual(
          cos2.heads,
          HashMap.of([streamA, opA0], [streamB, opB1]),
        ),
        true,
      );
    });

    it("reopens a closed writer", () => {
      const cos1 = cos.update(HashMap.of([streamA, opA1], [streamB, opB1]));
      expectPreludeEqual(cos1.value.tokens, Vector.of("a0"));

      // Reopen streamA.
      const cos2 = cos.update(HashMap.of([streamA, opA1], [streamB, opB2]));
      expectPreludeEqual(cos2.value.tokens, Vector.of("a0", "a1"));
      expectIdentical(
        ControlledOpSet.headsEqual(
          cos2.heads,
          HashMap.of([streamA, opA1], [streamB, opB2]),
        ),
        true,
      );
    });

    it("avoids redoing later ops from a removed writer", () => {
      const cos1 = cos.update(HashMap.of([streamA, opA2], [streamB, opB0]));
      expectPreludeEqual(cos1.value.tokens, Vector.of("a0", "a1", "a2"));

      const cos2 = cos.update(HashMap.of([streamA, opA2], [streamB, opB1]));
      // Even a2, which came after b1, which closed stream A, is gone.
      expectPreludeEqual(cos2.value.tokens, Vector.of("a0"));
      expectIdentical(
        ControlledOpSet.headsEqual(
          cos2.heads,
          HashMap.of([streamA, opA0], [streamB, opB1]),
        ),
        true,
      );
    });

    it("undo Add/Remove Writer", () => {
      const cos1 = cos.update(HashMap.of([streamA, opA1], [streamB, opB1]));
      expectPreludeEqual(cos1.value.tokens, Vector.of("a0"));
      // This forces b0 and b1 to be undone.
      const cos2 = cos1.update(
        HashMap.of([streamA, opA1], [streamB, opB0Alternate]),
      );

      expectPreludeEqual(cos2.value.tokens, Vector.of("b0Alternate"));
      expectIdentical(
        ControlledOpSet.headsEqual(
          cos2.heads,
          HashMap.of([streamB, opB0Alternate]),
        ),
        true,
      );
    });
  });
});
