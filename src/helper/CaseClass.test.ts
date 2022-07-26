import {CaseClass} from "./CaseClass";
import {expect} from "chai";
import {RoArray} from "./Collection";

describe("CaseClass", () => {
  it("allows writable fields in create, not in copyWith", () => {
    class CC extends CaseClass<{
      mutable: string;
      readonly immutable: string;
    }> {}
    const cc = CC.create({mutable: "a", immutable: "b"});
    cc.copyWith({mutable: "b1"});
    // @ts-expect-error: immutable fields can't be changed with `copyWith`.
    cc.copyWith({immutable: "a1"});
  });

  it("prevents unknown fields in copyWith when all fields readonly", () => {
    // Sadly, Typescript treats {} as an object that can contain any fields.
    // Thus, we have to do extra work to prevent random fields when all fields
    // are readonly.

    class CC extends CaseClass<{
      readonly immutable: string;
    }> {}
    const cc = CC.create({immutable: "b"});
    // @ts-expect-error: immutable fields can't be changed with `copyWith`.
    cc.copyWith({immutable: "a1"});
    // @ts-expect-error: unknown fields can't be changed with `copyWith`.
    cc.copyWith({junk: "a1"});
  });

  it("test", () => {
    class Parent<SubProps = {}> extends CaseClass<
      {
        name: string;
        ar: RoArray<string>;
      } & SubProps
    > {
      // noinspection JSUnusedGlobalSymbols
      beMoreParental(): number {
        return 0;
      }
    }

    const parent = Parent.create({name: "p", ar: []});
    expect(parent).instanceOf(Parent);
    parent.copyWith({name: "pp"});

    class Child extends Parent<{
      age: number;
    }> {
      // noinspection JSUnusedGlobalSymbols
      beMoreChildlike(): number {
        return 0;
      }
    }

    const child = new Child({name: "c", ar: ["foo"], age: 0});
    expect(child).instanceOf(Child);
    expect(child.p.name).equals("c");
    expect(child.p.age).equals(0);

    const child1 = child.copyWith({age: 1});
    expect(child1).instanceOf(Child);
    expect(child1.p.name).equals("c");
    expect(child1.p.age).equals(1);

    // Should not compile
    // @ts-expect-error: ar is an RoArray
    child.p.ar.push("bar");
    // @ts-expect-error: junk is not a valid property
    child.copyWith({junk: 5});
  });
});
