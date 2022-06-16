import {
  arrayComparison,
  readonly,
  RoSet,
  setAddAll,
  setFirst,
  setIntersection,
  setMap,
  writable,
} from "./Collection";
import {expect} from "chai";

describe("Collection", () => {
  it("readonly and writable", () => {
    const o0 = {};
    const o1 = {};
    const set0 = readonly(new Set<{}>([o0]));
    const set1 = writable(set0);
    set1.add(o1);
    expect(set0.has(o0)).equals(true);
    expect(set0.has(o1)).equals(false);
    expect(set0.has(6)).equals(false);
    expect(set1.has(o0)).equals(true);
    expect(set1.has(o1)).equals(true);
    expect(set1.has(6)).equals(false);
  });

  describe("Array", () => {
    describe("arrayComparison", () => {
      const numberComparison = (left: number, right: number): number =>
        left - right;
      const arrayNumberComparison = arrayComparison(numberComparison);
      it("both empty", () => {
        expect(arrayNumberComparison([], [])).equals(0);
      });
      it("one empty", () => {
        expect(arrayNumberComparison([5], [])).equals(1);
        expect(arrayNumberComparison([], [2])).equals(-1);
      });
      it("value different", () => {
        expect(arrayNumberComparison([1], [2])).equals(-1);
        expect(arrayNumberComparison([2], [1])).equals(1);
      });
    });
  });

  describe("Set", () => {
    it("setFirst returns undefined when set empty", () => {
      expect(setFirst(RoSet([]))).equals(undefined);
    });

    it("setFirst returns first vaue when set not empty", () => {
      expect(setFirst(RoSet([0, -3]))).equals(0);
    });

    it("setAddAll", () => {
      const set = new Set<number>();
      setAddAll(set, [0, 2, 2, 3]);
      expect(set).eqls(new Set([0, 2, 3]));
    });

    it("setIntersection", () => {
      const setA = RoSet([0, 1, 2]);
      const setB = RoSet([1, 2, 3]);
      expect(setIntersection(setA, setB)).eqls(new Set([1, 2]));
    });

    it("setMap", () => {
      const set = RoSet([0, 1, 3]);
      expect(setMap(set, (v) => "a" + v)).eqls(RoSet(["a0", "a1", "a3"]));
    });
  });
});
