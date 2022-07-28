import {arrayComparison} from "./Collection";
import {expect} from "chai";

describe("Collection", () => {
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
});
