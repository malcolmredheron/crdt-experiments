import {Timestamp} from "./Timestamp";
import {Clock} from "./Clock";

// A clock that returns an incrementing timestamp each time `now` is called.
export class CountingClock implements Clock {
  timestampCounter = 0;
  now(): Timestamp {
    return Timestamp.create(this.timestampCounter++);
  }
}
