import {Timestamp} from "./Timestamp";

export interface Clock {
  now(): Timestamp;
}

export const basicClock: Clock = {
  now: () => Timestamp.create(Date.now()),
};
