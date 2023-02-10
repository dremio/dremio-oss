/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import { Position } from "./position";

/**
 * The motivation of this type is very closely resembles Either monad
 * (e.g. https://hackage.haskell.org/package/category-extras-0.52.0/docs/Control-Monad-Either.html)
 * However, instead of having a generic implementation, we have a case-specific one, since there are
 * no other cases yet to generalize.
 */
export type Skippable<T> = Skip | Value<T>;

export type Skip = {
  readonly kind: "skip";
  readonly to: Position;
};

export type Value<T> = {
  readonly kind: "value";
  readonly value: T;
};

export const skip = (to: Position): Skip => {
  return {
    kind: "skip",
    to,
  };
};

export const value = <T>(valueToUse: T): Value<T> => {
  return {
    kind: "value",
    value: valueToUse,
  };
};

export const isSkipped = <T>(value: Skippable<T>): value is Skip =>
  value.kind === "skip";
