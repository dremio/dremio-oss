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

import { isSkipped, skip, Skippable, value } from "./skippable";
import { charIfExists } from "./stringUtils";

export type Position = {
  readonly line: number;
  readonly column: number;
  readonly index: number;
};

export const symbolAt = (query: string, position: Position): string =>
  query[position.index];
export const isPastLastPosition = isSkipped;
export const positionOf = (position: Skippable<Position>): Position =>
  isSkipped(position) ? position.to : position.value;

/**
 * Advances position by a provided number of indexes.
 * If end of input is reached, position of the last symbol is returned as Skip(position)
 */
export const advancePosition = (
  query: string,
  position: Position,
  advanceBy: number,
): Skippable<Position> => {
  if (advanceBy === 0) {
    return value(position);
  }
  const nextIndex = position.index + 1;
  if (nextIndex >= query.length) {
    return skip(position);
  }
  const isLineBreak = charIfExists(query, position.index) === "\n";
  const nextLine = isLineBreak ? position.line + 1 : position.line;
  const nextColumn = isLineBreak ? 1 : position.column + 1;
  const nextPosition = {
    index: nextIndex,
    line: nextLine,
    column: nextColumn,
  };
  return advancePosition(query, nextPosition, advanceBy - 1);
};
