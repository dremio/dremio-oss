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

import { findLastIndex as loFindLastIndex } from "lodash";

export function lastIndexOf<T>(
  array: T[],
  lookup: T,
  fromIndex?: number | undefined,
): number | undefined {
  let index: number;
  if (fromIndex != undefined) {
    index = array.lastIndexOf(lookup, fromIndex);
  } else {
    index = array.lastIndexOf(lookup);
  }
  if (index === -1) {
    return undefined;
  }
  return index;
}

export function indexOf<T>(
  array: T[],
  lookup: T,
  fromIndex?: number | undefined,
): number | undefined {
  let index: number;
  if (fromIndex != undefined) {
    index = array.indexOf(lookup, fromIndex);
  } else {
    index = array.indexOf(lookup);
  }
  if (index === -1) {
    return undefined;
  }
  return index;
}

export function findLastIndex<T>(
  array: T[] | null | undefined,
  predicate: (elem: { value: T; index: number }) => boolean,
  fromIndex?: number | undefined,
): number | undefined {
  const arrayWithIndices = array?.map((value, index) => ({ value, index }));
  const index = loFindLastIndex(arrayWithIndices, predicate, fromIndex);
  if (index === -1) {
    return undefined;
  }
  return index;
}

/**
 * E.g. toReadableString(["a", "b", "c"], "or") -> "a, b, or c"
 */
export function toReadableString(array: string[], conjunction: string): string {
  switch (array.length) {
    case 0:
      return "";
    case 1:
      return array[0];
    case 2:
      return array.join(` ${conjunction} `);
    default:
      // Note this does not handle languages like Chinese that use non-standard comma character for lists
      return `${array.slice(0, -1).join(", ")}, ${conjunction} ${
        array[array.length - 1]
      }`;
  }
}
