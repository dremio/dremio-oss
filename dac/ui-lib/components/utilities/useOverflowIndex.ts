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

import { useEffect, useState, type MutableRefObject, useCallback } from "react";
import { findOverflowIndex } from "./findOverflowIndex";
import { useElementRect } from "./useElementRect";
import { useMutationObserver } from "./useMutationObserver";

/**
 * Returns the index of the first child element that overflows its parent
 * and a reference to the first hidden element to help position an overflow menu trigger.
 */
export const useOverflowIndex = (
  ref: MutableRefObject<Element>,
): [number, Element] | [undefined, null] => {
  const [overflowIndex, setOverflowIndex] = useState<number | undefined>(
    undefined,
  );
  const [overflowedElement, setOverflowedElement] = useState<Element | null>(
    null,
  );

  // Track how many times the children have been modified
  const [childrenVersion, setChildrenVersion] = useState(0);

  const parentRect = useElementRect(ref);

  useMutationObserver(
    ref,
    useCallback(() => {
      setChildrenVersion((x) => x + 1);
    }, []),
  );

  // Every time the parentRect changes, recalculate which (if any) children have overflowed
  useEffect(() => {
    const overflowIndex = findOverflowIndex(ref.current);
    setOverflowIndex(overflowIndex);

    if (overflowIndex) {
      setOverflowedElement(ref.current.children.item(overflowIndex));
    } else {
      setOverflowedElement(null);
    }
  }, [childrenVersion, parentRect, ref]);

  if (!overflowIndex) {
    return [undefined, null];
  }

  return [overflowIndex, overflowedElement as Element];
};
