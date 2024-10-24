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

import { signal, useComputed } from "@preact/signals-react";
import { useSignal, useSignals } from "@preact/signals-react/runtime";
import {
  createContext,
  useCallback,
  useContext,
  type FC,
  type PropsWithChildren,
} from "react";

const ExpandedContext = createContext(signal(new Set<string>()));

export const useTreeItemExpanded = (id: string) => {
  // Register the hook as a signal consumer
  // when any Signal's `.value` proxy is read and
  // force a re-render when the value changes
  useSignals();

  // Get access to the tree's expandedNodes signal.
  // The reference of this will never change once
  // created unlike a regular React state / reducer.
  const expandedNodes = useContext(ExpandedContext);

  // Create a computed signal which only tracks this
  // tree item's expansion state. This prevents nodes
  // without changes in their expansion state from
  // re-rendering.
  return useComputed(() => expandedNodes.value.has(id)).value;
};

export const useToggleTreeItemExpansion = (id: string) => {
  const expandedNodes = useContext(ExpandedContext);

  return useCallback(() => {
    const next = new Set(expandedNodes.peek());

    if (!next.has(id)) {
      next.add(id);
    } else {
      next.delete(id);
    }

    expandedNodes.value = next;
  }, [id]);
};

export const ExpandedContextProvider: FC<PropsWithChildren> = (props) => (
  <ExpandedContext.Provider value={useSignal(new Set<string>())}>
    {props.children}
  </ExpandedContext.Provider>
);
