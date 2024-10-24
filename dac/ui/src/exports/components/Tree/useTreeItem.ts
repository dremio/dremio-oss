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

import {
  useToggleTreeItemExpansion,
  useTreeItemExpanded,
} from "./expandedContext";
import { useTreeItemLevel } from "./levelContext";

/**
 * @param id A unique identifier for a specific tree item
 * @returns props to be attached to a tree item
 */
export const useTreeItem = (id: string) => {
  const expanded = useTreeItemExpanded(id);

  return {
    "aria-expanded": expanded,
    "aria-level": useTreeItemLevel(),
    id,
    isExpanded: expanded,
    onClick: useToggleTreeItemExpansion(id),
    role: "treeitem" as const,
  };
};
