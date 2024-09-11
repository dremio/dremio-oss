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

// @ts-expect-error private file
import * as actions from "monaco-editor/esm/vs/platform/actions/common/actions";

/**
 * Updates the label of an existing command in Monaco's context menu.
 */
export const updateCommandLabel = (actionId: string, newLabel: string) => {
  const menus: Map<{ id: string }, any> = actions.MenuRegistry._menuItems;

  const contextMenuEntry = [...menus].find(
    (entry) => entry[0].id === "EditorContext",
  );

  const contextMenuLinks = contextMenuEntry?.[1];

  let contextNode = contextMenuLinks?._first;

  // Need to iterate through a linked list until the correct node is updated
  while (contextNode) {
    if (actionId === contextNode.element?.command?.id) {
      contextNode.element.command.title = newLabel;
      break;
    }

    contextNode = contextNode.next;
  }
};
