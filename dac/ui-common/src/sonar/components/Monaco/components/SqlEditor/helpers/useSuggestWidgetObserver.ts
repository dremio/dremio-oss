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

import { useCallback, useEffect } from "react";
import { ModifiedFunction } from "../../../../../functions/Functions.type";
import { getSqlEditorOptions } from "../../../utilities/sqlEditorOptions";

const SUGGEST_ITEM_HEIGHT = getSqlEditorOptions().suggestLineHeight;

const useMutationCallback = (functions: ModifiedFunction[]) => {
  const getFunctionLink = useCallback(
    (functionName: string | undefined) => {
      if (!functionName) {
        return;
      }

      for (const sqlFunction of functions) {
        if (sqlFunction.name === functionName) {
          return sqlFunction.link;
        }
      }
    },
    [functions],
  );

  return useCallback(
    (mutationList: MutationRecord[]) => {
      for (const mutation of mutationList) {
        if (mutation.type === "attributes") {
          // accounts for the suggest widget appearing
          const hasTreeMutated =
            (mutation.target as Element).className === "tree" &&
            mutation.target.parentElement?.className ===
              "editor-widget suggest-widget";

          // accounts for rows being focused with the arrow keys
          const hasFocusedElementChanged =
            (mutation.target as Element).className.startsWith(
              "monaco-list-row",
            ) &&
            (mutation.target as Element).className.endsWith("focused") &&
            mutation.target.parentElement?.className === "monaco-list-rows";

          if (hasTreeMutated || hasFocusedElementChanged) {
            const suggestedItems = document.querySelectorAll(
              ".suggest-widget .monaco-list-rows .monaco-list-row",
            );

            if (suggestedItems.length) {
              // append a doc link to function descriptions
              for (const row of Array.from(
                document.getElementsByClassName("monaco-list-row"),
              )) {
                if (
                  row.getElementsByClassName("codicon-symbol-constructor")
                    .length
                ) {
                  const functionLabel =
                    row.attributes.getNamedItem("aria-label")?.value;

                  const functionName = functionLabel?.substring(
                    0,
                    functionLabel.indexOf("("),
                  );

                  const label = row.getElementsByClassName("details-label")[0];

                  // only append the doc link if the function has a description
                  if (label.textContent && label.childElementCount < 1) {
                    const link = getFunctionLink(functionName);

                    if (!link) {
                      continue;
                    }

                    label.textContent = label.textContent.trimEnd();

                    if (!label.textContent.endsWith(".")) {
                      label.textContent += ".";
                    }

                    label.textContent += " ";

                    // create and append the doc link
                    const docLink = document.createElement("a");
                    docLink.innerText = "Docs";
                    docLink.href = link;
                    docLink.target = "_blank";
                    docLink.rel = "noopener noreferrer";
                    docLink.addEventListener("mousedown", (e) => {
                      e.stopPropagation();
                    });
                    label.appendChild(docLink);
                  }
                }
              }

              const suggestedFocusItem = document.getElementsByClassName(
                "monaco-list-row focused",
              )[0];

              // total tree height is the row height times the number of rows,
              // but need to account for rows that have descriptions when focused
              const treeHeight =
                (suggestedFocusItem
                  ? (suggestedFocusItem as HTMLElement).offsetHeight -
                    SUGGEST_ITEM_HEIGHT
                  : 0) +
                suggestedItems.length * SUGGEST_ITEM_HEIGHT;

              // adjust the rows container
              (
                document.querySelectorAll(
                  ".editor-widget.suggest-widget .tree",
                )[0] as HTMLElement
              ).style.height = `${treeHeight}px`;

              // adjust the bottom resizer
              (
                document.querySelectorAll(
                  ".editor-widget.suggest-widget .monaco-sash.orthogonal-edge-south",
                )[0] as HTMLElement
              ).style.top = `${treeHeight}px`;

              // resize the scrollbar
              (
                document.querySelectorAll(
                  ".editor-widget.suggest-widget .tree .scrollbar.vertical",
                )[0] as HTMLElement
              ).style.height = `${treeHeight}px`;
            }
          }

          // decode hover messages for errors in the editor
          const renderedHoverMessage = document.querySelector(
            ".rendered-markdown p",
          );

          if (renderedHoverMessage?.textContent) {
            renderedHoverMessage.textContent = decodeURI(
              renderedHoverMessage.textContent,
            );
          }
        }
      }
    },
    [getFunctionLink],
  );
};

/**
 * Will handle resizing the suggest widget if its contents change after rendering.
 * Also inserts doc links for functions.
 */
export const useSuggestWidgetObserver = (functions: ModifiedFunction[]) => {
  const targetNode = document.querySelector(".sqlAutocomplete .monaco-editor");

  const mutationCallback = useMutationCallback(functions);

  useEffect(() => {
    if (targetNode) {
      const observer = new MutationObserver(mutationCallback);

      observer.observe(targetNode, {
        attributeFilter: ["style", "class"],
        attributeOldValue: true,
        subtree: true,
      });

      return () => observer.disconnect();
    }
  }, [targetNode, mutationCallback]);
};
