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

import { useEffect, useRef, type FC } from "react";
import { parser } from "./parser/index.js";
import { EditorState } from "@codemirror/state";
import { EditorView, keymap, ViewUpdate } from "@codemirror/view";
import { LRLanguage, LanguageSupport, syntaxTree } from "@codemirror/language";
import { styleTags, tags } from "@lezer/highlight";
import { HighlightStyle } from "@codemirror/language";
import { syntaxHighlighting } from "@codemirror/language";
// import {
//   autocompletion,
//   startCompletion,
//   CompletionContext,
// } from "@codemirror/autocomplete";

const parserWMeta = parser.configure({
  props: [
    styleTags({
      SearchText: tags.string,
      FilterKeyword: tags.keyword,
      FilterValue: tags.variableName,
      QuotedString: tags.namespace,
    }),
  ],
});

const dremioSemanticSearch = new LanguageSupport(
  LRLanguage.define({
    parser: parserWMeta,
    languageData: {
      // autocomplete: dremioSemanticSearchCompletions,
    },
  })
);

const dremioSemanticSearchHighlights = HighlightStyle.define([
  // { tag: tags.string, color: "darkblue" },
  // { tag: tags.keyword, color: "#f5d" },
  // { tag: tags.variableName, color: "#f40" },
  // { tag: tags.namespace, color: "green" },
]);

export const SemanticSearchInput: FC<{
  onViewUpdate: (update: ViewUpdate) => void;
}> = (props) => {
  const parentRef = useRef<HTMLDivElement>(null);
  const onViewUpdateRef = useRef(props.onViewUpdate);
  onViewUpdateRef.current = props.onViewUpdate;
  useEffect(() => {
    if (!parentRef.current) {
      return;
    }

    const view = new EditorView({
      state: EditorState.create({
        doc: "",
        extensions: [
          dremioSemanticSearch,
          syntaxHighlighting(dremioSemanticSearchHighlights),
          // autocompletion(),
          EditorView.updateListener.of((update) => {
            if ("values" in update.state) {
              onViewUpdateRef.current?.(update);
            }
          }),
        ],
      }),
      parent: parentRef.current,
    });

    return () => {
      view.destroy();
    };
  }, []);

  return (
    <div className="form-control">
      <dremio-icon name="interface/search" class="icon-primary"></dremio-icon>
      <div ref={parentRef} />
    </div>
  );
};
