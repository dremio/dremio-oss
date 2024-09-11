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

import * as monaco from "monaco-editor";

export const SQL_LIGHT_THEME = "vs";
export const SQL_DARK_THEME = "vs-dark";

const fixedOptions = {
  automaticLayout: true,
  language: "dremio-sql",
  lineNumbersMinChars: 3,
  fontFamily: "Consolas, Fira Code",
  fontSize: 14,
  hideCursorInOverviewRuler: true,
  minimap: {
    enabled: false,
  },
  renderLineHighlight: "none",
  scrollbar: {
    alwaysConsumeMouseWheel: false,
    useShadows: false,
  },
  scrollBeyondLastLine: false,
  theme: SQL_LIGHT_THEME,
  wordWrap: "on",
  wrappingStrategy: "advanced",
} as const satisfies monaco.editor.IStandaloneEditorConstructionOptions;

export const getSqlEditorOptions = () =>
  ({
    ...fixedOptions,
    fixedOverflowWidgets: true,
    suggest: {
      showWords: false,
    },
    suggestLineHeight: 36,
  }) as const satisfies monaco.editor.IStandaloneEditorConstructionOptions;

export const getSqlViewerOptions = () =>
  ({
    ...fixedOptions,
    contextmenu: false,
    disableLayerHinting: true,
    overviewRulerBorder: false,
    readOnly: true,
    selectOnLineNumbers: false,
  }) as const satisfies monaco.editor.IStandaloneEditorConstructionOptions;
