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

export const getSelectedSql = (
  editor?: monaco.editor.IStandaloneCodeEditor | null,
) => {
  if (!editor) {
    return { sql: "", range: {} };
  }

  const selection = editor.getSelection();
  const range = {
    endColumn: selection?.endColumn || 1,
    endLineNumber: selection?.endLineNumber || 1,
    startColumn: selection?.startColumn || 1,
    startLineNumber: selection?.startLineNumber || 1,
  };

  return {
    sql: editor.getModel()?.getValueInRange(range),
    range,
  };
};
