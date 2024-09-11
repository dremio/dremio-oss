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
import { getApiContext } from "../../../../../../contexts/ApiContext";
import { getSessionContext } from "../../../../../../contexts/SessionContext";
import { initializeWorker } from "../../../../../../sql/worker/client/SQLParsingWorkerClient";
import { monacoLogger } from "../../../utilities/monacoLogger";
import { getSqlEditorOptions } from "../../../utilities/sqlEditorOptions";
import type { MonacoExtensions } from "../types/extensions.type";
import { updateCommandLabel } from "./sqlEditorContextMenu";

const registerAutocomplete = (extensions: MonacoExtensions) => {
  monacoLogger.debug("Registering completion provider");

  return monaco.languages.registerCompletionItemProvider(
    getSqlEditorOptions().language,
    extensions.completionItemProvider as monaco.languages.CompletionItemProvider,
  );
};

const registerFormatter = (extensions: MonacoExtensions) => {
  monacoLogger.debug("Registering formatting provider");

  updateCommandLabel("editor.action.formatDocument", "Format Query");

  monaco.editor.addKeybindingRule({
    keybinding:
      monaco.KeyMod.CtrlCmd | monaco.KeyMod.Shift | monaco.KeyCode.KeyF,
    command: "editor.action.formatDocument",
  });

  return monaco.languages.registerDocumentFormattingEditProvider(
    getSqlEditorOptions().language,
    extensions.documentFormattingEditProvider as monaco.languages.DocumentFormattingEditProvider,
  );
};

const registerExtensions = (extensions: MonacoExtensions) => {
  const disposers: monaco.IDisposable[] = [];

  for (const extension in extensions) {
    switch (extension as keyof MonacoExtensions) {
      case "completionItemProvider":
        disposers.push(registerAutocomplete(extensions));
        break;

      case "documentFormattingEditProvider":
        disposers.push(registerFormatter(extensions));
        break;

      default:
        throw new Error(`${extension} is not a supported extension`);
    }
  }

  return disposers;
};

export const configureExtensions = (extensions: MonacoExtensions) => {
  monacoLogger.debug("Configuring extensions", extensions);

  if (extensions.completionItemProvider) {
    monacoLogger.debug("Initializing extensions worker");

    initializeWorker({
      authToken: `Bearer ${getSessionContext().getSessionIdentifier?.()}`,
      getSuggestionsURL:
        getApiContext().createSonarUrl("sql/autocomplete").href,
      sqlFunctions:
        extensions.completionItemProvider.functions?.map((fn) => ({
          name: fn.name,
          description: fn.description,
          label: fn.label,
          snippet: fn.snippet,
        })) ?? [],
    });
  }

  return registerExtensions(extensions);
};
