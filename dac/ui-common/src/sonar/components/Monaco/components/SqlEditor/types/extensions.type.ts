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
import type { ModifiedFunction } from "../../../../../functions/Functions.type";

export type MonacoExtensions = Partial<{
  completionItemProvider: monaco.languages.CompletionItemProvider &
    Partial<{
      functions: ModifiedFunction[];
    }>;
  documentFormattingEditProvider: monaco.languages.DocumentFormattingEditProvider;
}>;

export type CompletionProviderExtension = (
  extensions: Omit<MonacoExtensions, "completionItemProvider">,
) => Omit<MonacoExtensions, "completionItemProvider"> &
  NonNullable<Pick<MonacoExtensions, "completionItemProvider">>;

export type FormattingProviderExtension = (
  extensions: Omit<MonacoExtensions, "documentFormattingEditProvider">,
) => Omit<MonacoExtensions, "documentFormattingEditProvider"> &
  NonNullable<Pick<MonacoExtensions, "documentFormattingEditProvider">>;
