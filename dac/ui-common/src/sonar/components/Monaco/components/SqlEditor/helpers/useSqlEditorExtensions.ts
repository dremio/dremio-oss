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

import { useEffect } from "react";
import * as monaco from "monaco-editor";
import type { MonacoExtensions } from "../types/extensions.type";
import { configureExtensions } from "./sqlEditorExtensions";

const compose = (
  fn1: (a: MonacoExtensions) => MonacoExtensions,
  ...fns: Array<(a: MonacoExtensions) => MonacoExtensions>
) => fns.reduce((prevFn, nextFn) => (value) => prevFn(nextFn(value)), fn1);

export const useSqlEditorExtensions = (
  extensions?: Array<(extensions: MonacoExtensions) => MonacoExtensions>,
) => {
  useEffect(() => {
    const extensionsDisposers: monaco.IDisposable[] = [];

    if (extensions?.length) {
      extensionsDisposers.push(
        // @ts-expect-error TS doesn't like the spread operator for non-rest parameters
        ...configureExtensions(compose(...extensions)({})),
      );
    }

    return () => extensionsDisposers.forEach((disposer) => disposer.dispose());
  }, [extensions]);
};
