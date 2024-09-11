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

import { useEffect, useRef } from "react";
import * as monaco from "monaco-editor";

export const useMonacoEditorInstance = (config: {
  mountRef: React.RefObject<HTMLElement>;
  options?: monaco.editor.IStandaloneEditorConstructionOptions;
}) => {
  const editorInstanceRef = useRef<monaco.editor.IStandaloneCodeEditor | null>(
    null,
  );

  useEffect(() => {
    // @font-face lazy loads fonts which causes the editor to render before the font
    // is ready in new browser tabs. The official solution is to call `remeasureFonts`
    // after a delay.
    setTimeout(() => {
      monaco.editor.remeasureFonts();
    }, 1500);
  }, []);

  useEffect(() => {
    if (!config.mountRef.current) {
      throw new Error("mountRef must contain an HTML element");
    }

    const editorInstance = monaco.editor.create(
      config.mountRef.current,
      config.options,
    );

    editorInstanceRef.current = editorInstance;

    return () => {
      editorInstance.dispose();
    };
  }, [config.mountRef, config.options]);

  return () => editorInstanceRef.current;
};
