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
  forwardRef,
  useEffect,
  useImperativeHandle,
  useMemo,
  useRef,
  useContext,
  type HTMLProps,
} from "react";
import * as monaco from "monaco-editor";
import { useModifiedFunctions } from "../../../../functions/providers/useModifiedFunctions";
import {
  SQL_DARK_THEME,
  SQL_LIGHT_THEME,
  getSqlEditorOptions,
} from "../../utilities/sqlEditorOptions";
import { useMonacoEditorInstance } from "../../utilities/useMonacoEditorInstance";
import { useMonacoTokenProvider } from "../../utilities/useMonacoTokenProvider";
import { useSqlEditorExtensions } from "./helpers/useSqlEditorExtensions";
import { useSuggestWidgetObserver } from "./helpers/useSuggestWidgetObserver";
import type { MonacoExtensions } from "./types/extensions.type";
import { useColorScheme } from "../../../../../appTheme/appTheme";

type SqlEditorProps = Omit<HTMLProps<HTMLDivElement>, "onChange"> &
  Partial<{
    autoResize: boolean;
    defaultValue: string;
    extensions: Array<(extensions: MonacoExtensions) => MonacoExtensions>;
    keyboardShortcuts: monaco.editor.IActionDescriptor[];
    onChange: (val: string) => unknown;
  }>;

type SqlEditorRef = {
  getEditorInstance: () => monaco.editor.IStandaloneCodeEditor | null;
  monaco: typeof monaco;
};

/**
 * @description Editable version of the SQL editor
 */
export const SqlEditor = forwardRef<SqlEditorRef, SqlEditorProps>(
  (
    {
      autoResize,
      defaultValue,
      extensions,
      keyboardShortcuts,
      onChange,
      ...props
    },
    ref,
  ) => {
    const divEl = useRef<HTMLDivElement>(null);
    const colorScheme = useColorScheme();
    const theme = colorScheme === "dark" ? SQL_DARK_THEME : SQL_LIGHT_THEME;

    const options: monaco.editor.IStandaloneEditorConstructionOptions = useMemo(
      () => ({ ...getSqlEditorOptions(), value: defaultValue }),
      [defaultValue],
    );

    const getEditorInstance = useMonacoEditorInstance({
      mountRef: divEl,
      options,
    });

    const modifiedFunctions = useModifiedFunctions();

    useSqlEditorExtensions(extensions);

    useSuggestWidgetObserver(modifiedFunctions);

    useMonacoTokenProvider(modifiedFunctions, theme, getEditorInstance);

    useEffect(() => {
      const editorInstance = getEditorInstance();

      if (!editorInstance || !autoResize) {
        return;
      }

      const sizeChangeDisposer = editorInstance.onDidContentSizeChange(() => {
        const contentHeight = Math.min(
          500,
          Math.max(150, editorInstance.getContentHeight()),
        );

        divEl.current!.style.height = `${contentHeight}px`;
      });

      return () => sizeChangeDisposer.dispose();
    }, [getEditorInstance, autoResize]);

    useEffect(() => {
      const editorInstance = getEditorInstance();

      if (!editorInstance || !onChange) {
        return;
      }

      const contentChangeDisposer = editorInstance.onDidChangeModelContent(
        () => {
          onChange(editorInstance.getValue());
        },
      );

      return () => contentChangeDisposer.dispose();
    }, [getEditorInstance, onChange]);

    useEffect(() => {
      const editorInstance = getEditorInstance();

      if (!editorInstance) {
        return;
      }

      const shortcutsDisposers: monaco.IDisposable[] = [];

      keyboardShortcuts?.forEach((shortcut) => {
        shortcutsDisposers.push(
          editorInstance.addAction({
            id: shortcut.id,
            keybindings: shortcut.keybindings,
            run: shortcut.run,
            label: shortcut.label,
          }),
        );
      });

      return () => shortcutsDisposers.forEach((disposer) => disposer.dispose());
    }, [getEditorInstance, keyboardShortcuts]);

    useImperativeHandle(
      ref,
      () => ({
        getEditorInstance,
        monaco,
      }),
      [getEditorInstance],
    );

    return <div {...props} ref={divEl} data-heap-redact-text />;
  },
);

SqlEditor.displayName = "SqlEditor";
