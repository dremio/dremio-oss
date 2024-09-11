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
  type HTMLProps,
  forwardRef,
  useImperativeHandle,
  useMemo,
  useRef,
} from "react";
import Immutable from "immutable";
import { useSqlErrorDecorations } from "dremio-ui-common/sonar/components/Monaco/components/SqlEditor/extensions/useSqlErrorDecorations.js";
import { completionProvider } from "dremio-ui-common/sonar/components/Monaco/components/SqlEditor/plugins/completionProvider.js";
import { formattingProvider } from "dremio-ui-common/sonar/components/Monaco/components/SqlEditor/plugins/formattingProvider.js";
import { type SQLError } from "dremio-ui-common/sql/errorDetection/types/SQLError.js";
import { addNotification } from "@app/actions/notification";
import { MSG_CLEAR_DELAY_SEC } from "@app/constants/Constants";
import {
  SqlEditor,
  type SqlEditorRef,
} from "@app/exports/components/MonacoWrappers/SqlEditor";
import { useSupportFlag } from "@app/exports/endpoints/SupportFlags/getSupportFlag";
import { LIVE_SYNTAX_ERROR_DETECTION } from "@app/exports/endpoints/SupportFlags/supportFlagConstants";
import { store } from "@app/store/store";
import { intl } from "@app/utils/intl";
import { SQL_DARK_THEME, SQL_LIGHT_THEME } from "@app/utils/sql-editor";
import { getExtraKeyboardShortcuts } from "@inject/utils/sql-editor-extra";
import { useSqlFunctions } from "./hooks/useSqlFunctions";
import { getKeyboardShortcuts } from "./utils/keyboardShortcuts";

type SqlEditorContainerProps = Omit<HTMLProps<HTMLDivElement>, "onChange"> &
  Partial<{
    defaultValue: string;
    extensionsConfig: Partial<{
      autocomplete: boolean;
      formatter: boolean;
    }>;
    onChange: (val: string) => unknown;
    queryContext: Immutable.List<string>;
    serverSqlErrors: SQLError[];
    theme: typeof SQL_LIGHT_THEME | typeof SQL_DARK_THEME;
    keyboardShortcutProps: {
      toggleExtraSQLPanel?: () => Record<string, any> | null;
    };
  }>;

type SqlEditorContainerRef = {
  getEditorInstance: (() => Record<string, any> | null) | undefined;
  insertSnippet: (snippet: string) => void;
  monaco: any;
};

/**
 * @description Explore page wrapper for the SQL editor
 */
const SqlEditorContainer = forwardRef<
  SqlEditorContainerRef,
  SqlEditorContainerProps
>(
  (
    {
      extensionsConfig,
      queryContext,
      serverSqlErrors,
      onChange,
      keyboardShortcutProps,
      ...props
    },
    ref,
  ) => {
    const editorRef = useRef<SqlEditorRef | null>(null);

    const [liveErrorsEnabled] = useSupportFlag(LIVE_SYNTAX_ERROR_DETECTION);

    const [sqlFunctions] = useSqlFunctions();

    const handleChange = useSqlErrorDecorations({
      editorInstance: editorRef.current?.getEditorInstance?.() ?? undefined,
      isLiveDetectionEnabled: liveErrorsEnabled,
      onChange,
      serverSqlErrors,
    });

    const keyboardShortcuts = useMemo(() => {
      return [
        ...getKeyboardShortcuts({
          editor: editorRef.current?.getEditorInstance?.(),
          monaco: editorRef.current?.monaco,
        }),
        ...getExtraKeyboardShortcuts({
          editor: editorRef.current?.getEditorInstance?.(),
          monaco: editorRef.current?.monaco,
          toggleExtraSQLPanel: keyboardShortcutProps?.toggleExtraSQLPanel,
        }),
      ];
    }, [keyboardShortcutProps]);

    const extensions = useMemo(() => {
      const extensions = [];

      if (extensionsConfig?.autocomplete) {
        extensions.push(
          completionProvider(sqlFunctions ?? undefined, queryContext),
        );
      }

      if (extensionsConfig?.formatter) {
        extensions.push(
          formattingProvider(() =>
            store.dispatch(
              addNotification(
                intl.formatMessage({ id: "SQL.Format.Error" }),
                "error",
                MSG_CLEAR_DELAY_SEC,
              ),
            ),
          ),
        );
      }

      return extensions;
    }, [
      extensionsConfig?.autocomplete,
      extensionsConfig?.formatter,
      queryContext,
      sqlFunctions,
    ]);

    useImperativeHandle(
      ref,
      () => ({
        getEditorInstance: editorRef.current?.getEditorInstance,
        insertSnippet: (snippet: string) => {
          editorRef.current
            ?.getEditorInstance?.()
            ?.getContribution("snippetController2")
            ?.insert(snippet);
        },
        monaco: editorRef.current?.monaco,
      }),
      [],
    );

    return (
      <SqlEditor
        {...props}
        ref={editorRef}
        extensions={extensions}
        keyboardShortcuts={keyboardShortcuts}
        onChange={handleChange}
      />
    );
  },
);

export default SqlEditorContainer;
