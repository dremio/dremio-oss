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

import { useEffect, useMemo, useRef, type FC, type HTMLProps } from "react";
import * as monaco from "monaco-editor";
import { useFunctions } from "../../../../functions/providers/useFunctions";
import {
  SQL_DARK_THEME,
  SQL_LIGHT_THEME,
  getSqlViewerOptions,
} from "../../utilities/sqlEditorOptions";
import { useMonacoEditorInstance } from "../../utilities/useMonacoEditorInstance";
import { useMonacoTokenProvider } from "../../utilities/useMonacoTokenProvider";

type SqlViewerProps = HTMLProps<HTMLDivElement> & {
  fitHeightToContent?: boolean;
  theme?: typeof SQL_LIGHT_THEME | typeof SQL_DARK_THEME;
  value: string;
};

/**
 * @description Read-only version of the SQL editor
 */
export const SqlViewer: FC<SqlViewerProps> = ({
  fitHeightToContent,
  theme = SQL_LIGHT_THEME,
  value,
  ...props
}) => {
  const divEl = useRef<HTMLDivElement>(null);

  const options: monaco.editor.IStandaloneEditorConstructionOptions = useMemo(
    () => ({ ...getSqlViewerOptions(), value }),
    [value],
  );

  const getEditorInstance = useMonacoEditorInstance({
    mountRef: divEl,
    options,
  });

  const { value: functionsObj } = useFunctions();

  useMonacoTokenProvider(functionsObj?.functions, theme, getEditorInstance);

  useEffect(() => {
    const editorInstance = getEditorInstance();

    if (!editorInstance || !fitHeightToContent) {
      return;
    }

    divEl.current!.style.height = `${editorInstance.getContentHeight() + 10}px`;
  }, [getEditorInstance, fitHeightToContent]);

  return <div {...props} ref={divEl} data-heap-redact-text />;
};
