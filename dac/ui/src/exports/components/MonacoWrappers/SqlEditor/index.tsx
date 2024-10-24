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

import { forwardRef, useImperativeHandle, useRef, type HTMLProps } from "react";
import clsx from "clsx";
import { SqlEditor as Editor } from "dremio-ui-common/sonar/components/Monaco/components/SqlEditor/SqlEditor.js";
import { SQL_DARK_THEME, SQL_LIGHT_THEME } from "#oss/utils/sql-editor";
import * as classes from "./SqlEditor.module.less";

type SqlEditorProps = Omit<HTMLProps<HTMLDivElement>, "onChange"> &
  Partial<{
    autoResize: boolean;
    defaultValue: string;
    extensions: Array<(extensions: any) => any>;
    keyboardShortcuts: Record<string, any>[];
    onChange: (val: string) => unknown;
    theme: typeof SQL_LIGHT_THEME | typeof SQL_DARK_THEME;
  }>;

export type SqlEditorRef = {
  getEditorInstance: (() => Record<string, any> | null) | undefined;
  monaco: any;
};

/**
 * OSS wrapper of the editable version of the SQL editor used to apply styles
 */
export const SqlEditor = forwardRef<SqlEditorRef, SqlEditorProps>(
  ({ className, ...props }, ref) => {
    const editorRef = useRef<SqlEditorRef | null>(null);

    useImperativeHandle(
      ref,
      () => ({
        getEditorInstance: editorRef.current?.getEditorInstance,
        monaco: editorRef.current?.monaco,
      }),
      [],
    );

    return (
      <Editor
        {...props}
        ref={editorRef}
        className={clsx(className, "w-full mt-1", classes["sql-editor"])}
      />
    );
  },
);
