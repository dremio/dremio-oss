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

import clsx from "clsx";
// @ts-ignore
import { Tooltip } from "dremio-ui-lib";
import { CopyButton } from "dremio-ui-lib/components";
import { SyntaxHighlighter } from "dremio-ui-lib/components";

import * as classes from "./SQLCell.module.less";

export const SQLCell = ({ sql }: { sql: string }) => {
  return (
    <Tooltip
      title={
        <div className={classes["sql-cell__tooltipWrapper"]}>
          <SyntaxHighlighter language="sql" wrapLongLines>
            {sql}
          </SyntaxHighlighter>
        </div>
      }
      placement="bottom-start"
      type="richTooltip"
      classes={{ tooltip: "richTooltip sql-cell__tooltip" }}
    >
      <div className={clsx(classes["sql-cell"], "dremio-typography-monospace")}>
        <div className={classes["sql-cell__text"]}>{sql}</div>
        <CopyButton contents={sql} className="copy-button" size="L" />
      </div>
    </Tooltip>
  );
};
