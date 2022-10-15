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
import { intl } from "@app/utils/intl";
import LoadingBar from "@app/components/LoadingBar/LoadingBar";
import ColumnMenuItem from "@app/components/DragComponents/ColumnMenuItem";
import * as classes from "./SummaryColumns.module.less";

type SummaryColumnsProps = {
  fields?: {
    name: string;
    type: string;
    isPartitioned: boolean;
    isSorted: boolean;
  }[];
  fieldsCount: number;
};

const SummaryColumns = ({ fields, fieldsCount }: SummaryColumnsProps) => {
  const { formatMessage } = intl;

  return (
    <div className={classes["summary-columns-container"]}>
      <div className={classes["summary-columns-title"]}>
        {formatMessage({ id: "Common.Columns" })}
        {fieldsCount && ` (${fieldsCount})`}
      </div>
      <div className={classes["summary-rows"]}>
        {fields ? (
          fields.map((field: any, i: number) => {
            return (
              <ColumnMenuItem
                dragType="NONE"
                className={classes["summary-row"]}
                key={i}
                item={field}
                preventDrag
              />
            );
          })
        ) : (
          <LoadingBar
            width={144}
            height={12}
            className={classes["summary-column-loader"]}
          />
        )}
      </div>
    </div>
  );
};

export default SummaryColumns;
