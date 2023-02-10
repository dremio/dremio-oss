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
import React, { useMemo } from "react";
import { TagList } from "@app/pages/HomePage/components/TagList";
import { IconButton } from "@app/../../ui-lib/dist-esm/IconButton";
import { ExternalLink } from "@app/../../ui-lib/dist-esm/ExternalLink";
import DragSource from "components/DragComponents/DragSource";
import { intl } from "@app/utils/intl";
import { ModifiedSQLFunction as SQLFunction } from "@app/endpoints/SQLFunctions/listSQLFunctions";
import { Parameter, SampleCode } from "@app/types/sqlFunctions";
import { FunctionCategoryLabels } from "@app/utils/sqlFunctionUtils";

import * as classes from "./SQLFunctionItem.module.less";

type SQLFunctionItemProps = {
  sqlFunction: SQLFunction;
  addFuncToSqlEditor: (name: string, args?: string) => void;
  onRowClick: (key: string) => void;
  isActiveRow: boolean;
  dragType: string;
};

const SQLFunctionItem = ({
  sqlFunction,
  addFuncToSqlEditor,
  onRowClick,
  isActiveRow,
  dragType,
}: SQLFunctionItemProps) => {
  const { name, signature, description, functionCategories, key } = sqlFunction;
  const {
    parameters = [],
    returnType,
    snippetOverride,
    sampleCodes = [],
  } = signature;

  const [functionLabel, functionSnippet, functionTags] = useMemo(() => {
    let params = "";
    if (parameters.length > 0) {
      parameters.forEach((param: Parameter, idx: number) => {
        params += `${param.type}${param?.name ? ` ${param.name}` : ""}${
          idx !== parameters.length - 1 ? ", " : ""
        }`;
      });
    }

    let snippet = "($1)";
    if (snippetOverride) {
      // BE response snippet is `<name>()`, and monaco only reads `()`
      snippet = snippetOverride.substring(name.length, snippetOverride.length);
    }

    const label = `${name}(${params}) → ${returnType}`;
    const tags = functionCategories?.map((cat) => FunctionCategoryLabels[cat]);

    return [label, snippet, tags];
  }, [name, parameters, returnType, snippetOverride, functionCategories]);

  return (
    <div
      className={`${classes["function-item"]} ${
        isActiveRow ? classes["--isActive"] : ""
      }`}
      onClick={() => onRowClick(key)}
    >
      <>
        <DragSource
          dragType={dragType}
          id={name}
          args={functionSnippet}
          key={key}
        >
          <span className={classes["function-item__header"]}>
            <TagList tags={functionTags ?? []} />
            <IconButton
              tooltip={intl.formatMessage({ id: "Tooltip.SQL.Editor.Add" })}
              onClick={(e: any) => {
                e?.stopPropagation();
                addFuncToSqlEditor(name, functionSnippet);
              }}
              className={classes["function-item__icon"]}
            >
              <dremio-icon
                name="interface/add-small"
                class={classes["function-item__add-icon"]}
              />
            </IconButton>
          </span>
          <span className={classes["function-item__label"]}>
            {functionLabel}
          </span>
        </DragSource>
        {description && (
          <span className={classes["function-item__description"]}>
            {description}
          </span>
        )}
        {isActiveRow && (
          <>
            <hr className={classes["function-item__divider"]} />
            {parameters.length > 0 && (
              <>
                <div className={classes["function-item__extraHeader"]}>
                  {intl.formatMessage({ id: "Dataset.FunctionArguments" })}
                </div>
                <ul>
                  {parameters.map((param: Parameter, i: number) => (
                    <li key={i}>
                      <b>{param.name ?? `x${i + 1}`}</b>
                      {`${
                        param.description ?? param?.type
                          ? `: ${param.description ?? param?.type}`
                          : ""
                      }`}
                    </li>
                  ))}
                </ul>
              </>
            )}
            {sampleCodes.length > 0 && (
              <>
                <div className={classes["function-item__extraHeader"]}>
                  {intl.formatMessage({ id: "Dataset.FunctionExamples" })}
                </div>
                {sampleCodes.map((sample: SampleCode, i: number) => (
                  <div
                    className={classes["function-item__samples"]}
                    key={i}
                  >{`${sample.call} → ${sample.result}`}</div>
                ))}
              </>
            )}
            {description && (
              <ExternalLink
                href={`https://docs.dremio.com/cloud/sql/sql-functions/functions/${name}/`}
                className={classes["function-item__learnMore"]}
                hideIcon
              >
                {intl.formatMessage({ id: "Common.LearnMoreNoDots" })}
              </ExternalLink>
            )}
          </>
        )}
      </>
    </div>
  );
};

const MemoizedSQLFunctionItem = React.memo(
  SQLFunctionItem,
  (prevProps: SQLFunctionItemProps, nextProps: SQLFunctionItemProps) =>
    prevProps.isActiveRow === nextProps.isActiveRow
);

export default MemoizedSQLFunctionItem;
