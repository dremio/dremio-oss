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
import React from "react";
import { ExternalLink, IconButton } from "dremio-ui-lib/components";
import DragSource from "components/DragComponents/DragSource";
import { intl } from "@app/utils/intl";
import { ModifiedSQLFunction } from "@app/endpoints/SQLFunctions/listSQLFunctions";
import {
  Parameter,
  ParameterKindEnum,
  SampleCode,
} from "@app/types/sqlFunctions";
import TextHighlight from "@app/components/TextHighlight";
import { Tag } from "../TagsEditor/Tag";

import * as classes from "./SQLFunctionItem.module.less";

type SQLFunctionItemProps = {
  sqlFunction: ModifiedSQLFunction;
  addFuncToSqlEditor: (name: string, args?: string) => void;
  onRowClick: (key: string) => void;
  isActiveRow: boolean;
  dragType: string;
  searchKey: string;
};

const SQLFunctionItem = ({
  sqlFunction,
  addFuncToSqlEditor,
  onRowClick,
  isActiveRow,
  dragType,
  searchKey,
}: SQLFunctionItemProps) => {
  const {
    name,
    signature,
    description,
    key,
    link,
    tags: functionTags,
    label: functionLabel,
    snippet: functionSnippet,
  } = sqlFunction;
  const { parameters = [], sampleCodes = [] } = signature;

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
            <div className={classes["function-item__header__tags"]}>
              {functionTags.length > 0 &&
                functionTags.map((tag) => <Tag text={tag} title key={tag} />)}
            </div>
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
            <TextHighlight
              text={name}
              inputValue={searchKey}
              showTooltip={false}
            />
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
                      <b>
                        {param.name ?? `x${i + 1}`}
                        {param?.kind === ParameterKindEnum.OPTIONAL
                          ? intl.formatMessage({ id: "Dataset.OptionalParam" })
                          : ""}
                      </b>
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
                href={link}
                className={classes["function-item__learnMore"]}
                hideIcon
                onClick={(e: any) => {
                  e?.stopPropagation();
                }}
              >
                <>{intl.formatMessage({ id: "Common.LearnMoreNoDots" })}</>
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
    // return false if one of these props are different
    !(
      prevProps.isActiveRow !== nextProps.isActiveRow ||
      prevProps.searchKey !== nextProps.searchKey
    )
);

export default MemoizedSQLFunctionItem;
