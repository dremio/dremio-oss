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

import { APIV2Call } from "@app/core/APICall";
import {
  FunctionSignature,
  ModelFunction,
  Parameter,
  ParameterKindEnum,
} from "@app/types/sqlFunctions";
import localStorageUtils from "@inject/utils/storageUtils/localStorageUtils";
import { cloneDeep } from "lodash";
// @ts-ignore
import { getDocsLink } from "@inject/utils/versionUtils";
import { FunctionCategoryLabels } from "@app/utils/sqlFunctionUtils";

export type ModifiedSQLFunction = ModelFunction & {
  key: string;
  signature: FunctionSignature;
  link: string;
  label: string;
  tags: any[];
  snippet: string;
};

const listSqlFunctionsURL = new APIV2Call().paths("sql/functions").toString();

function isLetter(c: string) {
  return c.toLowerCase() != c.toUpperCase();
}

function constructParamName(label: string, isOptional: boolean) {
  return isOptional ? `[${label}]` : `${label}`;
}

function constructParamNameWithComma(label: string, isOptional: boolean) {
  return isOptional ? ` [,${label}]` : `, ${label}`;
}

export const listSqlFunctions = (): Promise<ModifiedSQLFunction[]> =>
  fetch(listSqlFunctionsURL, {
    headers: {
      Authorization: (localStorageUtils as any)?.getAuthToken?.(),
    },
  })
    .then((res: any) => res.json())
    .then((res: any) => {
      const documentedFunctions = (res?.functions ?? []).filter(
        (fn: ModelFunction) => fn.description != null
      );
      const nonAlphabetFunctions = documentedFunctions.filter(
        (fn: ModelFunction) => !isLetter(fn?.name[0])
      );
      const sortedFunctions = documentedFunctions
        .filter((fn: ModelFunction) => isLetter(fn?.name[0]))
        .sort((a: ModelFunction, b: ModelFunction) => {
          if (a.name.toLowerCase() > b.name.toLowerCase()) return 1;
          else if (b.name.toLowerCase() > a.name.toLowerCase()) return -1;
          else return 0;
        });
      const allSortedFunctions = sortedFunctions.concat(nonAlphabetFunctions);
      allSortedFunctions.forEach((fn: ModelFunction) => {
        fn.functionCategories = fn.functionCategories?.sort();
      });
      return allSortedFunctions.flatMap((fn: ModelFunction) => {
        return fn?.signatures?.map(
          (signature: FunctionSignature, idx: number) => {
            const { parameters = [], returnType, snippetOverride } = signature;

            let params = "";
            if (parameters.length > 0) {
              parameters.forEach((param: Parameter, idx: number) => {
                const name = `${param.type}${
                  param?.name ? ` ${param.name}` : ""
                }`;
                if (idx === 0) {
                  params += constructParamName(
                    name,
                    param?.kind === ParameterKindEnum.OPTIONAL
                  );
                } else {
                  params += constructParamNameWithComma(
                    name,
                    param?.kind === ParameterKindEnum.OPTIONAL
                  );
                }
              });
            }

            let snippet = "($1)";
            if (snippetOverride) {
              // BE response snippet is `<name>()`, and monaco only reads `()`
              snippet = snippetOverride.substring(
                fn.name.length,
                snippetOverride.length
              );
            }

            const label = `(${params}) → ${returnType}`;
            const tags =
              fn.functionCategories?.map(
                (cat) => FunctionCategoryLabels[cat]
              ) ?? [];

            const newFunction = {
              ...cloneDeep(fn),
              signature: signature,
              key: fn.name + idx,
              link: `${getDocsLink?.()}/${fn.name}/`,
              snippet: snippet,
              label: label,
              tags: tags,
            };
            delete newFunction.signatures;
            return newFunction;
          }
        );
      });
    });
