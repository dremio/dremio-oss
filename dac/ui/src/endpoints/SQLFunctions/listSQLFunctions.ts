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

import { FunctionSignature, ModelFunction } from "#oss/types/sqlFunctions";
import localStorageUtils from "@inject/utils/storageUtils/localStorageUtils";
import { cloneDeep } from "lodash";
// @ts-ignore
import { getDocsLink } from "@inject/utils/versionUtils";
import { FunctionCategoryLabels } from "#oss/utils/sqlFunctionUtils";
import apiUtils from "#oss/utils/apiUtils/apiUtils";
import { constructLabel, isLetter } from "./utils";

export type ModifiedSQLFunction = ModelFunction & {
  key: string;
  signature: FunctionSignature;
  link: string;
  label: string;
  tags: any[];
  snippet: string;
};

const listSqlFunctionsURL = "sql/functions";

export const listSqlFunctions = async (): Promise<ModifiedSQLFunction[]> => {
  const token = (localStorageUtils as any)?.getAuthToken?.();
  return await apiUtils
    .fetch(
      listSqlFunctionsURL,
      {
        headers: {
          ...(token && { Authorization: token }),
        },
      },
      2,
    )
    .then((res: any) => res.json())
    .then((res: any) => {
      const functions = res?.functions ?? [];
      const nonAlphabetFunctions = functions.filter(
        (fn: ModelFunction) => !isLetter(fn?.name[0]),
      );
      const sortedFunctions = functions
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

            let snippet = parameters.length > 0 ? "($1)" : "()"; // Put cursor inside the snippet if there are params
            if (snippetOverride) {
              // BE response snippet is `<name>()`, and monaco only reads `()`
              snippet = snippetOverride.substring(
                fn.name.length,
                snippetOverride.length,
              );
            }

            const label = `(${constructLabel(
              parameters,
              snippetOverride?.substring(
                fn.name.length + 1,
                snippetOverride.length - 1,
              ) || "",
            )}) → ${returnType}`;
            const tags =
              fn.functionCategories?.map(
                (cat) => FunctionCategoryLabels[cat],
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
          },
        );
      });
    });
};
