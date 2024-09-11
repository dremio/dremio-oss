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

import { useMemo } from "react";
import { cloneDeep } from "lodash";
import { getSonarContext } from "../../../contexts/SonarContext";
import { ModifiedFunction, Parameter } from "../Functions.type";
import { useFunctions } from "./useFunctions";

const getDocsLink = (functionName: string) =>
  `https://docs.dremio.com/${!getSonarContext().getSelectedProjectId ? "current" : "cloud"}/reference/sql/sql-functions/functions/${functionName}/`;

const isLetter = (c: string) => {
  return c.toLowerCase() != c.toUpperCase();
};

const constructParamName = (label: string, isOptional: boolean) => {
  return isOptional ? `[${label}]` : `${label}`;
};

const constructParamNameWithComma = (label: string, isOptional: boolean) => {
  return isOptional ? ` [,${label}]` : `, ${label}`;
};

const contructLabelFromParams = (parameters: Parameter[]) => {
  let params = "";
  if (parameters.length > 0) {
    parameters.forEach((param, idx) => {
      const name = `${param.type}${param?.name ? ` ${param.name}` : ""}`;
      if (idx === 0) {
        params += constructParamName(name, param?.kind === "OPTIONAL");
      } else {
        params += constructParamNameWithComma(name, param?.kind === "OPTIONAL");
      }
    });
  }

  return params;
};

const constructLabelFromSnippet = (snippet: string, params: string[]) => {
  let snippetLabel = "";
  let stopConcat = false;
  let arg = "";
  const reversedParams = params.reverse();
  for (let i = 0; i < snippet.length; i++) {
    if (snippet[i] + snippet[i + 1] === "${") {
      stopConcat = true;
    }

    if (snippet[i] === "}") {
      if (!arg.includes("|")) {
        const curParam = reversedParams.pop();
        snippetLabel += curParam || arg.substring(4, arg.length);
      }
      arg = "";
      stopConcat = false;
      continue;
    }

    if (!stopConcat) {
      snippetLabel += snippet[i];
    } else {
      arg += snippet[i];
    }
  }
  return snippetLabel.trimStart();
};

const constructLabel = (parameters: Parameter[], snippetOverride: string) => {
  if (snippetOverride) {
    const paramsArray: string[] = [];
    parameters.forEach((param) => {
      paramsArray.push(`${param.type}${param.name ? ` ${param.name}` : ""}`);
    });
    return constructLabelFromSnippet(snippetOverride, paramsArray);
  } else {
    return contructLabelFromParams(parameters);
  }
};

/**
 * Gets and returns modified SQL functions to use with autocomplete
 */
export const useModifiedFunctions = (): ModifiedFunction[] => {
  const { value: functionsObj } = useFunctions();

  return useMemo(() => {
    const functions = functionsObj?.functions ?? [];
    const nonAlphabetFunctions = functions.filter(
      (fn) => !isLetter(fn.name[0]),
    );
    const sortedFunctions = functions
      .filter((fn) => isLetter(fn.name[0]))
      .sort((a, b) => {
        if (a.name.toLowerCase() > b.name.toLowerCase()) return 1;
        else if (b.name.toLowerCase() > a.name.toLowerCase()) return -1;
        else return 0;
      });
    const allSortedFunctions = sortedFunctions.concat(nonAlphabetFunctions);
    allSortedFunctions.forEach((fn) => {
      fn.functionCategories = fn.functionCategories?.sort();
    });

    return allSortedFunctions.reduce(
      (modifiedFunctions: ModifiedFunction[], fn) => {
        if (!fn.signatures) {
          return modifiedFunctions;
        }

        return modifiedFunctions.concat(
          fn.signatures.map((signature) => {
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

            return {
              ...cloneDeep(fn),
              label,
              link: getDocsLink(fn.name),
              snippet,
            };
          }),
        );
      },
      [],
    );
  }, [functionsObj?.functions]);
};
