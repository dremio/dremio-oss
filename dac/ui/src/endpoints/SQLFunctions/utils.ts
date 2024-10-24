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

import { Parameter, ParameterKindEnum } from "#oss/types/sqlFunctions";

export function isLetter(c: string) {
  return c.toLowerCase() != c.toUpperCase();
}

function constructParamName(label: string, isOptional: boolean) {
  return isOptional ? `[${label}]` : `${label}`;
}

function constructParamNameWithComma(label: string, isOptional: boolean) {
  return isOptional ? ` [,${label}]` : `, ${label}`;
}

export function constructLabel(
  parameters: Parameter[],
  snippetOverride?: string,
) {
  if (snippetOverride) {
    const paramsArray: string[] = [];
    parameters.forEach((param) => {
      paramsArray.push(`${param.type}${param?.name ? ` ${param.name}` : ""}`);
    });
    return constructLabelFromSnippet(snippetOverride, paramsArray);
  } else {
    return contructLabelFromParams(parameters);
  }
}

function contructLabelFromParams(parameters: Parameter[]) {
  let params = "";
  if (parameters.length > 0) {
    parameters.forEach((param: Parameter, idx: number) => {
      const name = `${param.type}${param?.name ? ` ${param.name}` : ""}`;
      if (idx === 0) {
        params += constructParamName(
          name,
          param?.kind === ParameterKindEnum.OPTIONAL,
        );
      } else {
        params += constructParamNameWithComma(
          name,
          param?.kind === ParameterKindEnum.OPTIONAL,
        );
      }
    });
  }

  return params;
}

function constructLabelFromSnippet(snippet: string, params: string[]) {
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
}
