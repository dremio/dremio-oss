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
import { FunctionSignature, ModelFunction } from "@app/types/sqlFunctions";
import localStorageUtils from "@inject/utils/storageUtils/localStorageUtils";
import { cloneDeep } from "lodash";

export type ModifiedSQLFunction = ModelFunction & {
  key: string;
  signature: FunctionSignature;
};

const listSqlFunctionsURL = new APIV2Call().paths("sql/functions").toString();

function isLetter(c: string) {
  return c.toLowerCase() != c.toUpperCase();
}

export const listSqlFunctions = (): Promise<ModifiedSQLFunction[]> =>
  fetch(listSqlFunctionsURL, {
    headers: {
      Authorization: (localStorageUtils as any)?.getAuthToken?.(),
    },
  })
    .then((res: any) => res.json())
    .then((res: any) => {
      const nonAlphabetFunctions = (res?.functions ?? [])?.filter(
        (fn: ModelFunction) => !isLetter(fn?.name[0])
      );
      const sortedFunctions = (res?.functions ?? [])
        ?.filter((fn: ModelFunction) => isLetter(fn?.name[0]))
        .sort((a: ModelFunction, b: ModelFunction) => {
          if (a.name.toLowerCase() > b.name.toLowerCase()) return 1;
          else if (b.name.toLowerCase() > a.name.toLowerCase()) return -1;
          else return 0;
        });
      const allSortedFunctions = sortedFunctions.concat(nonAlphabetFunctions);
      return allSortedFunctions.flatMap((fn: ModelFunction) => {
        return fn?.signatures?.map(
          (signature: FunctionSignature, idx: number) => {
            const newFunction = {
              ...cloneDeep(fn),
              signature: signature,
              key: fn.name + idx,
            };
            delete newFunction.signatures;
            return newFunction;
          }
        );
      });
    });
