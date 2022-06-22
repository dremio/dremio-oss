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
import * as NessieActions from "@app/actions/nessie/nessie";
import { COMMIT_TYPE, NESSIE_REF_PREFIX } from "@app/constants/nessie";
import { cloneDeep } from "lodash";
import { initialState, NessieRootState } from "./nessie";

export function getNessieRequestActions() {
  return Object.keys(NessieActions)
    .filter((cur) => cur.includes("_REQUEST"))
    .map((entry) => (NessieActions as any)[entry]);
}

//Copy current browsing context to reference picker state
export function initializeRefState(state: NessieRootState) {
  return Object.keys(state)
    .filter((key) => !key.startsWith(NESSIE_REF_PREFIX))
    .reduce((acc, cur) => {
      acc[`${NESSIE_REF_PREFIX}${cur}`] = cloneDeep(state[cur]);
      return acc;
    }, {} as NessieRootState);
}

// Takes in references stored in dataset (e.g. from review/preview/new_untitled/new_untitled_sql_and_run/transformAndRun)
// Outputs NessieRootState with updated references (intitializes the nessie ref picker to dataset values)
export function initializeDatasetRefs(
  state: NessieRootState,
  references: NessieActions.DatasetReference
) {
  if (!references) return {};

  return Object.keys(references).reduce(
    (acc: NessieRootState, cur: string) => {
      const key = `${NESSIE_REF_PREFIX}${cur}`;
      if (!acc[key]) acc[key] = { ...initialState };
      const ref = references[cur];
      //@ts-ignore
      acc[key] =
        ref.type === COMMIT_TYPE
          ? {
              ...acc[key],
              hash: ref.value,
            }
          : {
              ...acc[key],
              reference: {
                type: ref.type,
                name: ref.value,
              },
            };
      return acc;
    },
    { ...state }
  );
}
