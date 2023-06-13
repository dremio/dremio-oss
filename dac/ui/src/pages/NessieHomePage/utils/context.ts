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
import { NessieRootState, NessieState } from "@app/types/nessie";
import { selectState } from "@app/selectors/nessie/nessie";
import { DefaultApi, V2BetaApi } from "@app/services/nessie/client";
import { getTreeApi, getApiV2 } from "@app/services/nessie/impl/TreeApi";
import { createContext, useContext } from "react";
import * as commonPaths from "dremio-ui-common/paths/common.js";
import { getSonarContext } from "dremio-ui-common/contexts/SonarContext.js";

type SourceInfo = {
  name: string;
  id: string;
  endpoint?: string;
  endpointV1?: string;
};

type NessieContextType = {
  source: SourceInfo;
  state: NessieState;
  api: DefaultApi;
  apiV2: V2BetaApi;
  baseUrl: string;
  stateKey: string;
};

export const NessieContext = createContext<NessieContextType | null>(null);

export function useNessieContext(): NessieContextType {
  const context = useContext(NessieContext);
  if (context === null) throw new Error("Context is null");
  return context as NessieContextType;
}

export function createNessieContext(
  source: SourceInfo,
  state: NessieRootState,
  prefix = "",
  baseUrl = !source.endpoint && !source.endpointV1
    ? ""
    : commonPaths.dataplaneSource.link({
        sourceName: source.name,
        projectId: getSonarContext().getSelectedProjectId?.(),
      })
): NessieContextType {
  const stateKey = `${prefix}${source.name}`;
  return {
    source,
    stateKey,
    state: selectState(state, stateKey),
    api: getTreeApi(source.endpointV1),
    apiV2: getApiV2(source.endpoint),
    baseUrl, //Different routes for Dataplane only and Dataplane source
  };
}
