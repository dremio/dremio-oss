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
import { store } from "@app/store/store";
import { getRefQueryParamsFromPath } from "@app/utils/nessieUtils";
// @ts-ignore
import { getApiContext } from "dremio-ui-common/contexts/ApiContext.js";
import { VersionContext } from "dremio-ui-common/types/VersionContext.types";

export type GetEntitySummaryParams = {
  fullPath: string | string[];
  versionContext?: VersionContext;
};

export const getEntitySummaryUrl = ({
  fullPath,
  versionContext,
}: GetEntitySummaryParams) => {
  const params = versionContext
    ? {
        refType: versionContext.type,
        refValue: versionContext.value,
      }
    : getRefQueryParamsFromPath(fullPath, store.getState().nessie, "/");

  let joinedPath = "";

  if (fullPath) {
    if (typeof fullPath !== "string") {
      const newPath = fullPath;
      newPath[newPath.length - 1] = `"${newPath[newPath.length - 1]}"`;
      joinedPath = newPath.join("/");
    } else {
      joinedPath = fullPath;
    }
  }

  return new APIV2Call()
    .paths("datasets/summary")
    .paths(joinedPath)
    .params(params)
    .toString();
};

export const getEntitySummary = async (
  params: GetEntitySummaryParams
): Promise<any> => {
  return getApiContext()
    .fetch(getEntitySummaryUrl(params))
    .then((res: any) => res.json());
};
