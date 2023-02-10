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

import { ARCTIC } from "@app/constants/sourceTypes";
import { ConflictError } from "dremio-ui-common/errors/ConflictError";
import { InvalidParamsError } from "dremio-ui-common/errors/InvalidParamsError";
import { getApiContext } from "dremio-ui-common/contexts/ApiContext.js";
import { SonarProject } from "../SonarProjects/listSonarProjects";
import { ArcticCatalog } from "./ArcticCatalog.type";

type AddToSonarParams = {
  project: SonarProject;
  catalog: ArcticCatalog;
};

export const addCatalogToSonar = (params: AddToSonarParams): Promise<any> => {
  return getApiContext()
    .fetch(`/ui/projects/${params.project.id}/source/${params.catalog.name}`, {
      method: "put",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        name: params.catalog.name,
        type: ARCTIC,
        config: {
          arcticCatalogId: params.catalog.id,
          credentialType: "PROJECT_DATA_CRED",
          awsRootPath: params.project.projectStore,
          propertyList: [],
          asyncEnabled: true,
          isCachingEnabled: true,
          maxCacheSpacePct: 100,
        },
      }),
    })
    .then((res) => res.json())
    .catch((err) => {
      if (err instanceof ConflictError) {
        throw new InvalidParamsError(err.res, {
          type: "invalid_params",
          errors: [
            {
              code: "key_exists",
              source: {
                pointer: "/projectId",
              },
              meta: {
                provided: params.catalog.name,
              },
            },
          ],
        });
      }
      throw err;
    });
};
