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

import type {
  ResourceConfig,
  SonarV3Config,
} from "../../_internal/types/Config.js";
import { Script } from "./Script.js";
import { Ok, Err } from "ts-results-es";
import { HttpError } from "../../common/HttpError.js";
import {
  isProblem,
  type Problem,
  type ValidationProblem,
} from "../../interfaces/Problem.js";
import type { Query } from "../../common/Query.js";

const scriptNotFoundError = (id: string) =>
  ({
    meta: {
      provided: {
        id,
      },
    },
    title:
      "The script could not be found or you do not have permission to view it",
    type: "https://api.dremio.dev/problems/scripts/not-found",
  }) as const satisfies Problem;

const duplicateScriptNameError = {
  errors: [
    {
      detail: "A script with this name already exists",
      pointer: "#/name",
      type: "https://api.dremio.dev/problems/validation/field-conflict",
    },
  ],
  title: "There was a problem validating the content of the request",
  type: "https://api.dremio.dev/problems/validation-problem",
} as const satisfies ValidationProblem;

export const ScriptsResource = (config: ResourceConfig & SonarV3Config) => {
  const retrieve = (id: string) =>
    config
      .sonarV3Request(`scripts/${id}`)
      .then((res) => res.json())
      .then((properties) => Ok(Script.fromResource(properties, config)))
      .catch((e) => {
        if (e instanceof HttpError && e.status === 404) {
          return Err(scriptNotFoundError(id));
        }
        return Err(e);
      });

  const store = (properties: { name: string; query: Query }) => {
    const { query, ...rest } = properties;
    return config
      .sonarV3Request("scripts", {
        body: JSON.stringify({
          ...rest,
          content: query.sql,
          context: query.context,
        }),
        headers: {
          "Content-Type": "application/json",
        },
        keepalive: true,
        method: "POST",
      })
      .then((res) => res.json())
      .then((properties) => Ok(Script.fromResource(properties, config)))
      .catch((e) => {
        if (e instanceof HttpError && isProblem(e.body)) {
          if (e.body.detail?.includes("Cannot reuse the same script name"))
            return Err(duplicateScriptNameError);
        }
        return Err(e);
      });
  };

  return {
    list() {
      return {
        async *data() {
          yield* await config
            .sonarV3Request("scripts?maxResults=1000")
            .then((res) => res.json())
            .then(
              (response) =>
                response.data.map((properties: any) =>
                  Script.fromResource(properties, config),
                ) as Script[],
            );
        },
      };
    },
    retrieve,
    store,
  };
};
