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

import { createRequest } from "../_internal/createRequest.js";
import type {
  Config,
  ResourceConfig,
  SonarV3Config,
  V3Config,
} from "../_internal/types/Config.js";
import { Resources } from "./resources.js";

const getSonarResourceConfig = (config: Config) => (projectId: string) => {
  const request = createRequest(config);
  return {
    logger: config.logger,
    request,
    sonarV3Request: (path: string, init?: RequestInit): Promise<Response> =>
      request(`/v0/projects/${projectId}/${path}`, init),
    v3Request: (path: string, init?: RequestInit): Promise<Response> =>
      request(`/v0/${path}`, init),
  } satisfies ResourceConfig & SonarV3Config & V3Config;
};

export const Dremio = (config: Config) => {
  const sonarResourceConfig = getSonarResourceConfig(config);
  return {
    ...Resources(sonarResourceConfig),
    _request: createRequest(config),
    _sonarResourceConfig: sonarResourceConfig,
    _sonarV3Request: (projectId: string) =>
      sonarResourceConfig(projectId).sonarV3Request,
  };
};

export * from "../common/Query.js";

export { Resources as _Resources };
