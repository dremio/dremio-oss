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
import { Err, Ok, Result } from "ts-results-es";
import type {
  ResourceConfig,
  SonarV3Config,
} from "../../_internal/types/Config.js";
import { User } from "../../community/users/User.js";
import { userEntityToProperties } from "./utils.js";
import type { SignalParam } from "../../_internal/types/Params.js";

export const UsersResource = (config: ResourceConfig & SonarV3Config) => {
  return {
    /**
     * @hidden
     * @internal
     */
    _createFromEntity: (properties: unknown) =>
      new User(userEntityToProperties(properties)),
    retrieve: (
      id: string,
      { signal }: SignalParam = {},
    ): Promise<Result<User, unknown>> =>
      config
        .sonarV3Request(`user/${id}`, { signal })
        .then((res) => res.json())
        .then((properties) => Ok(new User(userEntityToProperties(properties))))
        .catch((e) => Err(e)),
    retrieveByEmail: (
      email: string,
      { signal }: SignalParam = {},
    ): Promise<Result<User, unknown>> =>
      config
        .sonarV3Request(`user/by-name/${email}`, { signal })
        .then((res) => res.json())
        .then((properties) => Ok(new User(userEntityToProperties(properties))))
        .catch((e) => Err(e)),
  };
};
