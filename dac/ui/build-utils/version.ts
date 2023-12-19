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
import * as env from "env-var";

const DREMIO_VERSION = env
  .get("DREMIO_VERSION")
  .default("DREMIO_VERSION_IS_NOT_PROVIDED")
  .asString();

export const dremioBeta = env.get("DREMIO_BETA").default("false").asBool();

export const dremioEdition = env.get("EDITION_TYPE").default("ce").asString();

export const dremioVersion = `${DREMIO_VERSION}-${dremioEdition}`;
