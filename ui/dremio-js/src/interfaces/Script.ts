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

import type { Ownable } from "./Ownable.js";
import type { Query } from "../common/Query.js";
import type { Result } from "ts-results-es";

export type ScriptProperties = {
  readonly createdAt: Temporal.Instant;
  readonly createdBy: string;
  readonly id: string;
  readonly modifiedAt: Temporal.Instant;
  readonly modifiedBy: Date;
  readonly name: string;
  readonly query: Query;
};

export type ScriptMethods = {
  delete(): Promise<Result<void, unknown>>;
  save(properties: {
    name?: ScriptProperties["name"];
    query?: ScriptProperties["query"];
  }): Promise<Result<Script, unknown>>;
};

export type EnterpriseScriptMethods = {
  delete(): Promise<Result<void, unknown>>;
  save(
    properties: Parameters<ScriptMethods["save"]>[0] & Partial<Ownable>,
  ): Promise<Result<EnterpriseScript, unknown>>;
};

export type Script = ScriptProperties & ScriptMethods;
export type CommunityScript = Script;
export type EnterpriseScript = Script & Ownable & EnterpriseScriptMethods;
