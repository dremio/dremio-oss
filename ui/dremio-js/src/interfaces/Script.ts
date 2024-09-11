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

import type { Result } from "ts-results-es";
import type { Query } from "../common/Query.js";

export type CommunityScriptProperties = {
  readonly createdAt: Date;
  readonly createdBy: string;
  readonly id: string;
  readonly modifiedAt: Date;
  readonly modifiedBy: Date;
  readonly name: string;
  readonly query: Query;
};

export type CommunityScriptMethods = {
  delete(): Promise<Result<void, unknown>>;
  save(properties: {
    name?: CommunityScriptProperties["name"];
    query?: CommunityScriptProperties["query"];
  }): Promise<Result<CommunityScript, unknown>>;
};

export type CommunityScript = CommunityScriptProperties &
  CommunityScriptMethods;

export type EnterpriseScriptProperties = CommunityScriptProperties & {
  owner: string;
};

export type EnterpriseScriptMethods = {
  save(properties: {
    name?: CommunityScriptProperties["name"];
    owner?: EnterpriseScriptProperties["owner"];
    query?: CommunityScriptProperties["query"];
  }): Promise<Result<CommunityScript, unknown>>;
  delete(): Promise<Result<void, unknown>>;
};

export type EnterpriseScript = EnterpriseScriptProperties &
  EnterpriseScriptMethods;
