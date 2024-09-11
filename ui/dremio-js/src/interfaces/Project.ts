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

export type ProjectProperties = {
  readonly cloudId: string;
  readonly cloudType: "AWS" | "AZURE" | "UNKNOWN";
  readonly createdAt: Date;
  readonly createdBy: string;
  readonly credentials: Record<string, any>;
  readonly id: string;
  readonly lastStateError: {
    error: string;
    timestamp: Date;
  } | null;
  readonly modifiedAt: Date;
  readonly modifiedBy: string;
  readonly name: string;
  readonly numberOfEngines: number;
  readonly primaryCatalog: string;
  readonly projectStore: string;
  readonly state:
    | "ACTIVATING"
    | "ACTIVE"
    | "ARCHIVED"
    | "ARCHIVING"
    | "CREATING"
    | "DEACTIVATING"
    | "INACTIVE"
    | "RESTORING";
  readonly type: "QUERY_ENGINE";
};

export type ProjectMethods = {
  delete(): Promise<Result<undefined, unknown>>;
};

export type Project = ProjectProperties & ProjectMethods;
