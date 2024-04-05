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
import { projectBase } from "./common";

type ExistingDataset = {
  resources: string;
  resourceId: string;
  tableId: string;
  pageType: string;
};
type NewQueryParam = {
  projectId?: string;
  resourceId?: string;
  version?: string;
  tipVersion?: string;
  jobId?: string;
};
type SqlEditorParam = {
  pageType?: string;
};

export const sqlEditor = projectBase.extend(
  (params: SqlEditorParam = {}) =>
    `new_query${params.pageType ? `(/${params?.pageType})` : ""}`,
);

export const existingDataset = projectBase.extend(
  (params: ExistingDataset) =>
    `${params.resources}(/${params.resourceId})/${params.tableId}(/${params.pageType})`,
);

export const newQuery = projectBase.extend(
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  ({ projectId: _ignored, ...params }: NewQueryParam = {}) => {
    const searchParams = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value)
        searchParams.set(key === "resourceId" ? "context" : key, value);
    });
    const qs = searchParams.toString();
    return "new_query" + (qs ? `?${qs}` : "");
  },
);

export const unsavedDatasetPath = projectBase.extend(() => "tmp/tmp/UNTITLED");
export const unsavedDatasetPathUrl = projectBase.extend(() => "tmp/UNTITLED");
