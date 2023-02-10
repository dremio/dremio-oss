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
import { defineRoute } from "define-route";
import { getSonarContext } from "../contexts/SonarContext";

type SourceType = "objectStorage" | "metastore" | "external" | "dataplane";
type SourceTypeParam = { sourceType: SourceType };
type ResourceIdParam = { resourceId: string };
type SourceNameParam = { sourceName: string };

export const projectBase = defineRoute(getSonarContext().getProjectBaseRoute);

/** home */
export const home = projectBase.extend(() => "home");
export const resource = home.extend(
  (params: ResourceIdParam) => `${params.resourceId}/folder/**`
);
export const redirect = projectBase.extend(() => "*/**/");
export const redirectTo = projectBase.extend(() => "*/**");

/** Datasets */
export const datasets = projectBase.extend(() => "datasets");
export const space = projectBase.extend(
  (params: ResourceIdParam) => `space/${params.resourceId}`
);
export const spaceFolder = space.extend(() => `folder/**`);

/** Sources */
export const sources = projectBase.extend(() => "sources");
export const objectStorage = sources.extend(() => "objectStorage/list");
export const metastore = sources.extend(() => "metastore/list");
export const external = sources.extend(() => "external/list");
export const dataplaneSource = sources.extend(
  (params: SourceNameParam) => `dataplane/${params.sourceName}`
);
export const dataplane = sources.extend(() => "dataplane/list");
export const arcticSource = sources.extend(
  (params: SourceNameParam) => `arctic/${params.sourceName}`
);
export const source = projectBase.extend(
  (params: ResourceIdParam) => `source/${params.resourceId}`
);
export const sourceFolder = source.extend(() => `folder/**`);

/** Listing */
export const projectsList = defineRoute(() => "/sonar");
export const sourcesList = projectBase.extend(
  (params: SourceTypeParam) => `sources/${params.sourceType}/list`
);
export const allSourcesList = projectBase.extend(() => "sources/list");
export const spacesList = projectBase.extend(() => "spaces/list");
