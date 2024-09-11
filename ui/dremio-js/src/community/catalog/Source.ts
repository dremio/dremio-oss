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

import "temporal-polyfill/global";
import parseMilliseconds from "parse-ms";
import type {
  CommunitySource,
  CommunitySourceProperties,
} from "../../interfaces/Source.js";
import { CatalogReference } from "./CatalogReference.js";
import { catalogReferenceEntityToProperties, pathString } from "./utils.js";

export class Source implements CommunitySource {
  readonly acceleration: CommunitySource["acceleration"];
  readonly allowCrossSourceSelection: CommunitySource["allowCrossSourceSelection"];
  #children: CatalogReference[];
  readonly config: CommunitySource["config"];
  readonly createdAt: CommunitySource["createdAt"];
  readonly disableMetadataValidityCheck: CommunitySource["disableMetadataValidityCheck"];
  readonly id: CommunitySource["id"];
  readonly metadataPolicy: CommunitySource["metadataPolicy"];
  readonly name: CommunitySource["name"];
  readonly status: CommunitySource["status"];
  readonly type: CommunitySource["type"];

  protected readonly tag: string;

  constructor(
    properties: CommunitySourceProperties & {
      children: CatalogReference[];
      tag: string;
    },
  ) {
    this.acceleration = properties.acceleration;
    this.allowCrossSourceSelection = properties.allowCrossSourceSelection;
    this.#children = properties.children;
    this.config = properties.config;
    this.createdAt = properties.createdAt;
    this.disableMetadataValidityCheck = properties.disableMetadataValidityCheck;
    this.id = properties.id;
    this.metadataPolicy = properties.metadataPolicy;
    this.name = properties.name;
    this.status = properties.status;
    this.tag = properties.tag;
    this.type = properties.type;
  }

  children() {
    const c = this.#children;
    return {
      async *data() {
        yield* c;
      },
    };
  }

  get path() {
    return [this.name];
  }

  pathString = pathString(() => this.path);

  static fromResource(properties: any, retrieve: any) {
    return new Source({
      acceleration: {
        activePolicyType: properties.accelerationActivePolicyType,
        gracePeriod: Temporal.Duration.from(
          parseMilliseconds(properties.accelerationGracePeriodMs),
        ),
        neverExpire: properties.accelerationNeverExpire,
        neverRefresh: properties.accelerationNeverRefresh,
        refreshPeriod: Temporal.Duration.from(
          parseMilliseconds(properties.accelerationRefreshPeriodMs),
        ),
        refreshSchedule: properties.accelerationRefreshSchedule,
      },
      allowCrossSourceSelection: properties.allowCrossSourceSelection,
      children: properties.children.map(
        (child: any) =>
          new CatalogReference(
            catalogReferenceEntityToProperties(child),
            retrieve,
          ),
      ),
      config: properties.config,
      createdAt: new Date(properties.createdAt),
      disableMetadataValidityCheck: properties.disableMetadataValidityCheck,
      id: properties.id,
      metadataPolicy: {
        authTTL: Temporal.Duration.from(
          parseMilliseconds(properties.metadataPolicy.authTTLMs),
        ),
        autoPromoteDatasets: properties.metadataPolicy.autoPromoteDatasets,
        datasetExpireAfter: Temporal.Duration.from(
          parseMilliseconds(properties.metadataPolicy.datasetExpireAfterMs),
        ),
        datasetRefreshAfter: Temporal.Duration.from(
          parseMilliseconds(properties.metadataPolicy.datasetRefreshAfterMs),
        ),
        datasetUpdateMode: properties.metadataPolicy.datasetUpdateMode,
        deleteUnavailableDatasets:
          properties.metadataPolicy.deleteUnavailableDatasets,
        namesRefresh: Temporal.Duration.from(
          parseMilliseconds(properties.metadataPolicy.namesRefreshMs),
        ),
      },
      name: properties.name,
      status: properties.state.status,
      tag: properties.tag,
      type: properties.type,
    });
  }
}
