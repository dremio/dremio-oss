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
import { Err, Ok } from "ts-results-es";
import type { SonarV3Config } from "../../_internal/types/Config.js";
import type { SignalParam } from "../../_internal/types/Params.js";
import type {
  BaseCatalogReferenceProperties,
  CatalogReference,
} from "../../interfaces/CatalogReference.js";
import { catalogReferenceFromProperties } from "./catalogReferenceFromProperties.js";
import { catalogReferenceEntityToProperties, pathString } from "./utils.js";

abstract class BaseCatalogReference {
  readonly id: string;
  readonly path: string[];
  abstract readonly type: string;

  constructor(properties: BaseCatalogReferenceProperties) {
    this.id = properties.id;
    this.path = properties.path;
  }

  get name() {
    return this.path.at(-1)!;
  }

  pathString = pathString(() => this.path);
}

export class DatasetCatalogReference extends BaseCatalogReference {
  readonly type: `DATASET_${"DIRECT" | "PROMOTED" | "VIRTUAL"}`;

  constructor(
    properties: BaseCatalogReferenceProperties & {
      type: `DATASET_${"DIRECT" | "PROMOTED" | "VIRTUAL"}`;
    },
  ) {
    super(properties);
    this.type = properties.type;
  }
}

export class FileCatalogReference extends BaseCatalogReference {
  readonly type = "FILE";
}

export class FolderCatalogReference extends BaseCatalogReference {
  readonly type = "FOLDER";
  #config: SonarV3Config;
  constructor(
    properties: BaseCatalogReferenceProperties,
    config: SonarV3Config,
  ) {
    super(properties);
    this.#config = config;
  }
  children() {
    return catalogChildren(this, this.#config);
  }
}

export class FunctionCatalogReference extends BaseCatalogReference {
  readonly type = "FUNCTION";
}

export class HomeCatalogReference extends BaseCatalogReference {
  readonly type = "HOME";
  #config: SonarV3Config;

  constructor(
    properties: BaseCatalogReferenceProperties,
    config: SonarV3Config,
  ) {
    super(properties);
    this.#config = config;
  }

  children() {
    return catalogChildren(this, this.#config);
  }
}

export class SourceCatalogReference extends BaseCatalogReference {
  readonly type = "SOURCE";
  #config: SonarV3Config;

  constructor(
    properties: BaseCatalogReferenceProperties,
    config: SonarV3Config,
  ) {
    super(properties);
    this.#config = config;
  }

  children() {
    return catalogChildren(this, this.#config);
  }
}

export class SpaceCatalogReference extends BaseCatalogReference {
  readonly type = "SPACE";
  #config: SonarV3Config;

  constructor(
    properties: BaseCatalogReferenceProperties,
    config: SonarV3Config,
  ) {
    super(properties);
    this.#config = config;
  }

  children() {
    return catalogChildren(this, this.#config);
  }
}

const catalogChildren = (
  catalogReference: CatalogReference,
  config: SonarV3Config,
) => {
  const getPage = (params: { nextPageToken?: string } & SignalParam = {}) => {
    const searchParams = new URLSearchParams();
    searchParams.set("maxChildren", "20");
    if (params.nextPageToken) {
      searchParams.set("pageToken", params.nextPageToken);
    }
    return config
      .sonarV3Request(
        `catalog/by-path/${catalogReference.path.map(encodeURIComponent).join("/")}?${searchParams.toString()}`,
        { signal: params.signal },
      )
      .then((res) => res.json())
      .then((response: { children: unknown[]; nextPageToken?: string }) => {
        return Ok({
          data: response.children.map((entity: unknown) =>
            catalogReferenceFromProperties(
              catalogReferenceEntityToProperties(entity),
              config,
            ),
          ),
          nextPageToken: response.nextPageToken,
        });
      })
      .catch((e) => Err(e));
  };
  return {
    async *data({ signal }: SignalParam = {}) {
      const firstPage = (await getPage({ signal })).unwrap();
      yield* firstPage.data;
      let nextPageToken = firstPage.nextPageToken;
      while (nextPageToken) {
        const nextPage = (await getPage({ nextPageToken, signal })).unwrap();
        yield* nextPage.data;
        nextPageToken = nextPage.nextPageToken;
      }
    },
    getPage,
  };
};
