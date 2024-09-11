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

import { Err, Ok, type Result } from "ts-results-es";
import type { SonarV3Config } from "../../_internal/types/Config.js";
import { Folder } from "./Folder.js";
import { Dataset } from "./Dataset.js";
import { Source } from "./Source.js";
import { CatalogReference } from "./CatalogReference.js";
import { Home } from "./Home.js";
import { Space } from "./Space.js";
import { File } from "./File.js";
import { VersionedDataset } from "./VersionedDataset.js";
import { catalogReferenceEntityToProperties } from "./utils.js";
import { emptyPathError, unableToRetrieveProblem } from "./catalogErrors.js";
import { batch } from "@e3m-io/batch-fn-calls";
import type { CatalogObject } from "../../interfaces/CatalogObject.js";
import { CatalogFunction } from "./CatalogFunction.js";
import { HttpError } from "../../common/HttpError.js";

export const CatalogResource = (config: SonarV3Config) => {
  const retrieve = batch(async (ids: Set<string>) => {
    const results = new Map<string, Result<CatalogObject, unknown>>();

    if (ids.size === 1) {
      const id = Array.from(ids).at(0)!;
      return config
        .sonarV3Request(`catalog/${id}`)
        .then((res) => res.json())
        .then((response) => {
          results.set(id, Ok(resolveCatalogObject(response)));
          return results;
        })
        .catch((e) => {
          if (e instanceof HttpError && (e.body as any)?.detail) {
            results.set(
              id,
              Err(unableToRetrieveProblem((e.body as any).detail)),
            );
          } else {
            results.set(id, Err(unableToRetrieveProblem()));
          }

          return results;
        });
    }

    const idsArray = Array.from(ids);
    const chunks = [];

    while (idsArray.length > 0) {
      chunks.push(idsArray.splice(0, Math.min(50, idsArray.length)));
    }

    const catalogItems = await Promise.all(
      chunks.map((chunk) =>
        config
          .sonarV3Request(`catalog/by-ids?maxChildren=20`, {
            body: JSON.stringify(chunk),
            headers: { "Content-Type": "application/json" },
            method: "POST",
          })
          .then((res) => res.json())
          .then((response) => response.data),
      ),
    ).then((chunks) => chunks.flat());

    for (const catalogItem of catalogItems) {
      results.set(catalogItem.id, Ok(resolveCatalogObject(catalogItem)));
    }

    for (const id of ids) {
      if (!results.has(id)) {
        results.set(id, Err(unableToRetrieveProblem()));
      }
    }

    return results;
  });

  const retrieveByPath = batch(async (paths: Set<string[]>) => {
    const results = new Map<string[], Result<CatalogObject, unknown>>();

    // Because the key is a reference type (array), we need to store the
    // original path reference
    const originalKeyRef = new Map<string, string[]>();
    for (const key of paths) {
      originalKeyRef.set(JSON.stringify(key), key);
    }

    if (paths.size === 1) {
      const path = Array.from(paths).at(0)!;
      return config
        .sonarV3Request(
          `catalog/by-path/${path.map(encodeURIComponent).join("/")}`,
        )
        .then((res) => res.json())
        .then((response) => {
          results.set(path, Ok(resolveCatalogObject(response)));
          return results;
        })
        .catch((e) => {
          if (e instanceof HttpError && (e.body as any)?.detail) {
            results.set(
              path,
              Err(unableToRetrieveProblem((e.body as any).detail)),
            );
          } else {
            results.set(path, Err(unableToRetrieveProblem()));
          }
          return results;
        });
    }

    const pathsArray = Array.from(paths);
    const chunks = [];

    while (pathsArray.length > 0) {
      chunks.push(pathsArray.splice(0, Math.min(50, pathsArray.length)));
    }

    const catalogItems = await Promise.all(
      chunks.map((chunk) =>
        config
          .sonarV3Request(`catalog/by-paths?maxChildren=20`, {
            body: JSON.stringify(chunk),
            headers: { "Content-Type": "application/json" },
            method: "POST",
          })
          .then((res) => res.json())
          .then((response) => response.data),
      ),
    ).then((chunks) => chunks.flat());

    for (const catalogItem of catalogItems) {
      const resolved = resolveCatalogObject(catalogItem);
      const originalKey = originalKeyRef.get(JSON.stringify(resolved.path));
      if (originalKey) {
        results.set(originalKey, Ok(resolved));
      }
    }

    for (const path of paths) {
      if (!results.has(path)) {
        results.set(path, Err(unableToRetrieveProblem()));
      }
    }

    return results;
  });

  const resolveCatalogObject = (resource: any): CatalogObject => {
    switch (resource.entityType) {
      case "EnterpriseFolder":
      case "folder":
        return Folder.fromResource(resource, retrieve);
      case "EnterpriseDataset":
      case "dataset": {
        try {
          JSON.parse(resource.id);
          return VersionedDataset.fromResource(resource);
        } catch (e) {
          // continue
        }
        return Dataset.fromResource(resource);
      }
      case "EnterpriseSource":
      case "source": {
        return Source.fromResource(resource, retrieve);
      }

      case "home":
        return Home.fromResource(resource, retrieve);
      case "EnterpriseSpace":
      case "space":
        return Space.fromResource(resource, retrieve);
      case "file":
        return File.fromResource(resource, retrieve);
      case "function":
        return new CatalogFunction({
          createdAt: new Date(resource.createdAt),
          id: resource.id,
          isScalar: resource.isScalar,
          lastModified: new Date(resource.lastModified),
          path: resource.path,
          returnType: resource.returnType,
          tag: resource.tag,
        });
      default:
        throw new Error("Unexpected " + resource.entityType);
    }
  };

  return {
    list: () => {
      return {
        async *data() {
          yield* await config
            .sonarV3Request("catalog")
            .then((res) => res.json())
            .then((response) => {
              return response.data.map(
                (entity: unknown) =>
                  new CatalogReference(
                    catalogReferenceEntityToProperties(entity),
                    retrieve,
                  ),
              ) as CatalogReference[];
            });
        },
      };
    },
    retrieve: retrieve,
    retrieveByPath: async (path: string[]) => {
      if (!path.length) {
        return Err(emptyPathError);
      }
      return retrieveByPath(path);
    },
  };
};
