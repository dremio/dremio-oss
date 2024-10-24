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

import { dremio } from "#oss/dremio";
import { queryClient } from "#oss/queryClient";
import { queryOptions } from "@tanstack/react-query";
import type {
  CatalogObject,
  CatalogObjectMethods,
  CatalogReference,
} from "@dremio/dremio-js/interfaces";

export const catalogRootQuery = () =>
  queryOptions({
    queryKey: ["catalog", "root"],
    queryFn: () => Array.fromAsync(dremio.catalog.list().data()),
    retry: false,
    staleTime: 60_000,
  });

export const catalogById = () => (id: string) =>
  queryOptions({
    queryKey: ["catalog", id] as const,
    queryFn: async () => {
      const catalogObject = await dremio.catalog
        .retrieve(id)
        .then((result) => result.unwrap());

      queryClient.setQueryData(["catalog", catalogObject.path], catalogObject);

      return catalogObject;
    },
    retry: false,
    staleTime: 300_000,
  });

export const catalogByPath = () => (path: string[]) =>
  queryOptions({
    queryKey: ["catalog", path] as const,
    queryFn: async () => {
      const catalogObject = await dremio.catalog
        .retrieveByPath(path)
        .then((result) => result.unwrap());

      queryClient.setQueryData(["catalog", catalogObject.id], catalogObject);

      return catalogObject;
    },
    retry: false,
    staleTime: 300_000,
  });

export const catalogReferenceChildren = (catalogReference: CatalogReference) =>
  queryOptions({
    // eslint-disable-next-line @tanstack/query/exhaustive-deps
    queryKey: ["catalog_reference_children", catalogReference.id],
    queryFn: async () => {
      if ("children" in catalogReference) {
        return Array.fromAsync(catalogReference.children().data());
      }
      throw new Error(
        "Unsupported catalogReference type: " + catalogReference.type,
      );
    },
    staleTime: 300_000,
  });

export const catalogObjectChildren = (
  catalogObject: CatalogObject &
    Required<Pick<CatalogObjectMethods, "children">>,
) =>
  queryOptions({
    queryKey: ["catalog_children", catalogObject.id],
    queryFn: async () => Array.fromAsync(catalogObject.children().data()),
  });
