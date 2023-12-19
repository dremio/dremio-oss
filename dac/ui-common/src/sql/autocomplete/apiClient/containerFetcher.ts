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

import { isEqual } from "lodash";

import type { AutocompleteApi, CatalogContainer } from "./autocompleteApi";
import type { ContainerType } from "../types/ContainerType";

type CacheKey = {
  tablePath: string[];
  queryContext: string[];
};

type CacheValueNode = {
  children: { [char: string]: CacheValueNode };
  value?: {
    containers: CatalogContainer[];
    isExhaustive: boolean;
  };
};

class CacheValue {
  private readonly root: CacheValueNode;

  constructor() {
    this.root = {
      children: {},
    };
  }

  insert(
    prefix: string,
    containers: CatalogContainer[],
    isExhaustive: boolean
  ): void {
    const lcPrefix = prefix.toLocaleLowerCase();
    let current: CacheValueNode = this.root;
    for (const char of lcPrefix) {
      if (!current.children[char]) {
        current.children[char] = {
          children: {},
        };
      }
      current = current.children[char];
    }
    current.value = { containers, isExhaustive };
  }

  get(prefix: string): CatalogContainer[] | undefined {
    const lcPrefix = prefix.toLocaleLowerCase();
    let current = this.root;
    for (const char of lcPrefix) {
      if (current.value && current.value.isExhaustive) {
        return current.value.containers.filter((container) =>
          container.name.toLocaleLowerCase().startsWith(lcPrefix)
        );
      }
      if (!current.children[char]) {
        return undefined;
      }
      current = current.children[char];
    }
    return current.value ? current.value.containers : undefined;
  }
}

export type IContainerFetcher = {
  getContainers(
    tablePath: string[],
    prefix: string,
    queryContext: string[],
    type?: Set<ContainerType>
  ): Promise<CatalogContainer[]>;
};

/** Read-through container cache */
export class ContainerFetcher implements IContainerFetcher {
  private autocompleteApi: AutocompleteApi;
  private containerCache: [CacheKey, CacheValue][] = [];

  constructor(autocompleteApi: AutocompleteApi) {
    this.autocompleteApi = autocompleteApi;
  }

  async getContainers(
    tablePath: string[],
    prefix: string,
    queryContext: string[],
    type?: Set<ContainerType>
  ): Promise<CatalogContainer[]> {
    const cacheKey: CacheKey = {
      tablePath: tablePath.map((pathPart: string) =>
        pathPart.toLocaleLowerCase()
      ),
      queryContext,
    };
    const cacheEntry = this.containerCache.find((cacheEntry) =>
      isEqual(cacheEntry[0], cacheKey)
    );
    const cachedContainers = cacheEntry && cacheEntry[1].get(prefix);
    if (cachedContainers) {
      return this.filterByType(type, cachedContainers);
    }
    const { containers, isExhaustive } =
      await this.autocompleteApi.getContainers(tablePath, prefix, queryContext);
    let cacheValue = cacheEntry?.[1];
    if (!cacheValue) {
      cacheValue = new CacheValue();
      this.containerCache.push([cacheKey, cacheValue]);
    }
    cacheValue.insert(prefix, containers, isExhaustive);
    return this.filterByType(type, containers);
  }

  private filterByType(
    type: Set<ContainerType> | undefined,
    containers: CatalogContainer[]
  ): CatalogContainer[] {
    return containers.filter((container) => !type || type.has(container.type));
  }
}
