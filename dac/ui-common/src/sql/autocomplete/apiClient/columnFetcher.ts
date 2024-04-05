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

import type { AutocompleteApi, CatalogColumn } from "./autocompleteApi";

type CacheKey = {
  tablePath: string[];
  queryContext: string[];
};

type CacheValue = {
  columns: CatalogColumn[];
};

export type IColumnFetcher = {
  getColumns(
    tablePath: string[],
    queryContext: string[],
  ): Promise<CatalogColumn[]>;
};

/** Read-through columns cache */
export class ColumnFetcher implements IColumnFetcher {
  private autocompleteApi: AutocompleteApi;
  private columnCache: [CacheKey, CacheValue][] = [];

  constructor(autocompleteApi: AutocompleteApi) {
    this.autocompleteApi = autocompleteApi;
  }

  async getColumns(
    tablePath: string[],
    queryContext: string[],
  ): Promise<CatalogColumn[]> {
    const cacheKey: CacheKey = {
      tablePath: tablePath.map((pathPart: string) =>
        pathPart.toLocaleLowerCase(),
      ),
      queryContext,
    };
    const cacheEntry = this.columnCache.find((cacheEntry) =>
      isEqual(cacheEntry[0], cacheKey),
    );
    if (cacheEntry) {
      return cacheEntry[1].columns;
    }
    const { columns } = await this.autocompleteApi.getColumns(
      [tablePath],
      queryContext,
    );
    this.columnCache.push([cacheKey, { columns }]);
    return columns;
  }
}
