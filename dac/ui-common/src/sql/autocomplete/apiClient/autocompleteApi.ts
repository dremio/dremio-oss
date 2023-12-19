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

import { TableVersionType } from "../../../arctic/datasets/TableVersionType.type";
import { SimpleDataType } from "../../../sonar/catalog/SimpleDataType.type";
import type { GetSuggestionsAPI } from "../endpoints/GetSuggestionsAPI";
import type { GetSuggestionsParams } from "../endpoints/GetSuggestionsParams.type";
import type { GetSuggestionsResponse } from "../endpoints/GetSuggestionsResponse.type";
import { ContainerType } from "../types/ContainerType";

export type CatalogContainer = {
  name: string;
  parentPath: string[];
  type: ContainerType;
};

export type CatalogColumn = {
  name: string;
  parentPath: string[];
  type: SimpleDataType;
};

export type CatalogReference = {
  name: string;
  type: TableVersionType;
};

export type GetContainersResult = {
  containers: CatalogContainer[];
  isExhaustive: boolean;
};

export type GetColumnsResult = {
  columns: CatalogColumn[];
};

export type GetReferencesResult = {
  references: CatalogReference[];
  isExhaustive: boolean;
};

const containerTypeMap: { [containerType: string]: ContainerType } = {
  source: ContainerType.SOURCE,
  home: ContainerType.HOME,
  space: ContainerType.SPACE,
  folder: ContainerType.FOLDER,
  promoted: ContainerType.PROMOTED,
  direct: ContainerType.DIRECT,
  virtual: ContainerType.VIRTUAL,
  function: ContainerType.FUNCTION,
};

const referenceTypeMap: { [referenceType: string]: TableVersionType } = {
  branch: TableVersionType.BRANCH,
  tag: TableVersionType.TAG,
};

export type AutocompleteApi = {
  getContainers(
    path: string[],
    prefix: string,
    queryContext: string[]
  ): Promise<GetContainersResult>;

  getColumns(
    tablePaths: string[][],
    queryContext: string[]
  ): Promise<GetColumnsResult>;

  getReferences(
    prefix: string,
    sourceName: string,
    queryContext: string[],
    type?: Set<TableVersionType>
  ): Promise<GetReferencesResult>;
};

export class AutocompleteApiClient implements AutocompleteApi {
  private getSuggestions: GetSuggestionsAPI;

  constructor(getSuggestions: GetSuggestionsAPI) {
    this.getSuggestions = getSuggestions;
  }

  async getContainers(
    path: string[],
    prefix: string,
    queryContext: string[]
  ): Promise<GetContainersResult> {
    const payload: GetSuggestionsParams = {
      type: "container",
      catalogEntityKeys: [path],
      queryContext,
      prefix,
    };
    const suggestionsResponse: GetSuggestionsResponse =
      await this.getSuggestions(payload);
    if (suggestionsResponse.suggestionsType !== "container") {
      throw new Error("Unexpected suggestions entityType");
    }
    const containers = suggestionsResponse.suggestions
      .map((container) => ({
        name: container.name.slice(-1)[0],
        parentPath: container.name.slice(0, -1),
        type: containerTypeMap[container.type],
      }))
      .filter((container) => container.type != null);
    const isExhaustive =
      suggestionsResponse.count === suggestionsResponse.maxCount;
    return { containers, isExhaustive };
  }

  async getColumns(
    tablePaths: string[][],
    queryContext: string[]
  ): Promise<GetColumnsResult> {
    const payload: GetSuggestionsParams = {
      type: "column",
      catalogEntityKeys: tablePaths,
      queryContext,
    };
    const suggestionsResponse = await this.getSuggestions(payload);
    if (suggestionsResponse.suggestionsType !== "column") {
      throw new Error("Unexpected suggestions entityType");
    }
    const columns = suggestionsResponse.suggestions.map((column) => ({
      name: column.name.slice(-1)[0],
      parentPath: column.name.slice(0, -1),
      type:
        SimpleDataType[column.type as keyof typeof SimpleDataType] ??
        SimpleDataType.OTHER,
    }));
    return { columns };
  }

  async getReferences(
    prefix: string,
    sourceName: string,
    queryContext: string[]
  ): Promise<GetReferencesResult> {
    const payload: GetSuggestionsParams = {
      type: "reference",
      catalogEntityKeys: [[sourceName]],
      queryContext,
      prefix,
    };
    const suggestionsResponse = await this.getSuggestions(payload);
    if (suggestionsResponse.suggestionsType !== "reference") {
      throw new Error("Unexpected suggestions entityType");
    }
    const references = suggestionsResponse.suggestions
      .map((reference) => ({
        name: reference.name[0],
        type: referenceTypeMap[reference.type],
      }))
      .filter((reference) => reference.type != null);
    const isExhaustive =
      suggestionsResponse.count === suggestionsResponse.maxCount;
    return { references, isExhaustive };
  }
}
