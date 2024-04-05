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

import type { GetSuggestionsResponse } from "../../endpoints/GetSuggestionsResponse.type";
import { SimpleDataType } from "../../../../sonar/catalog/SimpleDataType.type";
import { AutocompleteApiClient } from "../autocompleteApi";
import { ContainerType } from "../../types/ContainerType";
import { TableVersionType } from "../../../../arctic/datasets/TableVersionType.type";

describe("AutocompleteApiClient", () => {
  describe("getColumns", () => {
    it("fetches columns", async () => {
      const mockGetSuggestions = (): Promise<GetSuggestionsResponse> =>
        Promise.resolve({
          suggestionsType: "column",
          suggestions: [
            {
              name: ["space1", "tbl1", "colA"],
              type: "TEXT",
            },
            {
              name: ["space1", "tbl1", "colB"],
              type: "BOOLEAN",
            },
            {
              name: ["space1", "tbl1", "colC"],
              type: "INTEGER",
            },
            {
              name: ["space2", "tbl2", "col1"],
              type: "FLOAT",
            },
            {
              name: ["space2", "tbl2", "col2"],
              type: "NEW_UNKNOWN_TYPE",
            },
          ],
          count: 5,
          maxCount: 5,
        });
      const tables = [
        ["space1", "tbl1"],
        ["space2", "tbl2"],
      ];
      const queryContext: string[] = [];
      const autocompleteApi = new AutocompleteApiClient(mockGetSuggestions);
      expect(await autocompleteApi.getColumns(tables, queryContext)).toEqual({
        columns: [
          {
            name: "colA",
            parentPath: ["space1", "tbl1"],
            type: SimpleDataType.TEXT,
          },
          {
            name: "colB",
            parentPath: ["space1", "tbl1"],
            type: SimpleDataType.BOOLEAN,
          },
          {
            name: "colC",
            parentPath: ["space1", "tbl1"],
            type: SimpleDataType.INTEGER,
          },
          {
            name: "col1",
            parentPath: ["space2", "tbl2"],
            type: SimpleDataType.FLOAT,
          },
          {
            name: "col2",
            parentPath: ["space2", "tbl2"],
            type: SimpleDataType.OTHER,
          },
        ],
      });
    });
  });

  describe("getContainers", () => {
    const mockGetSuggestions = (): Promise<GetSuggestionsResponse> =>
      Promise.resolve({
        suggestionsType: "container",
        suggestions: [
          {
            name: ["space1"],
            type: "space",
          },
          {
            name: ["@dremio"],
            type: "home",
          },
          {
            name: ["source1"],
            type: "source",
          },
          {
            name: ["function1"],
            type: "function",
          },
          {
            name: ["space1", "table1"],
            type: "virtual",
          },
          {
            name: ["space1", "table2"],
            type: "direct",
          },
          {
            name: ["space1", "folder1"],
            type: "folder",
          },
          {
            name: ["space1", "function2"],
            type: "function",
          },
          {
            name: ["space1", "weird_object"],
            type: "new_unknown_type",
          },
        ],
        count: 9,
        maxCount: 9,
      });
    const path: string[] = [];
    const queryContext: string[] = ["space1"];
    const prefix = "";
    const autocompleteApi = new AutocompleteApiClient(mockGetSuggestions);

    it("fetches top containers", async () => {
      expect(
        await autocompleteApi.getContainers(path, prefix, queryContext),
      ).toEqual({
        containers: [
          {
            name: "space1",
            parentPath: [],
            type: ContainerType.SPACE,
          },
          {
            name: "@dremio",
            parentPath: [],
            type: ContainerType.HOME,
          },
          {
            name: "source1",
            parentPath: [],
            type: ContainerType.SOURCE,
          },
          {
            name: "function1",
            parentPath: [],
            type: ContainerType.FUNCTION,
          },
          {
            name: "table1",
            parentPath: ["space1"],
            type: ContainerType.VIRTUAL,
          },
          {
            name: "table2",
            parentPath: ["space1"],
            type: ContainerType.DIRECT,
          },
          {
            name: "folder1",
            parentPath: ["space1"],
            type: ContainerType.FOLDER,
          },
          {
            name: "function2",
            parentPath: ["space1"],
            type: ContainerType.FUNCTION,
          },
        ],
        isExhaustive: true,
      });
    });
  });
  describe("getReferences", () => {
    const mockGetSuggestions = (): Promise<GetSuggestionsResponse> =>
      Promise.resolve({
        suggestionsType: "reference",
        suggestions: [
          {
            name: ["branch1"],
            type: "branch",
          },
          {
            name: ["tag1"],
            type: "tag",
          },
          {
            name: ["weird_object"],
            type: "new_unknown_type",
          },
        ],
        count: 3,
        maxCount: 100,
      });
    const sourceName = "source1";
    const queryContext: string[] = [];
    const prefix = "";
    const autocompleteApi = new AutocompleteApiClient(mockGetSuggestions);

    it("fetches references", async () => {
      expect(
        await autocompleteApi.getReferences(prefix, sourceName, queryContext),
      ).toEqual({
        references: [
          {
            name: "branch1",
            type: TableVersionType.BRANCH,
          },
          {
            name: "tag1",
            type: TableVersionType.TAG,
          },
        ],
        isExhaustive: false,
      });
    });
  });
});
