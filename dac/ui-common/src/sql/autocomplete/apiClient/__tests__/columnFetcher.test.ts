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

import { SimpleDataType } from "../../../../sonar/catalog/SimpleDataType.type";
import type { AutocompleteApi } from "../autocompleteApi";
import { ColumnFetcher } from "../columnFetcher";

describe("containerFetcher", () => {
  describe("getContainers", () => {
    it("empty cache", async () => {
      const columns = [
        {
          name: "col1",
          type: SimpleDataType.TEXT,
          parentPath: ["space1", "table1"],
        },
      ];
      const mockAutocompleteApi: AutocompleteApi = {
        getColumns: () =>
          Promise.resolve({
            columns,
          }),
        getContainers: () => Promise.resolve({} as any),
        getReferences: () => Promise.resolve({} as any),
      };
      const containerFetcher = new ColumnFetcher(mockAutocompleteApi);
      expect(
        await containerFetcher.getColumns(["space1", "table1"], [])
      ).toEqual(columns);
    });

    it("cache hit", async () => {
      const columns = [
        {
          name: "col1",
          type: SimpleDataType.TEXT,
          parentPath: ["space1", "table1"],
        },
      ];
      const mockAutocompleteApi: AutocompleteApi = {
        getColumns: () =>
          Promise.resolve({
            columns,
          }),
        getContainers: () => Promise.resolve({} as any),
        getReferences: () => Promise.resolve({} as any),
      };
      const containerFetcher = new ColumnFetcher(mockAutocompleteApi);
      expect(
        await containerFetcher.getColumns(["space1", "table1"], [])
      ).toEqual(columns);
      // Change API to return no data to ensure it's not called again
      mockAutocompleteApi.getColumns = () =>
        Promise.resolve({
          columns: [],
        });
      expect(
        await containerFetcher.getColumns(["SPACE1", "TABLE1"], [])
      ).toEqual(columns);
    });

    it("cache miss different query context", async () => {
      const columns = [
        {
          name: "col1",
          type: SimpleDataType.TEXT,
          parentPath: ["space1", "table1"],
        },
      ];
      const mockAutocompleteApi: AutocompleteApi = {
        getColumns: () =>
          Promise.resolve({
            columns,
          }),
        getContainers: () => Promise.resolve({} as any),
        getReferences: () => Promise.resolve({} as any),
      };
      const containerFetcher = new ColumnFetcher(mockAutocompleteApi);
      expect(await containerFetcher.getColumns(["table1"], ["space1"])).toEqual(
        columns
      );
      // Change API to return no data to ensure it's called again
      mockAutocompleteApi.getColumns = () =>
        Promise.resolve({
          columns: [],
        });
      expect(await containerFetcher.getColumns(["table1"], ["space2"])).toEqual(
        []
      );
    });
  });
});
