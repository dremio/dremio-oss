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

import { ContainerType } from "../../types/ContainerType";
import type { AutocompleteApi } from "../autocompleteApi";
import { ContainerFetcher } from "../containerFetcher";

describe("containerFetcher", () => {
  describe("getContainers", () => {
    it("empty cache", async () => {
      const containers = [
        {
          name: "table1",
          type: ContainerType.VIRTUAL,
          parentPath: ["space1"],
        },
      ];
      const mockAutocompleteApi: AutocompleteApi = {
        getContainers: () =>
          Promise.resolve({
            containers,
            isExhaustive: true,
          }),
        getColumns: () => Promise.resolve({} as any),
        getReferences: () => Promise.resolve({} as any),
      };
      const containerFetcher = new ContainerFetcher(mockAutocompleteApi);
      expect(await containerFetcher.getContainers(["space1"], "", [])).toEqual(
        containers
      );
    });

    it("cache hit", async () => {
      const containers = [
        {
          name: "table1",
          type: ContainerType.VIRTUAL,
          parentPath: ["space1"],
        },
      ];
      const mockAutocompleteApi: AutocompleteApi = {
        getContainers: () =>
          Promise.resolve({
            containers,
            isExhaustive: true,
          }),
        getColumns: () => Promise.resolve({} as any),
        getReferences: () => Promise.resolve({} as any),
      };
      const containerFetcher = new ContainerFetcher(mockAutocompleteApi);
      await containerFetcher.getContainers(["space1"], "", []);
      // Change API to return no data to ensure it's not called again
      mockAutocompleteApi.getContainers = () =>
        Promise.resolve({
          containers: [],
          isExhaustive: true,
        });
      expect(await containerFetcher.getContainers(["SPACE1"], "", [])).toEqual(
        containers
      );
    });

    it("cache miss", async () => {
      const containers = [
        {
          name: "table1",
          type: ContainerType.VIRTUAL,
          parentPath: ["space1"],
        },
      ];
      const mockAutocompleteApi: AutocompleteApi = {
        getContainers: () =>
          Promise.resolve({
            containers,
            isExhaustive: true,
          }),
        getColumns: () => Promise.resolve({} as any),
        getReferences: () => Promise.resolve({} as any),
      };
      const containerFetcher = new ContainerFetcher(mockAutocompleteApi);
      await containerFetcher.getContainers(["space1"], "", []);
      // Change API to return no data to ensure it's called again
      mockAutocompleteApi.getContainers = () =>
        Promise.resolve({
          containers: [],
          isExhaustive: true,
        });
      expect(await containerFetcher.getContainers(["space2"], "", [])).toEqual(
        []
      );
    });

    it("cache hit with exhaustive prefix results", async () => {
      const containers = [
        {
          name: "table1",
          type: ContainerType.VIRTUAL,
          parentPath: ["space1"],
        },
      ];
      const mockAutocompleteApi: AutocompleteApi = {
        getContainers: () =>
          Promise.resolve({
            containers,
            isExhaustive: true,
          }),
        getColumns: () => Promise.resolve({} as any),
        getReferences: () => Promise.resolve({} as any),
      };
      const containerFetcher = new ContainerFetcher(mockAutocompleteApi);
      await containerFetcher.getContainers(["space1"], "ta", []);
      // Change API to return no data to ensure it's not called again
      mockAutocompleteApi.getContainers = () =>
        Promise.resolve({
          containers: [],
          isExhaustive: true,
        });
      expect(
        await containerFetcher.getContainers(["space1"], "TABLE", [])
      ).toEqual(containers);
    });

    it("cache miss with non-exhaustive prefix results", async () => {
      const containers = [
        {
          name: "table1",
          type: ContainerType.VIRTUAL,
          parentPath: ["space1"],
        },
      ];
      const mockAutocompleteApi: AutocompleteApi = {
        getContainers: () =>
          Promise.resolve({
            containers,
            isExhaustive: false,
          }),
        getColumns: () => Promise.resolve({} as any),
        getReferences: () => Promise.resolve({} as any),
      };
      const containerFetcher = new ContainerFetcher(mockAutocompleteApi);
      await containerFetcher.getContainers(["space1"], "ta", []);
      const newContainers = [
        {
          name: "table123",
          type: ContainerType.VIRTUAL,
          parentPath: ["space1"],
        },
      ];
      mockAutocompleteApi.getContainers = () =>
        Promise.resolve({
          containers: newContainers,
          isExhaustive: true,
        });
      expect(
        await containerFetcher.getContainers(["space1"], "table12", [])
      ).toEqual(newContainers);
    });

    it("cache hit, different type", async () => {
      const containers = [
        {
          name: "table1",
          type: ContainerType.VIRTUAL,
          parentPath: ["space1"],
        },
        {
          name: "func1",
          type: ContainerType.FUNCTION,
          parentPath: ["space1"],
        },
      ];
      const mockAutocompleteApi: AutocompleteApi = {
        getContainers: () =>
          Promise.resolve({
            containers,
            isExhaustive: true,
          }),
        getColumns: () => Promise.resolve({} as any),
        getReferences: () => Promise.resolve({} as any),
      };
      const containerFetcher = new ContainerFetcher(mockAutocompleteApi);
      expect(
        await containerFetcher.getContainers(
          ["space1"],
          "",
          [],
          new Set([ContainerType.VIRTUAL])
        )
      ).toEqual([containers[0]]);
      // Change API to return no data to ensure it's not called again
      mockAutocompleteApi.getContainers = () =>
        Promise.resolve({
          containers: [],
          isExhaustive: true,
        });
      expect(
        await containerFetcher.getContainers(
          ["space1"],
          "",
          [],
          new Set([ContainerType.FUNCTION])
        )
      ).toEqual([containers[1]]);
    });

    it("cache miss different query context", async () => {
      const containers = [
        {
          name: "table1",
          type: ContainerType.VIRTUAL,
          parentPath: ["space1", "folder1"],
        },
      ];
      const mockAutocompleteApi: AutocompleteApi = {
        getContainers: () =>
          Promise.resolve({
            containers,
            isExhaustive: true,
          }),
        getColumns: () => Promise.resolve({} as any),
        getReferences: () => Promise.resolve({} as any),
      };
      const containerFetcher = new ContainerFetcher(mockAutocompleteApi);
      await containerFetcher.getContainers(["folder1"], "", ["space1"]);
      const newContainers = [
        {
          name: "table2",
          type: ContainerType.VIRTUAL,
          parentPath: ["space2", "folder1"],
        },
      ];
      mockAutocompleteApi.getContainers = () =>
        Promise.resolve({
          containers: newContainers,
          isExhaustive: true,
        });
      expect(
        await containerFetcher.getContainers(["folder1"], "", ["space2"])
      ).toEqual(newContainers);
    });
  });
});
