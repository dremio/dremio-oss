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
import Immutable from "immutable";
// @ts-ignore
import { render } from "rtlUtils";
import { expect } from "chai";

import {
  getIsStarred,
  constructSummaryFullPath,
  getEntityTypeFromNode,
  starredResourceTreeNodeDecorator,
  resourceTreeNodeDecorator,
} from "./resourceTreeUtils";

describe("resourceTreeUtils", () => {
  describe("getIsStarred", () => {
    const starredItems = [{ id: "49600127-5396-46cf-9a8f-6e3653fdb4a3" }];
    it("correctly identify node is meant to be starred", () => {
      const id = "49600127-5396-46cf-9a8f-6e3653fdb4a3";
      const result = getIsStarred(starredItems, id);
      expect(result).to.equal(true);
    });
    it("correctly identify node is meant to be unstarred", () => {
      const id = "f1285325-24d6-4021-99a2-98648db7c672";
      const result = getIsStarred(starredItems, id);
      expect(result).to.equal(false);
    });
  });
  describe("constructSummaryFullPath", () => {
    it("parse a normal path properly", () => {
      const pathParts = "MySQL.HR.countries";
      const result = constructSummaryFullPath(pathParts);
      expect(result).to.equal("MySQL/HR/countries");
    });
    it("parse a quoted path properly", () => {
      const pathParts =
        'Samples."samples.dremio.com"."SF weather 2018-2019.csv"';
      const result = constructSummaryFullPath(pathParts);
      expect(result).to.equal(
        "Samples/samples.dremio.com/SF weather 2018-2019.csv"
      );
    });
  });
  describe("getEntityTypeFromNode", () => {
    it("Correctly idetifies the entity type for container node", () => {
      const node1 = Immutable.Map({ type: "HOME" });
      const result1 = getEntityTypeFromNode(node1);
      expect(result1).to.equal("container");

      const node2 = Immutable.Map({ type: "SOURCE" });
      const result2 = getEntityTypeFromNode(node2);
      expect(result2).to.equal("container");

      const node3 = Immutable.Map({ type: "FOLDER" });
      const result3 = getEntityTypeFromNode(node3);
      expect(result3).to.equal("container");

      const node4 = Immutable.Map({ type: "SPACE" });
      const result4 = getEntityTypeFromNode(node4);
      expect(result4).to.equal("container");

      const result5 = getEntityTypeFromNode(undefined); //should default to container
      expect(result5).to.equal("container");
    });
    it("Correctly idetifies the entity type for dataset node", () => {
      const node1 = Immutable.Map({ type: "PHYSICAL_DATASET" });
      const result1 = getEntityTypeFromNode(node1);
      expect(result1).to.equal("dataset");

      const node2 = Immutable.Map({ type: "PHYSICAL_DATASET_HOME_FILE" });
      const result2 = getEntityTypeFromNode(node2);
      expect(result2).to.equal("dataset");

      const node3 = Immutable.Map({ type: "PHYSICAL_DATASET_SOURCE_FILE" });
      const result3 = getEntityTypeFromNode(node3);
      expect(result3).to.equal("dataset");

      const node4 = Immutable.Map({ type: "VIRTUAL_DATASET" });
      const result4 = getEntityTypeFromNode(node4);
      expect(result4).to.equal("dataset");

      const node5 = Immutable.Map({ type: "PHYSICAL_DATASET_SOURCE_FOLDER" });
      const result5 = getEntityTypeFromNode(node5);
      expect(result5).to.equal("dataset");

      const node6 = Immutable.Map({ type: "INVALID_DATASET_TYPE" });
      const result6 = getEntityTypeFromNode(node6);
      expect(result6).to.equal("dataset");

      const node7 = Immutable.Map({ type: "PHYSICAL_DATASET_HOME_FOLDER" });
      const result7 = getEntityTypeFromNode(node7);
      expect(result7).to.equal("dataset");

      const node8 = Immutable.Map({ type: "OTHERS" });
      const result8 = getEntityTypeFromNode(node8);
      expect(result8).to.equal("dataset");
    });
  });
  describe("starredResourceTreeNodeDecorator", () => {
    it("returns starred top level resource tree data in correct format", () => {
      const state = Immutable.Map({
        starResourceList: Immutable.List(),
      });
      const action: any = {
        meta: {
          viewId: "StarredItems",
        },
        payload: {
          entities: [
            {
              fullPath: ["space1"],
              id: "459d564b-ecce-4c44-8ca1-c412b6fb6025",
              name: "space1",
              starredAt: "2022-04-08T00:02:40.481Z",
              type: "SPACE",
            },
          ],
        },
      };
      const payloadKey = "entities";

      const expectedResult = {
        starResourceList: [
          {
            fullPath: ["space1"],
            id: "459d564b-ecce-4c44-8ca1-c412b6fb6025",
            name: "space1",
            starredAt: "2022-04-08T00:02:40.481Z",
            type: "SPACE",
            baseNode: true,
            viewPath: ["space1"],
            branchId: 0,
          },
        ],
      };
      const result = starredResourceTreeNodeDecorator(
        state,
        // @ts-ignore
        action,
        payloadKey
      );

      expect(result.toJS()).to.deep.equal(expectedResult);
    });
    it("returns starred resource tree data in correct format with stars from same resource tree branch", () => {
      const state: any = Immutable.Map({
        starResourceList: Immutable.fromJS([
          {
            viewPath: ["space 1"],
            name: "space 1",
            starredAt: "2022-04-12T20:00:54.264Z",
            baseNode: true,
            fullPath: ["space 1"],
            type: "SPACE",
            id: "b067e1f5-bba4-4fe3-9838-8f39c03cb55b",
            branchId: 0,
            resources: [
              {
                type: "FOLDER",
                name: "folder 1",
                fullPath: ["space 1", "folder 1"],
                url: "/resourcetree/%22space%201%22.%22folder%201%22",
                id: "299d90e5-ad87-47d0-a17e-ddf3536557a8",
                viewPath: ["space 1", "folder 1"],
                branchId: 0,
                resources: [
                  {
                    type: "VIRTUAL_DATASET",
                    name: "dataset 1",
                    fullPath: ["space 1", "folder 1", "dataset 1"],
                    id: "844fe207-5538-40d9-8342-4d07071f45da",
                    viewPath: ["space 1", "folder 1", "dataset 1"],
                    branchId: 0,
                  },
                ],
              },
            ],
          },
        ]),
      });
      const action: any = {
        meta: {
          viewId: "StarredItems",
          fullPath: "space 1/folder 1/dataset 1",
          isSummaryDatasetResponse: true,
          nodeExpanded: true,
          currNode: Immutable.Map({
            type: "VIRTUAL_DATASET",
            name: "dataset 1",
            fullPath: Immutable.List(["space 1", "folder 1", "dataset 1"]),
            id: "844fe207-5538-40d9-8342-4d07071f45da",
            viewPath: ["space 1", "folder 1", "dataset 1"],
            branchId: 0,
          }),
        },
        payload: Immutable.fromJS({
          entities: {
            summaryDataset: {
              "space 1,folder 1,dataset 1": {
                datasetVersion: "0002187242826607",
                permissions: {},
                references: {},
                jobCount: 0,
                datasetType: "VIRTUAL_DATASET",
                fullPath: ["space 1", "folder 1", "dataset 1"],
                descendants: 0,
                fields: [
                  {
                    name: "A",
                    type: "TEXT",
                    isPartitioned: false,
                    isSorted: false,
                  },
                ],
                links: {},
                apiLinks: {},
              },
            },
          },
          result: ["space 1", "folder 1", "dataset 1"],
        }),
        type: "LOAD_STARRED_RESOURCE_LIST_SUCCESS",
      };
      const payloadKey = "resources";

      const result = starredResourceTreeNodeDecorator(
        state,
        // @ts-ignore
        action,
        payloadKey
      );
      const expectedResult = {
        starResourceList: [
          {
            resources: [
              {
                type: "FOLDER",
                name: "folder 1",
                fullPath: ["space 1", "folder 1"],
                url: "/resourcetree/%22space%201%22.%22folder%201%22",
                id: "299d90e5-ad87-47d0-a17e-ddf3536557a8",
                viewPath: ["space 1", "folder 1"],
                branchId: 0,
                resources: [
                  {
                    type: "VIRTUAL_DATASET",
                    name: "dataset 1",
                    fullPath: ["space 1", "folder 1", "dataset 1"],
                    id: "844fe207-5538-40d9-8342-4d07071f45da",
                    viewPath: ["space 1", "folder 1", "dataset 1"],
                    branchId: 0,
                    resources: [
                      {
                        name: "A",
                        type: "TEXT",
                        isPartitioned: false,
                        isSorted: false,
                        fullPath: ["space 1", "folder 1", "dataset 1"],
                        isColumnItem: true,
                        viewPath: ["space 1", "folder 1", "dataset 1", "A"],
                        branchId: 0,
                      },
                    ],
                  },
                ],
              },
            ],
            viewPath: ["space 1"],
            name: "space 1",
            starredAt: "2022-04-12T20:00:54.264Z",
            baseNode: true,
            fullPath: ["space 1"],
            type: "SPACE",
            id: "b067e1f5-bba4-4fe3-9838-8f39c03cb55b",
            branchId: 0,
          },
        ],
      };

      expect(result.toJS()).to.deep.equal(expectedResult);
    });
  });
  describe("resourceTreeNodeDecorator", () => {
    it("returns top level resource tree data in correct format", () => {
      const action: any = {
        meta: {
          viewId: "ResourceTree",
          path: "",
        },
        payload: {
          resources: [
            {
              type: "SPACE",
              name: "space 1",
              fullPath: ["space 1"],
              url: "/resourcetree/%22space%201%22",
              id: "ab627559-4677-4814-9abd-1de1e44c0511",
            },
            {
              type: "HOME",
              name: "@dremio",
              fullPath: ["@dremio"],
              url: "/resourcetree/%22%40dremio%22",
              id: "219fea5a-6989-482a-b311-f5cc05236eee",
            },
          ],
        },
      };
      const state = Immutable.fromJS({
        tree: [
          {
            type: "HOME",
            name: "@dremio",
            fullPath: ["@dremio"],
            url: "/resourcetree/%22%40dremio%22",
            id: "219fea5a-6989-482a-b311-f5cc05236eee",
          },
        ],
      });
      const result = resourceTreeNodeDecorator(state, action);
      const expectedResult = {
        tree: [
          {
            type: "HOME",
            name: "@dremio",
            fullPath: ["@dremio"],
            url: "/resourcetree/%22%40dremio%22",
            id: "219fea5a-6989-482a-b311-f5cc05236eee",
          },
          {
            type: "SPACE",
            name: "space 1",
            fullPath: ["space 1"],
            url: "/resourcetree/%22space%201%22",
            id: "ab627559-4677-4814-9abd-1de1e44c0511",
          },
        ],
      };

      expect(result.toJS()).to.deep.equal(expectedResult);
    });
    it("returns column level resource tree data in correct format", () => {
      const action: any = {
        meta: {
          viewId: "ResourceTree",
          fullPath: "space 1/folder 1/dataset 1",
          errorMessage: "Cannot provide more information about this dataset.",
          isSummaryDatasetResponse: true,
        },
        payload: Immutable.fromJS({
          entities: {
            summaryDataset: {
              "space 1,folder 1,dataset 1": {
                datasetVersion: "0003203456615073",
                references: {},
                jobCount: 0,
                datasetType: "VIRTUAL_DATASET",
                fullPath: ["space 1", "folder 1", "dataset 1"],
                descendants: 0,
                fields: [
                  {
                    name: "A",
                    type: "TEXT",
                    isPartitioned: false,
                    isSorted: false,
                  },
                ],
              },
            },
          },
          result: ["space 1", "folder 1", "dataset 1"],
        }),
      };
      const state = Immutable.fromJS({
        tree: [
          {
            type: "HOME",
            name: "@dremio",
            fullPath: ["@dremio"],
            url: "/resourcetree/%22%40dremio%22",
            id: "219fea5a-6989-482a-b311-f5cc05236eee",
          },
          {
            type: "SPACE",
            name: "space 1",
            fullPath: ["space 1"],
            url: "/resourcetree/%22space%201%22",
            id: "ab627559-4677-4814-9abd-1de1e44c0511",
            resources: [
              {
                type: "FOLDER",
                name: "folder 1",
                fullPath: ["space 1", "folder 1"],
                url: "/resourcetree/%22space%201%22.%22folder%201%22",
                id: "e927a9ca-5b37-483e-959a-c5e12ba6bb63",
                resources: [
                  {
                    type: "VIRTUAL_DATASET",
                    name: "dataset 1",
                    fullPath: ["space 1", "folder 1", "dataset 1"],
                    id: "2d93add2-3fbc-4ec6-a31b-c082ef4fa7c6",
                  },
                ],
              },
            ],
          },
        ],
      });
      const result = resourceTreeNodeDecorator(state, action);
      const expectedResult = {
        tree: [
          {
            type: "HOME",
            name: "@dremio",
            fullPath: ["@dremio"],
            url: "/resourcetree/%22%40dremio%22",
            id: "219fea5a-6989-482a-b311-f5cc05236eee",
          },
          {
            type: "SPACE",
            name: "space 1",
            fullPath: ["space 1"],
            url: "/resourcetree/%22space%201%22",
            id: "ab627559-4677-4814-9abd-1de1e44c0511",
            resources: [
              {
                type: "FOLDER",
                name: "folder 1",
                fullPath: ["space 1", "folder 1"],
                url: "/resourcetree/%22space%201%22.%22folder%201%22",
                id: "e927a9ca-5b37-483e-959a-c5e12ba6bb63",
                resources: [
                  {
                    type: "VIRTUAL_DATASET",
                    name: "dataset 1",
                    fullPath: ["space 1", "folder 1", "dataset 1"],
                    id: "2d93add2-3fbc-4ec6-a31b-c082ef4fa7c6",
                    resources: [
                      {
                        name: "A",
                        type: "TEXT",
                        isPartitioned: false,
                        isSorted: false,
                        fullPath: ["space 1", "folder 1", "dataset 1"],
                        isColumnItem: true,
                      },
                    ],
                  },
                ],
              },
            ],
          },
        ],
      };

      expect(result.toJS()).to.deep.equal(expectedResult);
    });
  });
  it("returns combined resource tree and taking payload's duplicate resource if there is any", () => {
    const action: any = {
      meta: {
        viewId: "ResourceTree",
        path: "\"space 1\"",
      },
      payload: {
        resources: [
          {
            type: "FOLDER",
            name: "folder 1",
            fullPath: ["space 1", "folder 1"],
            url: "/resourcetree/%22space%201%22.%22folder%201%22",
            id: "e927a9ca-5b37-483e-959a-c5e12ba6bb63",
          },
          {
            type: "FOLDER",
            name: "folder 2",
            fullPath: ["space 1", "folder 2"],
            url: "/resourcetree/%22space%201%22.%22folder%202%22",
            id: "23rbe2st-5b37-483e-959a-c5e12ba6bb63",
          },
          {
            type: "VIRTUAL_DATASET",
            name: "dataset 1",
            fullPath: ["space 1", "dataset 1"],
            url: "/resourcetree/%22space%201%22.%dataset%201%22",
            id: "2d93add2-3fbc-4ec6-a31b-c082ef4fa7c6",
          },
        ],
      },
    };
    const state = Immutable.fromJS({
      tree: [
        {
          type: "HOME",
          name: "@dremio",
          fullPath: ["@dremio"],
          url: "/resourcetree/%22%40dremio%22",
          id: "219fea5a-6989-482a-b311-f5cc05236eee",
        },
        {
          type: "SPACE",
          name: "space 1",
          fullPath: ["space 1"],
          url: "/resourcetree/%22space%201%22",
          id: "ab627559-4677-4814-9abd-1de1e44c0511",
          resources: [
            {
              type: "FOLDER",
              name: "folder 1",
              fullPath: ["space 1", "folder 1"],
              url: "/resourcetree/%22space%201%22.%22folder%201%22",
              id: "e927a9ca-5b37-483e-959a-c5e12ba6bb63",
            },
            {
              type: "FOLDER",
              name: "folder 2",
              fullPath: ["space 1", "folder 2"],
              url: "/resourcetree/%22space%201%22.%22folder%202%22",
              id: "23rre2st-5b37-483e-959a-c5e12ba6bb63",
            }
          ],
        },
      ],
    });
    const result = resourceTreeNodeDecorator(state, action);
    const expectedResult = {
      tree: [
        {
          type: "HOME",
          name: "@dremio",
          fullPath: ["@dremio"],
          url: "/resourcetree/%22%40dremio%22",
          id: "219fea5a-6989-482a-b311-f5cc05236eee",
        },
        {
          type: "SPACE",
          name: "space 1",
          fullPath: ["space 1"],
          url: "/resourcetree/%22space%201%22",
          id: "ab627559-4677-4814-9abd-1de1e44c0511",
          resources: [
            {
              type: "FOLDER",
              name: "folder 1",
              fullPath: ["space 1", "folder 1"],
              url: "/resourcetree/%22space%201%22.%22folder%201%22",
              id: "e927a9ca-5b37-483e-959a-c5e12ba6bb63",
            },
            {
              type: "FOLDER",
              name: "folder 2",
              fullPath: ["space 1", "folder 2"],
              url: "/resourcetree/%22space%201%22.%22folder%202%22",
              id: "23rbe2st-5b37-483e-959a-c5e12ba6bb63",
            },
            {
              type: "VIRTUAL_DATASET",
              name: "dataset 1",
              fullPath: ["space 1", "dataset 1"],
              url: "/resourcetree/%22space%201%22.%dataset%201%22",
              id: "2d93add2-3fbc-4ec6-a31b-c082ef4fa7c6",
            },
          ],
        },
      ],
    };

    expect(result.toJS()).to.deep.equal(expectedResult);
  });
});
