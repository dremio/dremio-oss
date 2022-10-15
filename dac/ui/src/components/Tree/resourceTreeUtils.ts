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
import { splitFullPath, constructFullPath } from "utils/pathUtils";
import {
  CONTAINER_ENTITY_TYPES,
  DATASET_ENTITY_TYPES,
} from "@app/constants/Constants";
import { TreeNode } from "./ResourceTree.types";

export const starTabNames = {
  all: "All",
  starred: "Starred",
};

export const DATA_SCRIPT_TABS = {
  Data: "Data",
  Scripts: "Scripts",
};

export const entityTypes = {
  container: "container",
  dataset: "dataset",
  columnItem: "columnItem",
};

export const STARRED_VIEW_ID = "StarredItems";
export const RESOURCE_TREE_VIEW_ID = "ResourceTree";

export const LOAD_RESOURCE_TREE = "LOAD_RESOURCE_TREE";
export const LOAD_STARRED_RESOURCE_LIST = "LOAD_STARRED_RESOURCE_LIST";

export const RESOURCE_LIST_SORT_MENU = [
  {
    category: "Name",
    dir: "desc",
    compare: sortByName("desc"),
  },
  {
    category: "Name",
    dir: "asc",
    compare: sortByName("asc"),
  },
];

function sortByName(direction: string) {
  return (a: any, b: any) => {
    if (a.get("name").toLowerCase() < b.get("name").toLowerCase()) {
      return direction === "asc" ? -1 : 1;
    } else if (a.get("name").toLowerCase() > b.get("name").toLowerCase()) {
      return direction === "asc" ? 1 : -1;
    } else {
      return 0;
    }
  };
}

function getSummaryDatasetPayload(payload: any, fullPath: string) {
  const payloadPath = [
    "entities",
    "summaryDataset",
    fullPath.split("/").join(","),
    "fields",
  ];
  const fields = payload.getIn([...payloadPath]) || [];
  const resourcesFromPayload = fields.map((item: any) => {
    // these are needed to manipulate TreeNode.js
    item = item.set("fullPath", Immutable.List(fullPath.split("/")));
    item = item.set("isColumnItem", true);
    return item;
  });
  return resourcesFromPayload;
}

export function getIsStarred(starredItems: any[], id: string) {
  for (let i = 0; i < starredItems.length; i++) {
    if (starredItems[i].id === id) {
      return true;
    }
  }
  return false;
}

export function constructSummaryFullPath(pathParts: string) {
  if (!pathParts) {
    return undefined;
  }
  let result;
  if (pathParts.indexOf('"') === -1) {
    result = pathParts.split(".").join("/");
  } else {
    for (let i = 0; i < pathParts.length; i) {
      const periodIndex = pathParts.indexOf(".", i);
      const quoteIndex = pathParts.indexOf('"', i);
      let firstBit;
      let secondBit;
      let possibleMiddleBit;
      if (
        periodIndex !== -1 &&
        (periodIndex < quoteIndex || quoteIndex === -1)
      ) {
        firstBit = pathParts.slice(0, periodIndex);
        secondBit = pathParts.slice(periodIndex + 1);
        pathParts = firstBit + "/" + secondBit;
        i = periodIndex - 1;
      } else if (quoteIndex !== -1) {
        const endIndex = pathParts.indexOf('"', quoteIndex + 1);
        firstBit = pathParts.slice(0, quoteIndex);
        possibleMiddleBit = pathParts.slice(quoteIndex + 1, endIndex);
        secondBit = pathParts.slice(endIndex + 1);
        pathParts = firstBit + possibleMiddleBit + secondBit;
        i = endIndex - 1;
      } else {
        break;
      }
    }
    result = pathParts;
  }
  return result;
}

export function getEntityTypeFromNode(node: any) {
  if (!node || CONTAINER_ENTITY_TYPES.has(node.get("type"))) {
    return entityTypes.container;
  } else if (DATASET_ENTITY_TYPES.has(node.get("type"))) {
    return entityTypes.dataset;
  } else {
    return entityTypes.columnItem;
  }
}

function buildPath(tree: any, payloadNodes: any[], currNode: any) {
  let resources = tree || Immutable.List();
  const paths = [];
  const viewPath = currNode && currNode.get("viewPath");
  const nodes = payloadNodes.map((node, index, curArr) => {
    const delimeter = index ? "." : "";
    return curArr.slice(0, index).join(".") + delimeter + node;
  });

  while (nodes.length && resources.size) {
    const index = viewPath
      ? resources.findIndex(
          (node: any) =>
            constructFullPath(node.get("viewPath"), true) === nodes[0]
        )
      : resources.findIndex(
          (node: any) =>
            constructFullPath(node.get("fullPath"), true) === nodes[0]
        );
    paths.push(index, "resources");
    resources = resources.getIn([index, "resources"]) || Immutable.List();
    nodes.shift();
  }
  return paths;
}

function starredResourceDecorator(
  resources: any,
  state: any,
  parentNode: any,
  nodeExpanded: boolean,
  builtPath: any[]
) {
  if (!nodeExpanded) {
    // top most level so it needs to be set as the base node for styling and view path for rendering future children
    const baseNodeResources = resources?.map((item: any, index: number) => {
      item = item.set("baseNode", true);
      item = item.set("viewPath", [item.get("name")]);
      item = item.set("branchId", index);
      item = item.set("starredNode", true);
      return item;
    });
    return state.set("starResourceList", baseNodeResources);
  } else {
    // children nodes need to have a new view path so the path to their location to store things is correct
    const resourcesWithViewPath = resources?.map((item: any) => {
      const parentViewPath = parentNode.get("viewPath");
      const parentBranchId = parentNode.get("branchId");
      item = item.set("viewPath", [...parentViewPath, item.get("name")]);
      item = item.set("branchId", parentBranchId);
      return item;
    });
    return state.setIn(
      ["starResourceList", ...builtPath],
      resourcesWithViewPath
    );
  }
}

export function starredResourceTreeNodeDecorator(
  state: any,
  action: {
    payload: any;
    meta: {
      viewId: string;
      path: string;
      fullPath: string;
      isExpand: boolean;
      nodeExpanded: boolean;
      currNode: any;
      isSummaryDatasetResponse: boolean;
    };
  },
  payloadKey: string
) {
  const {
    path = "",
    currNode,
    nodeExpanded,
    isSummaryDatasetResponse,
    fullPath,
  } = action.meta;
  const nodes = !nodeExpanded ? splitFullPath(path) : currNode.get("viewPath");
  const builtPath = buildPath(state.get("starResourceList"), nodes, currNode);
  const payloadResources = Immutable.fromJS(
    isSummaryDatasetResponse
      ? getSummaryDatasetPayload(action.payload, fullPath)
      : // @ts-ignore
        action.payload[payloadKey]
  );
  const resources = payloadResources?.sort(
    (prevRes: any, res: any) =>
      (prevRes.get("type") !== "HOME" && res.get("type") === "HOME") ||
      (prevRes.get("type") === "HOME" && res.get("type") !== "HOME" && -1)
  );
  const result = starredResourceDecorator(
    resources,
    state,
    currNode,
    nodeExpanded,
    builtPath
  );
  return result;
}

export function resourceTreeNodeDecorator(
  state: any,
  action: {
    payload: any;
    meta: {
      viewId: string;
      path: string;
      fullPath: string;
      isExpand: boolean;
      nodeExpanded: boolean;
      currNode: any;
      isSummaryDatasetResponse: boolean;
    };
  }
) {
  const {
    path = "",
    isExpand,
    isSummaryDatasetResponse,
    fullPath,
  } = action.meta;
  let nodes;
  if (isSummaryDatasetResponse) {
    nodes = fullPath.split("/");
  } else {
    nodes = path.length ? splitFullPath(path) : [];
  }

  const payloadResources = Immutable.fromJS(
    isSummaryDatasetResponse
      ? getSummaryDatasetPayload(action.payload, fullPath)
      : action.payload?.resources || []
  );

  const builtPath = buildPath(state.get("tree"), nodes, undefined); // [1, 'resources']
  const resources = payloadResources.sort(
    (prevRes: any, res: any) =>
      (prevRes.get("type") !== "HOME" && res.get("type") === "HOME") ||
      (prevRes.get("type") === "HOME" && res.get("type") !== "HOME" && -1)
  );

  // Do nothing in the case when datasets, spaces, or sources are hidden and there are 0 resources
  if (resources.size === 0) return state;

  if (builtPath.length && !isExpand) {
    builtPath.unshift("tree");
    const currentResources = state.getIn(builtPath);

    // If resources already exist, merge the payload results with current resources list
    if (currentResources && currentResources.size > 0) {
      let sortedResources;
      if (
        resources.size === currentResources.size ||
        resources.size < currentResources.size
      ) {
        sortedResources = resources.merge(currentResources);
      } else {
        // Combined both the current resources with the payload resources into 1 list and chooses
        // payload resources item over current resources if there's a duplicate.
        const updatedResources = resources
          .toSet()
          .union(currentResources.toSet())
          .toList();
        sortedResources = updatedResources.sort((a: any, b: any) => {
          return a.get("name").localeCompare(b.get("name"));
        });
      }
      return state.setIn(builtPath, sortedResources);
    } else {
      // Create new resource(s) for item
      return state.setIn(builtPath, resources);
    }
  }
  // Initial load of the resource tree
  return state.set("tree", resources);
}

export function getNodeBranchId(node: any) {
  return `${node.get("id")}-${node.get("branchId")}`;
}

export function clearResourcesByName(state: any, action: { payload: string }) {
  const tree: TreeNode[] = state.get("tree").toJS();
  const idx = tree.findIndex((cur) => cur.name === action.payload);
  if (idx === -1) return;
  if (tree[idx].resources) delete tree[idx].resources;
  return state.set("tree", Immutable.fromJS(tree));
}
