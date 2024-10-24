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

import { store } from "#oss/store/store";
import { useSelector } from "react-redux";
import { useIsArsEnabled } from "@inject/utils/arsUtils";
import { getHomeSource, getSortedSources } from "#oss/selectors/home";
import { defaultConfigContext } from "#oss/components/Tree/treeConfigContext";
import { ENTITY_TYPES } from "#oss/constants/Constants";
import { sortByName } from "#oss/components/Tree/resourceTreeUtils";

export const findDatasetFromPath = (root: any, remainingPath: any): any => {
  const [name, ...restOfPath] = remainingPath;
  const match = root.find((node: any) => node.get("name") === name);
  if (!match) {
    return null;
  }
  if (restOfPath.length === 0 || !match.get("resources")) {
    const matched = match.toJS();
    try {
      return {
        ...matched,
        ...JSON.parse(matched.id),
      };
    } catch (e) {
      return matched;
    }
  }
  return findDatasetFromPath(match.get("resources"), restOfPath);
};

export const getDatasetByPath = (pathList: string[]) => {
  return findDatasetFromPath(
    store.getState().resources.entities.get("tree"),
    pathList,
  );
};

export const useFilterTreeArs = () => {
  const [, isArsEnabled] = useIsArsEnabled();

  const homeSource = useSelector((state) => {
    return getHomeSource(getSortedSources(state));
  });

  if (!isArsEnabled) return defaultConfigContext.filterTree;

  return (tree: any) => {
    let homeSourceNode: any = null;
    const filtered = tree
      .filter((node: any) => {
        if (
          homeSource &&
          node.get("type").toLowerCase() === ENTITY_TYPES.source &&
          node.get("name") === homeSource.get("name")
        ) {
          homeSourceNode = node;
          return false;
        }

        return node.get("type").toLowerCase() !== ENTITY_TYPES.space;
      })
      .sort(sortByName("asc"));

    if (homeSourceNode) {
      return filtered.splice(0, 0, homeSourceNode);
    } else {
      return filtered;
    }
  };
};

export const withFilterTreeArs =
  <T,>(WrappedComponent: React.ComponentClass) =>
  (props: T) => {
    const filterTreeArs = useFilterTreeArs();
    return <WrappedComponent filterTree={filterTreeArs} {...props} />;
  };
