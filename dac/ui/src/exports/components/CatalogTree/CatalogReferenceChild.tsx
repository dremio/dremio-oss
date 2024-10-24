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

import { useContext, useDeferredValue, useMemo, type FC } from "react";
import type {
  CatalogObject,
  CatalogObjectMethods,
  CatalogReference,
  CommunityDataset,
} from "@dremio/dremio-js/interfaces";
import { useQuery, useSuspenseQuery } from "@tanstack/react-query";
import { CatalogObjectDisplay } from "dremio-ui-common/catalog/CatalogObjectDisplay.js";
import { getSonarContext } from "dremio-ui-common/contexts/SonarContext.js";
import { IconButton } from "dremio-ui-lib/components";
import { catalogById, catalogObjectChildren } from "@inject/queries/catalog";
import {
  starredResourcesQuery,
  useAddStarMutation,
  useRemoveStarMutation,
} from "@inject/queries/stars";
import { TreeConfigContext } from "#oss/components/Tree/treeConfigContext";
import { getIdListString } from "#oss/exports/utilities/getIdListString";
import {
  renderExpandedIcon,
  TreeItemLevelContextProvider,
  useTreeItem,
} from "../Tree";
import { CatalogTableColumns } from "./CatalogTableColumns";
import { CatalogTreeChildren } from "./CatalogTreeChildren";

const CatalogTreeItem: FC<{
  catalogObject: CatalogObject &
    Required<Pick<CatalogObjectMethods, "children">>;
  isStarred: boolean;
}> = (props) => {
  const { addToEditor } = useContext(TreeConfigContext);

  const treeItemProps = useTreeItem(props.catalogObject.id);

  const childrenQuery = useQuery({
    ...catalogObjectChildren(props.catalogObject),
    enabled: treeItemProps.isExpanded,
  });

  const childrenIds = useMemo(() => {
    if (!childrenQuery.data) {
      return undefined;
    }

    return getIdListString(childrenQuery.data);
  }, [childrenQuery.data]);

  const expandedDeferred = useDeferredValue(treeItemProps.isExpanded);

  const addStar = useAddStarMutation(
    getSonarContext().getSelectedProjectId?.(),
  );
  const removeStar = useRemoveStarMutation(
    getSonarContext().getSelectedProjectId?.(),
  );

  return (
    <>
      <div aria-owns={childrenIds} {...treeItemProps}>
        <div className="flex flex-row items-center h-full position-relative overflow-hidden">
          {renderExpandedIcon(treeItemProps.isExpanded)}
          <CatalogObjectDisplay catalogObject={props.catalogObject} />
        </div>
        <div className="px-05 ml-auto catalog-treeitem__actions">
          <IconButton
            tooltip="Open details"
            tooltipPortal
            onClick={(e: React.MouseEvent<HTMLElement>) => {
              e.stopPropagation();
            }}
          >
            <dremio-icon name="interface/meta" />
          </IconButton>
          <IconButton
            tooltip="Add to query"
            tooltipPortal
            onClick={(e: React.MouseEvent<HTMLElement>) => {
              e.stopPropagation();
              addToEditor?.(props.catalogObject.path);
            }}
          >
            <dremio-icon name="interface/add-small" />
          </IconButton>
          <IconButton
            tooltip="Add to starred list"
            tooltipPortal
            onClick={(e: React.MouseEvent<HTMLElement>) => {
              e.stopPropagation();

              if (props.isStarred) {
                removeStar.mutate(props.catalogObject.id);
              } else {
                addStar.mutate(props.catalogObject.id);
              }
            }}
          >
            <dremio-icon
              name={`interface/star-${props.isStarred ? "starred" : "unstarred"}`}
            />
          </IconButton>
        </div>
      </div>
      {expandedDeferred && childrenQuery.data ? (
        <TreeItemLevelContextProvider>
          <CatalogTreeChildren catalogReferences={childrenQuery.data} />
        </TreeItemLevelContextProvider>
      ) : null}
    </>
  );
};

const CatalogTreeLeaf: FC<{
  catalogObject: CatalogObject;
  isStarred: boolean;
}> = (props) => {
  const { addToEditor } = useContext(TreeConfigContext);

  const treeItemProps = useTreeItem(props.catalogObject.id);

  const expandedDeferred = useDeferredValue(treeItemProps.isExpanded);

  const isDataset = (
    catalogObject: CatalogObject,
  ): catalogObject is CommunityDataset =>
    !!(catalogObject as CommunityDataset).fields;

  const childrenIds = useMemo(() => {
    if (!isDataset(props.catalogObject)) {
      return;
    }

    return props.catalogObject.fields
      .reduce((accum, curr) => accum + " " + curr.name, "")
      .trimStart();
  }, [props.catalogObject]);

  const addStar = useAddStarMutation(
    getSonarContext().getSelectedProjectId?.(),
  );
  const removeStar = useRemoveStarMutation(
    getSonarContext().getSelectedProjectId?.(),
  );

  return (
    <>
      <div aria-owns={childrenIds} {...treeItemProps}>
        <div className="flex flex-row items-center h-full position-relative overflow-hidden">
          {renderExpandedIcon(treeItemProps.isExpanded)}
          <CatalogObjectDisplay catalogObject={props.catalogObject} />
        </div>
        <div className="px-05 ml-auto catalog-treeitem__actions">
          <IconButton
            tooltip="Open details"
            tooltipPortal
            onClick={(e: React.MouseEvent<HTMLElement>) => {
              e.stopPropagation();
            }}
          >
            <dremio-icon name="interface/meta" />
          </IconButton>
          <IconButton
            tooltip="Add to query"
            tooltipPortal
            onClick={(e: React.MouseEvent<HTMLElement>) => {
              e.stopPropagation();
              addToEditor?.(props.catalogObject.path);
            }}
          >
            <dremio-icon name="interface/add-small" />
          </IconButton>
          <IconButton
            tooltip="Add to starred list"
            tooltipPortal
            onClick={(e: React.MouseEvent<HTMLElement>) => {
              e.stopPropagation();

              if (props.isStarred) {
                removeStar.mutate(props.catalogObject.id);
              } else {
                addStar.mutate(props.catalogObject.id);
              }
            }}
          >
            <dremio-icon
              name={`interface/star-${props.isStarred ? "starred" : "unstarred"}`}
            />
          </IconButton>
        </div>
      </div>
      {expandedDeferred && isDataset(props.catalogObject) ? (
        <TreeItemLevelContextProvider>
          <CatalogTableColumns dataset={props.catalogObject} />
        </TreeItemLevelContextProvider>
      ) : null}
    </>
  );
};

export const CatalogReferenceChild: FC<{
  catalogReference: CatalogReference;
}> = (props) => {
  const catalogObject: CatalogObject & Pick<CatalogObjectMethods, "children"> =
    useSuspenseQuery(
      catalogById(getSonarContext().getSelectedProjectId?.())(
        props.catalogReference.id,
      ),
    ).data;

  const starredResources = useQuery(
    starredResourcesQuery(getSonarContext().getSelectedProjectId?.()),
  ).data;

  const isStarred = useMemo(
    () => !!starredResources?.has(catalogObject.id),
    [catalogObject.id, starredResources],
  );

  return catalogObject.children ? (
    <CatalogTreeItem catalogObject={catalogObject} isStarred={isStarred} />
  ) : (
    <CatalogTreeLeaf catalogObject={catalogObject} isStarred={isStarred} />
  );
};
