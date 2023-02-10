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

import { useState } from "react";
import { connect } from "react-redux";

import { loadResourceTree } from "@app/actions/resources/tree";
import {
  loadStarredResources,
  unstarItem,
  starItem,
} from "@app/actions/resources/stars";
import { LOADING_ITEMS, loadSummaryDataset } from "actions/resources/dataset";
import {
  getResourceTree,
  getStarredItemIds,
  getStarredResources,
} from "selectors/tree";
import { fetchScripts, setActiveScript } from "@app/actions/resources/scripts";

import ResourceTreeController from "@app/components/Tree/ResourceTreeController.js";

import { getSortedSources } from "@app/selectors/home";
import { getRefQueryParams } from "@app/utils/nessieUtils";

import {
  starTabNames,
  entityTypes,
  STARRED_VIEW_ID,
  RESOURCE_TREE_VIEW_ID,
  LOAD_RESOURCE_TREE,
  LOAD_STARRED_RESOURCE_LIST,
  getEntityTypeFromNode,
  constructSummaryFullPath,
} from "@app/components/Tree/resourceTreeUtils";
import { getViewState } from "@app/selectors/resources";
import clsx from "clsx";

export type ResourceTreeContainerProps = {
  className?: string;
  resourceTree: any;
  sources: any; //Loaded from parent
  starredResourceTree: any;
  starredItems: any;
  preselectedNodeId: string;
  dragType: string;
  isDatasetsDisabled: boolean;
  isSourcesHidden: boolean;
  hideDatasets: boolean;
  hideSpaces: boolean;
  hideSources: boolean;
  hideHomes: boolean;
  sidebarCollapsed: boolean;
  isCollapsable: boolean;
  fromModal: boolean;
  browser: boolean;
  isExpandable: boolean;
  shouldShowOverlay: boolean;
  shouldAllowAdd: boolean;
  isSqlEditorTab: boolean;
  style: object;
  nessie: any;
  user: any; //immutable map
  loadingItems: object;
  datasetsPanel: boolean;
  stopAtDatasets: boolean;
  onChange: () => void;
  insertFullPathAtCursor: () => void;
  handleSidebarCollapse: () => void;
  dispatchLoadResourceTree: typeof loadResourceTree;
  dispatchFetchScripts: typeof fetchScripts;
  dispatchLoadSummaryDataset: typeof loadSummaryDataset;
  dispatchSetActiveScript: typeof setActiveScript;
  dispatchLoadStarredResources: typeof loadStarredResources;
  dispatchStarItem: (id: string) => Promise<any>;
  dispatchUnstarItem: (id: string) => Promise<any>;
};

export const ResourceTreeContainer = ({
  className,
  resourceTree,
  sources,
  starredResourceTree,
  starredItems,
  preselectedNodeId,
  dragType,
  isDatasetsDisabled,
  isSourcesHidden,
  hideDatasets,
  hideSpaces,
  hideSources,
  hideHomes,
  sidebarCollapsed,
  isCollapsable,
  fromModal,
  browser,
  isExpandable,
  shouldShowOverlay,
  shouldAllowAdd,
  isSqlEditorTab,
  style,
  nessie,
  user,
  loadingItems,
  datasetsPanel,
  stopAtDatasets,
  onChange,
  insertFullPathAtCursor,
  handleSidebarCollapse,
  dispatchLoadResourceTree,
  dispatchFetchScripts,
  dispatchSetActiveScript,
  dispatchLoadSummaryDataset,
  dispatchLoadStarredResources,
  dispatchStarItem,
  dispatchUnstarItem,
}: ResourceTreeContainerProps) => {
  const [tabRendered, handleTabChange] = useState(starTabNames.all);
  const [hasError, setHasError] = useState(false);
  const [currentNode, setCurrentNode] = useState({});

  const showDatasets = !hideDatasets;
  const showSpaces = !hideSpaces;
  const showSources = !hideSources;
  const showHomes = !hideHomes;

  const handleStarredTabChange = (tab: string) => {
    const tabStartsWith = tab.split(" ");
    handleTabChange(tabStartsWith[0]);
  };

  const updateTreeNodeData = (
    loadStarTree: boolean,
    path: string,
    isNodeExpanded: boolean,
    currNode: object
  ) => {
    const { entityType, fullPath, params } = prepareArgumentsForFetch(
      path,
      isNodeExpanded,
      currNode
    );

    setCurrentNode(currNode);

    if (isNodeExpanded && tabRendered === starTabNames.all && !loadStarTree) {
      return fetchResourceTreeResources(
        entityType,
        fullPath,
        params,
        isNodeExpanded,
        currNode
      );
    } else if (tabRendered.startsWith(starTabNames.starred)) {
      return fetchStarredResourceTreeResources(
        entityType,
        fullPath,
        params,
        isNodeExpanded,
        currNode
      );
    } else {
      return fetchResourceTreeResources(
        entityType,
        fullPath,
        params,
        isNodeExpanded,
        currNode
      ).then(
        loadStarTree &&
          fetchStarredResourceTreeResources(
            entityType,
            fullPath,
            params,
            isNodeExpanded,
            currNode
          )
      );
    }
  };

  const fetchResourceTreeResources = (
    entityType: string,
    fullPath: string | undefined,
    params: object,
    isNodeExpanded: boolean,
    currNode: object
  ): any => {
    if (isNodeExpanded) return;
    if (entityType === entityTypes.container) {
      return dispatchLoadResourceTree(
        LOAD_RESOURCE_TREE,
        RESOURCE_TREE_VIEW_ID,
        fullPath,
        params,
        isNodeExpanded,
        currNode
      );
    } else if (entityType === entityTypes.dataset) {
      return dispatchLoadSummaryDataset(
        fullPath,
        RESOURCE_TREE_VIEW_ID,
        LOAD_RESOURCE_TREE,
        isNodeExpanded,
        currNode
      );
    } else {
      return dispatchLoadResourceTree(
        LOAD_RESOURCE_TREE,
        RESOURCE_TREE_VIEW_ID,
        fullPath,
        params,
        isNodeExpanded,
        currNode
      );
    }
  };

  const fetchStarredResourceTreeResources = (
    entityType: string,
    fullPath: string | undefined,
    params: object,
    isNodeExpanded: boolean | undefined,
    currNode: any
  ) => {
    if (isNodeExpanded === undefined) {
      return dispatchLoadStarredResources();
    } else if (!isNodeExpanded && entityType === entityTypes.container) {
      return dispatchLoadResourceTree(
        LOAD_STARRED_RESOURCE_LIST,
        STARRED_VIEW_ID,
        fullPath,
        params,
        true,
        currNode
      );
    } else if (!isNodeExpanded && entityType === entityTypes.dataset) {
      return dispatchLoadSummaryDataset(
        fullPath,
        STARRED_VIEW_ID,
        LOAD_STARRED_RESOURCE_LIST,
        true,
        currNode
      );
    }
    return;
  };

  const starNodeAndGetStarList = (id: string) => {
    dispatchStarItem(id).then(dispatchLoadStarredResources);
  };

  const unstarNodeAndGetStarList = (id: string) => {
    dispatchUnstarItem(id).then(dispatchLoadStarredResources);
  };

  const prepareArgumentsForFetch = (
    path = "",
    isExpand: boolean,
    currNode: any
  ) => {
    const [sourceName] = path.split(".");
    const refQueryParams = getRefQueryParams(
      nessie,
      sourceName.replace(/"/g, "")
    );

    const params = {
      showDatasets,
      showSpaces,
      showSources,
      showHomes,
      isExpand,
      refQueryParams,
    };

    const entityType = getEntityTypeFromNode(currNode);

    const fullPath =
      entityType === entityTypes.dataset
        ? constructSummaryFullPath(path)
        : path;

    return { entityType, fullPath, params };
  };

  return (
    <div
      data-qa="resourceTreeContainer"
      className={clsx("resourceTreeContainer", className)}
    >
      <ResourceTreeController
        sidebarCollapsed={sidebarCollapsed}
        isCollapsable={isCollapsable}
        fromModal={fromModal}
        browser={browser}
        isExpandable={isExpandable}
        shouldShowOverlay={shouldShowOverlay}
        isDatasetsDisabled={isDatasetsDisabled}
        isSourcesHidden={isSourcesHidden}
        shouldAllowAdd={shouldAllowAdd}
        isSqlEditorTab={isSqlEditorTab}
        datasetsPanel={datasetsPanel}
        style={style}
        resourceTree={
          tabRendered === starTabNames.all ? resourceTree : starredResourceTree
        }
        sources={sources}
        starredItems={starredItems}
        preselectedNodeId={preselectedNodeId}
        dragType={dragType}
        user={user}
        tabRendered={tabRendered}
        updateTreeNodeData={updateTreeNodeData}
        starNode={starNodeAndGetStarList}
        unstarNode={unstarNodeAndGetStarList}
        handleTabChange={handleStarredTabChange}
        onChange={onChange}
        insertFullPathAtCursor={insertFullPathAtCursor}
        handleSidebarCollapse={handleSidebarCollapse}
        dispatchFetchScripts={dispatchFetchScripts}
        dispatchSetActiveScript={dispatchSetActiveScript}
        hasError={hasError}
        currentNode={currentNode}
        loadingItems={loadingItems}
        hideDatasets={hideDatasets}
        hideSpaces={hideSpaces}
        hideSources={hideSources}
        hideHomes={hideHomes}
        stopAtDatasets={stopAtDatasets}
      />
    </div>
  );
};

const mapStateToProps = (state: { nessie: any; account: any }) => {
  return {
    resourceTree: getResourceTree(state),
    starredResourceTree: getStarredResources(state),
    starredItems: getStarredItemIds(state),
    sources: getSortedSources(state),
    user: state.account.get("user"),
    nessie: state.nessie,
    loadingItems: getViewState(state, LOADING_ITEMS),
  };
};

const mapDispatchToProps = {
  dispatchFetchScripts: fetchScripts,
  dispatchSetActiveScript: setActiveScript,
  dispatchLoadResourceTree: loadResourceTree,
  dispatchLoadStarredResources: loadStarredResources,
  dispatchLoadSummaryDataset: loadSummaryDataset,
  dispatchStarItem: starItem,
  dispatchUnstarItem: unstarItem,
};

export default connect(
  mapStateToProps,
  mapDispatchToProps
  //@ts-ignore
)(ResourceTreeContainer);
