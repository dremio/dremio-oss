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
import { PureComponent } from "react";
import PropTypes from "prop-types";
import Immutable, { fromJS } from "immutable";

import { constructFullPath, splitFullPath } from "utils/pathUtils";
import { starTabNames } from "@app/components/Tree/resourceTreeUtils";

import { fetchAllAndMineScripts } from "@app/components/SQLScripts/sqlScriptsUtils";

import ViewStateWrapper from "@app/components/ViewStateWrapper";
import ResourceTree from "./ResourceTree";

class ResourceTreeController extends PureComponent {
  static propTypes = {
    resourceTree: PropTypes.instanceOf(Immutable.List),
    sources: PropTypes.instanceOf(Immutable.List), //Loaded from parent
    isDatasetsDisabled: PropTypes.bool,
    isSourcesHidden: PropTypes.bool,
    hideDatasets: PropTypes.bool,
    hideSpaces: PropTypes.bool,
    hideSources: PropTypes.bool,
    hideHomes: PropTypes.bool,
    onChange: PropTypes.func,
    insertFullPathAtCursor: PropTypes.func,
    sidebarCollapsed: PropTypes.bool,
    isCollapsable: PropTypes.bool,
    fromModal: PropTypes.bool,
    handleSidebarCollapse: PropTypes.func,
    preselectedNodeId: PropTypes.string,
    dragType: PropTypes.string,
    style: PropTypes.object,
    browser: PropTypes.bool,
    isExpandable: PropTypes.bool,
    shouldShowOverlay: PropTypes.bool,
    shouldAllowAdd: PropTypes.bool,
    isSqlEditorTab: PropTypes.bool,
    dispatchFetchScripts: PropTypes.func,
    nessie: PropTypes.object,
    user: PropTypes.instanceOf(Immutable.Map),
    dispatchSetActiveScript: PropTypes.func,
    starredItems: PropTypes.array,
    updateTreeNodeData: PropTypes.func,
    tabRendered: PropTypes.string,
    treeInitialized: PropTypes.bool,
    starNode: PropTypes.any,
    unstarNode: PropTypes.any,
    handleTabChange: PropTypes.any,
    hasError: PropTypes.bool,
    currentNode: PropTypes.object,
    loadingItems: PropTypes.object,
    datasetsPanel: PropTypes.bool,
    stopAtDatasets: PropTypes.bool,
  };

  constructor(props) {
    super(props);

    this.state = {
      selectedNodeId: props.preselectedNodeId || "",
      expandedNodes: Immutable.List(),
      expandedStarredNodes: Immutable.List(),
      treeInitialized: false,
      viewState: fromJS({ isInProgress: false }),
    };

    this.handleSelectedNodeChange = this.handleSelectedNodeChange.bind(this);
    this.handleNodeClick = this.handleNodeClick.bind(this);
  }

  static formatIdFromNode = (node) => {
    if (!node) return "";
    return constructFullPath(node.get("fullPath"), "");
  };

  initializeTree = async (initialLoad) => {
    const {
      dispatchFetchScripts,
      onChange,
      updateTreeNodeData,
      sidebarCollapsed,
      isSqlEditorTab,
    } = this.props;
    const { selectedNodeId } = this.state;
    this.setState({ treeInitialized: true });

    if ((isSqlEditorTab || sidebarCollapsed) && dispatchFetchScripts) {
      fetchAllAndMineScripts(dispatchFetchScripts, null);
    }

    try {
      this.setLoading(true);
      if (
        selectedNodeId &&
        selectedNodeId !== "tmp" &&
        sidebarCollapsed &&
        !initialLoad
      ) {
        const res = await updateTreeNodeData(true, selectedNodeId);
        if (res.error && !this.isTreeloadRetried) {
          this.isTreeloadRetried = true;
          updateTreeNodeData(true);
        }
        if (onChange) {
          onChange(selectedNodeId);
        }
        this.expandPathToSelectedNode(selectedNodeId);
      } else {
        await updateTreeNodeData(true);
      }
    } finally {
      this.setLoading(false);
    }
  };

  setLoading = (isInProgress) => {
    this.setState({ viewState: fromJS({ isInProgress }) });
  };

  // Async for testing purposes
  async componentDidMount() {
    const { resourceTree, datasetsPanel, sidebarCollapsed, fromModal } =
      this.props;
    const INITIAL_LOAD = true;
    // Only create the Resource Tree if it has not been intialized || initialize resources when opening Save as Dataset modal
    if (
      ((resourceTree && resourceTree.size > 0 && !datasetsPanel) ||
        (datasetsPanel && sidebarCollapsed)) &&
      !fromModal
    )
      return;
    return this.initializeTree(INITIAL_LOAD);
  }

  componentDidUpdate(prevProps) {
    const { treeInitialized } = this.state;
    const {
      isSqlEditorTab,
      sidebarCollapsed,
      dispatchFetchScripts,
      nessie,
      dispatchSetActiveScript,
      resourceTree,
      datasetsPanel,
    } = this.props;
    if (
      (!treeInitialized && !sidebarCollapsed && prevProps.sidebarCollapsed) ||
      (!treeInitialized && resourceTree.size === 0 && !datasetsPanel)
    ) {
      this.initializeTree();
    }

    if (isSqlEditorTab && !prevProps.isSqlEditorTab) {
      fetchAllAndMineScripts(dispatchFetchScripts, null);
    }

    if (!isSqlEditorTab && prevProps.isSqlEditorTab) {
      dispatchSetActiveScript({ script: {} });
    }

    if (prevProps.nessie !== nessie) {
      //Collapse nodes when nessie state is changed
      this.setState({
        //eslint-disable-line
        expandedNodes: Immutable.List(),
        expandedStarredNodes: Immutable.List(),
      });
    }
  }

  handleSelectedNodeChange = (selectedNodeId, node) => {
    const { onChange } = this.props;
    if (onChange) {
      onChange(selectedNodeId, node);
    }
    this.setState({
      selectedNodeId: selectedNodeId,
    });
  };

  isNodeExpanded = (node, nodeError) => {
    if (nodeError) return false;
    const { tabRendered } = this.props;
    const { expandedNodes, expandedStarredNodes } = this.state;
    const nodeId = ResourceTreeController.formatIdFromNode(node);
    const branchId = node.get("branchId");
    if (tabRendered.startsWith(starTabNames.starred)) {
      return Boolean(
        expandedStarredNodes.find(
          (expNodeId) => expNodeId === branchId + nodeId
        )
      );
    } else {
      return Boolean(expandedNodes.find((expNodeId) => expNodeId === nodeId));
    }
  };

  expandPathToSelectedNode = (path) => {
    const { tabRendered } = this.props;
    const { expandedNodes, expandedStarredNodes } = this.state;
    const parentsFullPathList = splitFullPath(path);
    const parents = parentsFullPathList
      .slice(0, parentsFullPathList.length - 1)
      .map((node, index, curArr) => {
        return constructFullPath(curArr.slice(0, index + 1));
      });
    if (tabRendered.startsWith(starTabNames.starred)) {
      this.setState({
        expandedStarredNodes: expandedStarredNodes.concat(
          Immutable.fromJS(parents)
        ),
      });
    } else {
      this.setState({
        expandedNodes: expandedNodes.concat(Immutable.fromJS(parents)),
      });
    }
  };

  handleNodeClick = (clickedNode, isNodeExpanded) => {
    const { updateTreeNodeData, tabRendered } = this.props;
    const selectedNodeId = ResourceTreeController.formatIdFromNode(clickedNode);
    const branchId = clickedNode.get("branchId");

    const shouldExpandNode = !this.isNodeExpanded(clickedNode);

    const nodeIdsToClose = new Set();
    const nodeIdsToOpen = new Set();

    if (!shouldExpandNode) {
      // TODO: DX-6992 ResourceTreeController should maintain child disclosure
      // see notes there for info why this code is here right now
      const findOpenChildren = (node) => {
        const resources = node.get("resources");
        nodeIdsToClose.add(
          tabRendered.startsWith(starTabNames.starred)
            ? branchId + ResourceTreeController.formatIdFromNode(node)
            : ResourceTreeController.formatIdFromNode(node)
        );
        if (!resources) return;
        resources.forEach(findOpenChildren);
      };
      findOpenChildren(clickedNode);
    } else {
      nodeIdsToOpen.add(
        tabRendered.startsWith(starTabNames.starred)
          ? branchId + selectedNodeId
          : selectedNodeId
      );
    }

    updateTreeNodeData(
      tabRendered.startsWith(starTabNames.starred),
      selectedNodeId,
      isNodeExpanded,
      clickedNode
    );

    if (tabRendered.startsWith(starTabNames.starred)) {
      this.setState((state) => {
        state.expandedStarredNodes
          .filter((nodeId) => {
            return !nodeIdsToClose.has(nodeId);
          })
          .push(...nodeIdsToOpen);
        return {
          selectedNodeId,
          expandedStarredNodes: state.expandedStarredNodes
            .filter((nodeId) => !nodeIdsToClose.has(nodeId))
            .push(...nodeIdsToOpen),
        };
      });
    } else {
      this.setState((state) => {
        state.expandedNodes
          .filter((nodeId) => {
            return !nodeIdsToClose.has(nodeId);
          })
          .push(...nodeIdsToOpen);
        return {
          selectedNodeId,
          expandedNodes: state.expandedNodes
            .filter((nodeId) => !nodeIdsToClose.has(nodeId))
            .push(...nodeIdsToOpen),
        };
      });
    }
  };

  render() {
    const {
      isDatasetsDisabled,
      isSourcesHidden,
      style,
      resourceTree,
      sources,
      dragType,
      browser,
      isExpandable,
      shouldShowOverlay,
      shouldAllowAdd,
      insertFullPathAtCursor,
      isSqlEditorTab,
      sidebarCollapsed,
      handleSidebarCollapse,
      isCollapsable,
      fromModal,
      starredItems,
      handleTabChange,
      tabRendered,
      unstarNode,
      starNode,
      currentNode,
      loadingItems,
      hideDatasets,
      hideSpaces,
      hideSources,
      hideHomes,
      stopAtDatasets,
    } = this.props;

    const { selectedNodeId, viewState } = this.state;

    return (
      <ViewStateWrapper viewState={viewState}>
        <ResourceTree
          isDatasetsDisabled={isDatasetsDisabled}
          isSourcesHidden={isSourcesHidden}
          style={style}
          resourceTree={resourceTree}
          sources={sources}
          selectedNodeId={selectedNodeId}
          dragType={dragType}
          formatIdFromNode={ResourceTreeController.formatIdFromNode}
          isNodeExpanded={this.isNodeExpanded}
          handleSelectedNodeChange={this.handleSelectedNodeChange}
          handleNodeClick={this.handleNodeClick}
          browser={browser}
          isExpandable={isExpandable}
          shouldShowOverlay={shouldShowOverlay}
          shouldAllowAdd={shouldAllowAdd}
          insertFullPathAtCursor={insertFullPathAtCursor}
          isSqlEditorTab={isSqlEditorTab}
          sidebarCollapsed={sidebarCollapsed}
          handleSidebarCollapse={handleSidebarCollapse}
          isCollapsable={isCollapsable}
          fromModal={fromModal}
          starredItems={starredItems}
          starNode={starNode}
          unstarNode={unstarNode}
          changeStarredTab={handleTabChange}
          selectedStarredTab={tabRendered}
          currentNode={currentNode}
          loadingItems={loadingItems}
          hideDatasets={hideDatasets}
          hideSpaces={hideSpaces}
          hideSources={hideSources}
          hideHomes={hideHomes}
          stopAtDatasets={stopAtDatasets}
        />
      </ViewStateWrapper>
    );
  }
}

export default ResourceTreeController;
