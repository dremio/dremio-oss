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
import sentryUtil from "@app/utils/sentryUtil";

export class ResourceTreeController extends PureComponent {
  static propTypes = {
    resourceTree: PropTypes.instanceOf(Immutable.List),
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

  //Recursively fetch selected path and then expand
  fetchAndExpandSelectedNode = (nodeArg) => {
    try {
      const { selectedNodeId } = this.state;
      const node = nodeArg || selectedNodeId;
      if (!node || node === "tmp") return;
      const [root, ...rest] = node.split(".");
      return this.fetchNodePath(root, rest.join("."));
    } catch (e) {
      sentryUtil.logException(e);
    }
  };

  fetchNodePath = async (
    root,
    remain,
    nodesToExpand = [],
    skipLast = false // Skip expanding the last node
  ) => {
    if (!root) return;
    const [next, ...remaining] = remain.split(".") || [];
    const { updateTreeNodeData } = this.props;
    try {
      this.setLoading(true);
      await updateTreeNodeData(true, root);
    } catch (e) {
      sentryUtil.logException(e);
      this.setLoading(false);
      return;
    }

    this.setState({
      expandedNodes: Immutable.fromJS([
        ...nodesToExpand,
        constructFullPath(root.split(".")), //Wrap paths that have spaces in quotes, etc
      ]),
    });

    if (next && (!skipLast || remaining.length)) {
      return this.fetchNodePath([root, next].join("."), remaining.join("."), [
        ...nodesToExpand,
        root,
      ]);
    } else {
      this.setLoading(false);
      // No more to fetch, now select and expand to the node
      this.props.onChange?.(
        skipLast ? root.split(".").concat(remain).join(".") : root
      );
    }
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
    const { resourceTree, datasetsPanel, sidebarCollapsed } = this.props;
    // Only create the Resource Tree if it has not been intialized || initialize resources when opening Save as Dataset modal
    if (datasetsPanel && sidebarCollapsed) return;

    if (resourceTree && resourceTree.size > 0) {
      // Expand to the preselected node even when tree is already initialized
      this.fetchAndExpandSelectedNode();
    }

    return this.initializeTree(true).then(this.fetchAndExpandSelectedNode);
  }

  componentDidUpdate(prevProps) {
    const { treeInitialized } = this.state;
    const {
      isSqlEditorTab,
      sidebarCollapsed,
      dispatchFetchScripts,
      dispatchSetActiveScript,
      resourceTree,
      datasetsPanel,
      preselectedNodeId,
    } = this.props;

    if (
      (!treeInitialized && !sidebarCollapsed && prevProps.sidebarCollapsed) ||
      (!treeInitialized && resourceTree.size === 0 && !datasetsPanel)
    ) {
      return this.initializeTree().then(() =>
        this.fetchAndExpandSelectedNode(preselectedNodeId)
      );
    }

    if (isSqlEditorTab && !prevProps.isSqlEditorTab) {
      fetchAllAndMineScripts(dispatchFetchScripts, null);
    }

    if (!isSqlEditorTab && prevProps.isSqlEditorTab) {
      dispatchSetActiveScript({ script: {} });
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

    const shouldExpandNode = !isNodeExpanded;

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

    this.setState((state) => {
      const key = tabRendered.startsWith(starTabNames.starred)
        ? "expandedStarredNodes"
        : "expandedNodes";
      return {
        selectedNodeId,
        [key]: state[key]
          .filter((nodeId) => !nodeIdsToClose.has(nodeId))
          .push(...nodeIdsToOpen),
      };
    });
  };

  render() {
    const {
      isDatasetsDisabled,
      isSourcesHidden,
      style,
      resourceTree,
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
