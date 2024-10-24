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
import { Component } from "react";
import { compose } from "redux";
import PropTypes from "prop-types";
import Immutable from "immutable";
import { injectIntl } from "react-intl";

import {
  CONTAINER_ENTITY_TYPES,
  DATASET_ENTITY_TYPES,
} from "#oss/constants/Constants";

import Tree from "./Tree";
import TreeBrowser from "./TreeBrowser";
import { withTreeConfigContext } from "./treeConfigContext";
import { withEntityProps } from "dyn-load/utils/entity-utils";

import "./ResourceTree.less";

export class ResourceTree extends Component {
  static propTypes = {
    resourceTree: PropTypes.instanceOf(Immutable.List),
    selectedNodeId: PropTypes.string,
    isDatasetsDisabled: PropTypes.bool,
    isSourcesHidden: PropTypes.bool,
    dragType: PropTypes.string,
    formatIdFromNode: PropTypes.func,
    isNodeExpanded: PropTypes.func,
    handleSelectedNodeChange: PropTypes.func,
    handleNodeClick: PropTypes.func,
    insertFullPathAtCursor: PropTypes.func,
    sidebarCollapsed: PropTypes.bool,
    handleSidebarCollapse: PropTypes.func,
    style: PropTypes.object,
    intl: PropTypes.object.isRequired,
    browser: PropTypes.bool,
    isExpandable: PropTypes.bool,
    shouldShowOverlay: PropTypes.bool,
    shouldAllowAdd: PropTypes.bool,
    isSqlEditorTab: PropTypes.bool,
    isCollapsable: PropTypes.bool,
    fromModal: PropTypes.bool,
    hideSourcesforModal: PropTypes.bool,
    changeStarredTab: PropTypes.func,
    starNode: PropTypes.func,
    unstarNode: PropTypes.func,
    starredItems: PropTypes.array,
    selectedStarredTab: PropTypes.string,
    currentNode: PropTypes.object,
    loadingItems: PropTypes.object,
    hideDatasets: PropTypes.bool,
    hideSpaces: PropTypes.bool,
    hideSources: PropTypes.bool,
    hideHomes: PropTypes.bool,
    stopAtDatasets: PropTypes.bool,
    treeConfigContext: PropTypes.object,
    handleDatasetDetails: PropTypes.func,
    isManageAccessEnabled: PropTypes.bool,
  };

  constructor(props) {
    super(props);

    this.addtoEditor = this.addtoEditor.bind(this);
    this.handleSelectedNodeChange = this.handleSelectedNodeChange.bind(this);
  }

  static isNodeExpandable = (node, stopAtDatasets = false) => {
    return (
      CONTAINER_ENTITY_TYPES.has(node.get("type")) ||
      (DATASET_ENTITY_TYPES.has(node.get("type")) && !stopAtDatasets)
    );
  };

  handleSelectedNodeChange(node, isNodeExpanded) {
    const {
      handleNodeClick,
      handleSelectedNodeChange,
      formatIdFromNode,
      stopAtDatasets,
    } = this.props;

    if (node && ResourceTree.isNodeExpandable(node, stopAtDatasets)) {
      handleNodeClick(node, isNodeExpanded);
    }
    handleSelectedNodeChange(formatIdFromNode(node), node);
  }

  addtoEditor(id) {
    const { insertFullPathAtCursor } = this.props;
    insertFullPathAtCursor(id);
  }

  render() {
    const {
      browser,
      resourceTree,
      style,
      selectedNodeId,
      isNodeExpanded,
      dragType,
      formatIdFromNode,
      isDatasetsDisabled,
      isSourcesHidden,
      shouldAllowAdd,
      shouldShowOverlay,
      isExpandable,
      starredItems,
      starNode,
      unstarNode,
      changeStarredTab,
      selectedStarredTab,
      isSqlEditorTab,
      handleSidebarCollapse,
      sidebarCollapsed,
      isCollapsable,
      fromModal,
      hideSourcesforModal,
      currentNode,
      loadingItems,
      hideDatasets,
      hideSpaces,
      hideSources,
      hideHomes,
      stopAtDatasets,
      isManageAccessEnabled,
    } = this.props;

    return (
      <div style={{ ...style }} className="resourceTree">
        {browser ? (
          <TreeBrowser
            resourceTree={resourceTree}
            selectedNodeId={selectedNodeId}
            isNodeExpanded={isNodeExpanded}
            dragType={dragType}
            addtoEditor={this.addtoEditor}
            formatIdFromNode={formatIdFromNode}
            isDatasetsDisabled={isDatasetsDisabled}
            isSourcesHidden={isSourcesHidden}
            shouldAllowAdd={shouldAllowAdd}
            shouldShowOverlay={shouldShowOverlay}
            handleSelectedNodeChange={this.handleSelectedNodeChange}
            isNodeExpandable={(node) =>
              ResourceTree.isNodeExpandable(node, stopAtDatasets)
            }
            isExpandable={isExpandable}
            isSqlEditorTab={isSqlEditorTab}
            handleSidebarCollapse={handleSidebarCollapse}
            sidebarCollapsed={sidebarCollapsed}
            isCollapsable={isCollapsable}
            starredItems={starredItems}
            starNode={starNode}
            unstarNode={unstarNode}
            changeStarredTab={changeStarredTab}
            selectedStarredTab={selectedStarredTab}
            currentNode={currentNode}
            loadingItems={loadingItems}
            hideDatasets={hideDatasets}
            hideSpaces={hideSpaces}
            hideSources={hideSources}
            hideHomes={hideHomes}
            isManageAccessEnabled={isManageAccessEnabled}
          />
        ) : (
          <Tree
            resourceTree={resourceTree}
            selectedNodeId={selectedNodeId}
            fromModal={fromModal}
            hideSourcesforModal={hideSourcesforModal}
            isNodeExpanded={isNodeExpanded}
            dragType={dragType}
            addtoEditor={this.addtoEditor}
            formatIdFromNode={formatIdFromNode}
            isDatasetsDisabled={isDatasetsDisabled}
            isSourcesHidden={isSourcesHidden}
            shouldAllowAdd={shouldAllowAdd}
            shouldShowOverlay={shouldShowOverlay}
            handleSelectedNodeChange={this.handleSelectedNodeChange}
            isNodeExpandable={(node) =>
              ResourceTree.isNodeExpandable(node, stopAtDatasets)
            }
            isExpandable={isExpandable}
            starredItems={starredItems}
            starNode={starNode}
            unstarNode={unstarNode}
            currentNode={currentNode}
            loadingItems={loadingItems}
            hideDatasets={hideDatasets}
            hideSpaces={hideSpaces}
            hideSources={hideSources}
            hideHomes={hideHomes}
            isManageAccessEnabled={isManageAccessEnabled}
          />
        )}
      </div>
    );
  }
}
export default compose(
  injectIntl,
  withTreeConfigContext,
  withEntityProps,
)(ResourceTree);
