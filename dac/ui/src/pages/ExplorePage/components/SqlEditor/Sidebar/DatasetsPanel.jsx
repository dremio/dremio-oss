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
import { connect } from "react-redux";
import Immutable from "immutable";
import { injectIntl } from "react-intl";

import PropTypes from "prop-types";

import { loadParents } from "actions/resources/spaceDetails";
import { getParentList, getViewState } from "selectors/resources";
import ResourceTreeContainer from "components/Tree/ResourceTreeContainer";
import DatasetList from "components/DatasetList/DatasetList";
import SearchDatasetsPopover from "@app/components/DatasetList/SearchDatasetsPopover";
import exploreUtils from "@app/utils/explore/exploreUtils";
import { compose } from "redux";
import { withCatalogARSFlag } from "@inject/utils/arsUtils";
import { getHomeSource, getSortedSources } from "@app/selectors/home";
import { clearResourceTree } from "@app/actions/resources/tree";
import { TreeConfigContext } from "@app/components/Tree/treeConfigContext";
import { defaultConfigContext } from "@app/components/Tree/treeConfigContext";

export const PARENTS_TAB = "PARENTS_TAB";
export const BROWSE_TAB = "BROWSE_TAB";
export const SEARCH_TAB = "SEARCH_TAB";

const PARENT_LIST_VIEW_ID = "PARENT_LIST_VIEW_ID";

export class DatasetsPanel extends Component {
  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map),
    height: PropTypes.number,
    isVisible: PropTypes.bool.isRequired,
    search: PropTypes.object,
    dragType: PropTypes.string,
    addFuncToSqlEditor: PropTypes.func,
    loadSearchData: PropTypes.func,
    addFullPathToSqlEditor: PropTypes.func,
    sidebarCollapsed: PropTypes.bool,
    handleSidebarCollapse: PropTypes.func,
    parentList: PropTypes.array,
    loadParents: PropTypes.func,
    viewState: PropTypes.instanceOf(Immutable.Map),
    parentListViewState: PropTypes.instanceOf(Immutable.Map),
    intl: PropTypes.object.isRequired,
    insertFullPathAtCursor: PropTypes.func,
    location: PropTypes.object,
    isArsEnabled: PropTypes.bool,
    isArsLoading: PropTypes.bool,
    homeSource: PropTypes.any,
    clearResourceTree: PropTypes.func,
    isSourcesLoading: PropTypes.bool,
    handleDatasetDetails: PropTypes.func,
  };

  static contextTypes = {
    routeParams: PropTypes.object,
  };

  static defaultProps = {
    parentList: [],
  };

  constructor(props) {
    super(props);
    /**
     * used for display headertabs in Datasetspanel
     * @type {Array}
     */
    this.tabs = [
      {
        name: props.intl.formatMessage({ id: "Dataset.Parents" }),
        id: PARENTS_TAB,
      },
      {
        name: props.intl.formatMessage({ id: "Dataset.Browse" }),
        id: BROWSE_TAB,
      },
      {
        name: props.intl.formatMessage({ id: "Dataset.Search" }),
        id: SEARCH_TAB,
      },
    ];

    this.state = {
      activeTabId: undefined,
      isCollapsable: true,
    };
  }

  UNSAFE_componentWillMount() {
    this.receiveProps(this.props, {});
  }

  UNSAFE_componentWillReceiveProps(nextProps) {
    this.receiveProps(nextProps, this.props);
  }

  // Workaround for double-fetch bug that has existed for a while when exiting->entering sql runner page
  componentWillUnmount() {
    this.props.clearResourceTree();
  }

  getActiveTabId() {
    const { activeTabId } = this.state;

    if (activeTabId) return activeTabId;
    return BROWSE_TAB;
  }

  loadParentDatasets(dataset) {
    const { parentListViewState } = this.props;
    this.props.loadParents(
      dataset.get("fullPath"),
      dataset.get("datasetVersion"),
      parentListViewState && parentListViewState.get("viewId"),
    );
  }

  isDataLoading() {
    const { viewState, parentListViewState } = this.props;
    return (
      (parentListViewState && parentListViewState.get("isInProgress")) ||
      Boolean(
        viewState &&
          viewState.has("isInProgress") &&
          viewState.get("isInProgress"),
      )
    );
  }

  receiveProps(nextProps, oldProps) {
    const nextDataset = nextProps.dataset;
    const nextDatasetVersion = nextDataset && nextDataset.get("datasetVersion");
    const oldDatasetVersion =
      oldProps.dataset && oldProps.dataset.get("datasetVersion");
    const isDatasetVersionChanged =
      nextDatasetVersion && nextDatasetVersion !== oldDatasetVersion;

    const viewState = nextProps.viewState || Immutable.Map();
    if (
      (nextDataset && nextDataset.get("isNewQuery")) ||
      viewState.get("isInProgress") ||
      // fetching parents for tmp.UNTITLED currently always 500s, so prevent for now. See DX-7466
      Immutable.List(["tmp", "UNTITLED"]).equals(
        nextDataset && nextDataset.get("fullPath"),
      )
    ) {
      return;
    }

    const { isVisible } = nextProps;
    const becameVisible = isVisible && !oldProps.isVisible;
    if (
      (becameVisible && nextDatasetVersion) ||
      (isVisible && isDatasetVersionChanged)
    ) {
      this.loadParentDatasets(nextDataset);
    }
  }

  /**
   * [chooseItemTab]
   * [choosed tab, func displays data of tab]
   * @return {SearchDatasets or ResourceTree component}
   */
  chooseItemTab = () => {
    const activeTabId = this.getActiveTabId();
    const {
      parentList,
      dragType,
      insertFullPathAtCursor,
      location,
      handleSidebarCollapse,
      sidebarCollapsed,
      homeSource,
      isArsLoading,
      isSourcesLoading,
      handleDatasetDetails,
    } = this.props;

    switch (activeTabId) {
      case PARENTS_TAB:
        return (
          <DatasetList
            dragType={dragType}
            data={Immutable.fromJS(parentList)}
            changeSelectedNode={() => {}}
            style={styles.datasetList}
            isInProgress={this.isDataLoading()}
          />
        );
      case SEARCH_TAB:
        return (
          <SearchDatasetsPopover
            changeSelectedNode={() => {}}
            dragType={dragType}
            insertFullPathAtCursor={insertFullPathAtCursor}
            showAddIcon
          />
        );
      case BROWSE_TAB:
        if (isArsLoading || isSourcesLoading) return null; // Don't show tree until spaces filtered out and homeSource fetched
        return (
          <TreeConfigContext.Provider
            value={{
              ...defaultConfigContext,
              handleDatasetDetails: handleDatasetDetails,
            }}
          >
            <ResourceTreeContainer
              preselectedNodeId={
                // May want to eventually decouple the homeSource expansion from the preselectedNodeId
                homeSource?.get("name") || this.context.routeParams.resourceId
              }
              insertFullPathAtCursor={insertFullPathAtCursor}
              dragType={dragType}
              isSqlEditorTab={exploreUtils.isSqlEditorTab(location)}
              sidebarCollapsed={sidebarCollapsed}
              handleSidebarCollapse={handleSidebarCollapse}
              isCollapsable={this.state.isCollapsable}
              datasetsPanel={true}
              browser
              isExpandable
              shouldShowOverlay
              shouldAllowAdd
            />
          </TreeConfigContext.Provider>
        );
      default:
        throw new Error("unknown tab id");
    }
  };

  updateActiveTab(id) {
    this.setState({
      activeTabId: id,
    });
  }

  shouldShowParentTab() {
    const { dataset } = this.props;
    const isNewDataset = dataset && dataset.get("isNewQuery");
    return (
      (this.props.parentList.length > 0 || this.isDataLoading()) &&
      !isNewDataset
    );
  }

  render() {
    return <>{this.chooseItemTab()}</>;
  }
}

DatasetsPanel = injectIntl(DatasetsPanel);

const mapStateToProps = (state, { isArsEnabled }) => ({
  isSourcesLoading:
    getViewState(state, "AllSources")?.get("isInProgress") ?? true,
  ...(isArsEnabled && {
    homeSource: getHomeSource(getSortedSources(state)),
  }),
  parentList: getParentList(state),
  parentListViewState: getViewState(state, PARENT_LIST_VIEW_ID),
});

export default compose(
  withCatalogARSFlag,
  connect(mapStateToProps, { loadParents, clearResourceTree }),
)(DatasetsPanel);

const styles = {
  datasetList: {
    overflowY: "auto",
  },
  headerTabs: {
    width: "100%",
    backgroundColor: "var(--fill--secondary)",
  },
};
