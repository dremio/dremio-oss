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
import { Component } from 'react';
import { connect } from 'react-redux';
import Immutable from 'immutable';
import classNames from 'classnames';
import Radium from 'radium';
import { injectIntl } from 'react-intl';

import PropTypes from 'prop-types';

import { loadParents } from 'actions/resources/spaceDetails';
import { getParentList, getViewState } from 'selectors/resources';
import ResourceTreeController from 'components/Tree/ResourceTreeController';
import { datasetTitle } from 'uiTheme/radium/typography';
import { PALE_GREY, SECONDARY_BORDER } from 'uiTheme/radium/colors';
import DatasetList from 'components/DatasetList/DatasetList';
import SearchDatasetsPopover from '@app/components/DatasetList/SearchDatasetsPopover';
import exploreUtils from '@app/utils/explore/exploreUtils';

export const PARENTS_TAB = 'PARENTS_TAB';
export const BROWSE_TAB = 'BROWSE_TAB';
export const SEARCH_TAB = 'SEARCH_TAB';

const PARENT_LIST_VIEW_ID = 'PARENT_LIST_VIEW_ID';

@injectIntl
@Radium
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
    location: PropTypes.object
  };

  static contextTypes = {
    routeParams: PropTypes.object
  };

  static defaultProps = {
    parentList: []
  };

  constructor(props) {
    super(props);
    /**
     * used for display headertabs in Datasetspanel
     * @type {Array}
     */
    this.tabs = [
      { name: props.intl.formatMessage({id: 'Dataset.Parents'}), id: PARENTS_TAB},
      { name: props.intl.formatMessage({id: 'Dataset.Browse'}), id: BROWSE_TAB},
      { name: props.intl.formatMessage({id: 'Dataset.Search'}), id: SEARCH_TAB}
    ];

    this.state = {
      activeTabId: undefined,
      isCollapsable: true
    };
  }

  componentWillMount() {
    this.receiveProps(this.props, {});
  }


  componentWillReceiveProps(nextProps) {
    this.receiveProps(nextProps, this.props);
  }

  getActiveTabId() {
    const { activeTabId } = this.state;

    if (activeTabId) return activeTabId;
    return BROWSE_TAB;
  }

  loadParentDatasets(dataset) {
    const { parentListViewState } = this.props;
    this.props.loadParents(
      dataset.get('fullPath'),
      dataset.get('datasetVersion'),
      parentListViewState && parentListViewState.get('viewId')
    );
  }

  isDataLoading() {
    const { viewState, parentListViewState } = this.props;
    return (parentListViewState && parentListViewState.get('isInProgress')) || (Boolean(viewState && viewState.has('isInProgress') && viewState.get('isInProgress')));
  }

  receiveProps(nextProps, oldProps) {
    const nextDataset = nextProps.dataset;
    const nextDatasetVersion = nextDataset && nextDataset.get('datasetVersion');
    const oldDatasetVersion = oldProps.dataset && oldProps.dataset.get('datasetVersion');
    const isDatasetVersionChanged = nextDatasetVersion && nextDatasetVersion !== oldDatasetVersion;

    const viewState = nextProps.viewState || Immutable.Map();
    if (
      nextDataset && nextDataset.get('isNewQuery') ||
      viewState.get('isInProgress') ||
      // fetching parents for tmp.UNTITLED currently always 500s, so prevent for now. See DX-7466
      Immutable.List(['tmp', 'UNTITLED']).equals(nextDataset && nextDataset.get('fullPath'))
    ) {
      return;
    }

    const { isVisible } = nextProps;
    const becameVisible = isVisible && !oldProps.isVisible;
    if ((becameVisible && nextDatasetVersion) || (isVisible && isDatasetVersionChanged)) {
      this.loadParentDatasets(nextDataset);
    }
  }

  /**
   * [chooseItemTab]
   * [choosed tab, func displays data of tab]
   * @return {SearchDatasets or ResourceTree component}
   */
  chooseItemTab = () => {
    const activeTabId  = this.getActiveTabId();
    const { parentList,
      dragType,
      insertFullPathAtCursor,
      location,
      handleSidebarCollapse,
      sidebarCollapsed
    } = this.props;

    switch (activeTabId) {
    case PARENTS_TAB:
      return <DatasetList
        dragType={dragType}
        data={Immutable.fromJS(parentList)}
        changeSelectedNode={() => {}}
        style={styles.datasetList}
        isInProgress={this.isDataLoading()}
      />;
    case SEARCH_TAB:
      return <SearchDatasetsPopover
        changeSelectedNode={() => {}}
        dragType={dragType}
        insertFullPathAtCursor={insertFullPathAtCursor}
        showAddIcon
      />;
    case BROWSE_TAB:
      return <>
        <ResourceTreeController
          style={{ minHeight: 'initial', maxHeight: 'initial' }}
          preselectedNodeId={this.context.routeParams.resourceId}
          insertFullPathAtCursor={insertFullPathAtCursor}
          dragType={dragType}
          isSqlEditorTab={exploreUtils.isSqlEditorTab(location)}
          sidebarCollapsed={sidebarCollapsed}
          handleSidebarCollapse={handleSidebarCollapse}
          isCollapsable={this.state.isCollapsable}
          browser
          isExpandable
          shouldShowOverlay
          shouldAllowAdd
        />
      </>;
    default:
      throw new Error('unknown tab id');
    }
  }

  updateActiveTab(id) {
    this.setState({
      activeTabId: id
    });
  }

  shouldShowParentTab() {
    const { dataset } = this.props;
    const isNewDataset = dataset && dataset.get('isNewQuery');
    return (this.props.parentList.length > 0 || this.isDataLoading()) && !isNewDataset;
  }

  /**
   * [displayHeaderTabsItems]
   * @return {header tabs}
   */
  renderHeaderTabsItems = () => {
    return this.tabs.map((tab) => {
      const isActive = tab.id === this.getActiveTabId();
      const headerTabsClasses = classNames('header-tabs-item', { activeTab: isActive });
      if (tab.id === PARENTS_TAB) {
        if (!this.shouldShowParentTab()) {
          return null;
        }
      }
      return (
        <div
          key={tab.id} className={headerTabsClasses}
          style={[styles.headerTab, isActive && styles.headerTab.activeTab]}
          onMouseDown={e => e.preventDefault()}
          onClick={this.updateActiveTab.bind(this, tab.id)}>
          {tab.name}
        </div>
      );
    });
  }

  render() {
    return (
      <>
        { this.chooseItemTab() }
      </>
    );
  }
}

const mapStateToProps = (state) => ({
  parentList: getParentList(state),
  parentListViewState: getViewState(state, PARENT_LIST_VIEW_ID)
});

export default connect(mapStateToProps, { loadParents })(DatasetsPanel);

const styles = {
  datasetList: {
    overflowY: 'auto'
  },
  headerTabs: {
    width: '100%',
    backgroundColor: PALE_GREY
  },
  headerTab: {
    float: 'left',
    cursor: 'pointer',
    width: 55,
    height: 24,
    ...datasetTitle,
    fontWeight: 400,
    fontSize: 12,
    alignItems: 'center',
    display: 'inline-flex',
    padding: 10,
    justifyContent: 'center',
    ':hover': {
      backgroundColor: SECONDARY_BORDER
    },
    activeTab: {
      backgroundColor: SECONDARY_BORDER
    }
  }
};
