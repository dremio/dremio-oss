/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
import PropTypes from 'prop-types';
import Immutable from 'immutable';
import { connect }   from 'react-redux';
import { withRouter } from 'react-router';
import domHelpers  from 'dom-helpers';

import { getExploreViewState } from 'selectors/resources';
import { moduleStateHOC } from '@app/containers/ModuleStateContainer';
import explore from '@app/reducers/explore';
import { getHistory, exploreStateKey, getExploreState, getExplorePageDataset } from 'selectors/explore';
import { performLoadDataset } from 'actions/explore/dataset/get';
import { setCurrentSql } from 'actions/explore/view';
import { resetViewState } from 'actions/resources';
import { withDatasetChanges } from '@app/pages/ExplorePage/DatasetChanges';
import { withHookProvider, withRouteLeaveSubscription } from '@app/containers/RouteLeave.js';

import {
  updateSqlPartSize
} from 'actions/explore/ui';

import { showConfirmationDialog } from 'actions/confirmation';

import { setResizeProgressState } from 'actions/explore/ui';

import { updateGridSizes, updateRightTreeVisibility } from 'actions/ui/ui';

import { hasDatasetChanged } from 'utils/datasetUtils';

import { PageTypes, pageTypeValuesSet } from '@app/pages/ExplorePage/pageTypes';

import QlikStateModal from './components/modals/QlikStateModal';

import ExplorePage from './ExplorePage';

const HEIGHT_AROUND_SQL_EDITOR = 175;
const defaultPageType = PageTypes.default;

export class ExplorePageControllerComponent extends Component {
  static propTypes = {
    pageType: PropTypes.string, // string, because we validate page type in receiveProps and render methods
    dataset: PropTypes.instanceOf(Immutable.Map).isRequired,
    location: PropTypes.object.isRequired,
    sqlState: PropTypes.bool.isRequired,
    sqlSize: PropTypes.number,
    route: PropTypes.object,
    history: PropTypes.instanceOf(Immutable.Map),
    rightTreeVisible: PropTypes.bool,
    updateRightTreeVisibility: PropTypes.func,
    updateGridSizes: PropTypes.func,
    setResizeProgressState: PropTypes.func,
    isResizeInProgress: PropTypes.bool,
    updateSqlPartSize: PropTypes.func.isRequired,
    exploreViewState: PropTypes.instanceOf(Immutable.Map),
    performLoadDataset: PropTypes.func.isRequired,
    setCurrentSql: PropTypes.func.isRequired,
    resetViewState: PropTypes.func.isRequired,
    style: PropTypes.object,
    showConfirmationDialog: PropTypes.func,
    router: PropTypes.object,
    addHasChangesHook: PropTypes.func, // (hasChangesCallback[: (nextLocation) => bool]) => void
    // provided by withDatasetChanges
    getDatasetChangeDetails: PropTypes.func.isRequired
  };

  static defaultProps = {
    pageType: defaultPageType
  };

  discardUnsavedChangesConfirmed = false;

  constructor(props) {
    super(props);
    this.toggleRightTree = this.toggleRightTree.bind(this);
    this.state = {
      dragType: 'groupBy',
      accessModalState: false,
      nextLocation: null,
      isUnsavedChangesModalShowing: false
    };
  }

  componentDidMount() {
    const { addHasChangesHook } = this.props;
    this.receiveProps(this.props);
    if (addHasChangesHook) {
      addHasChangesHook(this.shouldShowUnsavedChangesPopup);
    }
  }

  componentWillReceiveProps(nextProps) {
    this.receiveProps(nextProps, this.props);
  }

  isPageTypeValid(pageType) {
    return pageTypeValuesSet.has(pageType);
  }

  receiveProps(nextProps, prevProps = {}) {
    if (!this.isPageTypeValid(nextProps.pageType)) {
      nextProps.router.push('/');
    }

    const datasetChanged = hasDatasetChanged(nextProps.dataset, prevProps.dataset);
    if (datasetChanged) {
      // reset the view state in case we had an error, but now we are navigating to a properly loaded and cached version
      // or a New Query.
      nextProps.resetViewState(nextProps.exploreViewState.get('viewId'));
      // also clear sql changes
      nextProps.setCurrentSql({sql: undefined});
    }

    const needsLoad = nextProps.dataset.get('needsLoad');
    const prevNeedsLoad = prevProps.dataset ? prevProps.dataset.get('needsLoad') : false;

    if (needsLoad && (needsLoad !== prevNeedsLoad || datasetChanged)) {
      //todo move viewId handling in handlePerformLoadDataset saga. See /dac/ui/src/sagas/performLoadDataset.js
      const {exploreViewState} = nextProps;
      const viewId = exploreViewState.get('viewId');
      nextProps.performLoadDataset(nextProps.dataset, viewId);
    }
  }

  shouldComponentUpdate(nextProps) {
    const propKeys = [
      'pageType', 'location', 'sqlState', 'sqlSize', 'rightTreeVisible', 'isResizeInProgress',
      'exploreViewState'
    ];

    return !nextProps.dataset.equals(this.props.dataset) || propKeys.some((key) => nextProps[key] !== this.props[key]);
  }

  toggleRightTree() {
    this.props.updateRightTreeVisibility(!this.props.rightTreeVisible);
  }

  _areLocationsSameDataset(history, oldLocation, newLocation) {

    // eg /space/myspace/path.to.dataset
    // Compare fullPath in pathname. Ignore prefix/suffix like /details
    const oldParts = oldLocation.pathname.split('/');
    const newParts = newLocation.pathname.split('/');
    if (newParts[2] === oldParts[2] && newParts[3] === oldParts[3]) return true;

    if (newParts[2] === 'tmp' && newParts[3] === 'UNTITLED') return true;
    const {version: newVersion } = newLocation.query || {};
    // special case to allow going back to previous version to handle back from New Query => physical dataset
    if (newVersion && history && newVersion === history.getIn(['items', 1])) {
      return true;
    }
    return false;
  }

  shouldShowUnsavedChangesPopup = (nextLocation) => {
    const {
      dataset,
      location,
      history,
      getDatasetChangeDetails
    } = this.props;

    if (this.discardUnsavedChangesConfirmed) {
      this.discardUnsavedChangesConfirmed = false;
      return false;
    }

    const {
      sqlChanged,
      historyChanged
    } = getDatasetChangeDetails();

    const {tipVersion: nextTipVersion, version: nextVersion} = nextLocation.query || {};
    const historyTipVersion = history && history.get('tipVersion');

    // Check if we are navigating within same history or this hook was called after saving dataset
    if (nextTipVersion && nextTipVersion === historyTipVersion) {
      // not actually leaving datasetVersion? eg moving to ./graph
      if (dataset.get('datasetVersion') === nextVersion) {
        return false;
      }
      return sqlChanged;
    }

    // Transforming navigates to the new version that is not in the current history yet,
    // so check if next location is related to current dataset, or new query.
    if (this._areLocationsSameDataset(history, location, nextLocation)) {
      return false;
    }

    if (sqlChanged) {
      return true;
    }
    return historyChanged;
  };

  didConfirmDiscardUnsavedChanges() {
    const { nextLocation } = this.state;
    this.discardUnsavedChangesConfirmed = true;
    if (nextLocation) {
      this.props.router.push(nextLocation);
    }
  }

  render() {
    const {
      pageType
    } = this.props;
    return (
      <div style={this.props.style}>
        <ExplorePage
          pageType={this.isPageTypeValid(pageType) ? pageType : defaultPageType}
          dataset={this.props.dataset}
          history={this.props.history}
          setResizeProgressState={this.props.setResizeProgressState}
          rightTreeVisible={this.props.rightTreeVisible}
          toggleRightTree={this.toggleRightTree}
          dragType={this.state.dragType}
          updateGridSizes={this.props.updateGridSizes}
          location={this.props.location}
          updateSqlPartSize={this.props.updateSqlPartSize}
          sqlState={this.props.sqlState}
          sqlSize={this.props.sqlSize}
          style={this.props.style}
          isResizeInProgress={this.props.isResizeInProgress}
        />
        <QlikStateModal />
      </div>
    );
  }
}

function mapStateToProps(state, ownProps) {
  const { location, routeParams } = ownProps;
  const isNewQuery = location.pathname === '/new_query';
  const dataset = getExplorePageDataset(state);
  const explorePageState = getExploreState(state);
  const sqlHeight = Math.min(explorePageState.ui.get('sqlSize'), domHelpers.ownerWindow().innerHeight - HEIGHT_AROUND_SQL_EDITOR);

  return {
    pageType: routeParams.pageType,
    dataset,
    history: getHistory(state, dataset.get('tipVersion')),
    // in New Query, force sql open, but don't change state in localStorage
    sqlState: explorePageState.ui.get('sqlState') || isNewQuery,
    sqlSize: sqlHeight,
    isResizeInProgress: explorePageState.ui.get('isResizeInProgress'),
    rightTreeVisible: state.ui.get('rightTreeVisible'),
    exploreViewState: getExploreViewState(state)
  };
}

export const ExplorePageController = withRouter(withRouteLeaveSubscription(ExplorePageControllerComponent));

const Connected = withHookProvider(connect(mapStateToProps, {
  performLoadDataset,
  setCurrentSql,
  resetViewState,
  updateSqlPartSize,
  updateGridSizes,
  setResizeProgressState,
  updateRightTreeVisibility,
  showConfirmationDialog
})(withDatasetChanges(ExplorePageController)));

export default moduleStateHOC(exploreStateKey, explore)(Connected);

