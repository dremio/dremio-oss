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
import $ from 'jquery';

import { getViewState, getEntity } from 'selectors/resources';
import { getDataset, getHistory } from 'selectors/explore';
import { performLoadDataset } from 'actions/explore/dataset/get';
import { setCurrentSql } from 'actions/explore/view';
import { resetViewState } from 'actions/resources';
import { withDatasetChanges } from '@app/pages/ExplorePage/DatasetChanges';

import {
  updateSqlPartSize
} from 'actions/explore/ui';

import { showConfirmationDialog } from 'actions/confirmation';

import { setResizeProgressState } from 'actions/explore/ui';

import { updateGridSizes, updateRightTreeVisibility } from 'actions/ui/ui';

import { hasDatasetChanged } from 'utils/datasetUtils';
import { constructFullPathAndEncode, splitFullPath } from 'utils/pathUtils';

import { EXPLORE_VIEW_ID } from 'reducers/explore/view'; // NOTE: typically want exploreViewState.get('viewId')

import QlikStateModal from './components/modals/QlikStateModal';

import ExplorePage from './ExplorePage';

const HEIGHT_OF_AREA_MARGIN = 160;

export const PAGE_TYPES = new Set(['default', 'details', 'graph']);

export class ExplorePageControllerComponent extends Component {
  static propTypes = {
    pageType: PropTypes.string,
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
    initialDatasetVersion: PropTypes.string,
    updateSqlPartSize: PropTypes.func.isRequired,
    exploreViewState: PropTypes.instanceOf(Immutable.Map),
    performLoadDataset: PropTypes.func.isRequired,
    setCurrentSql: PropTypes.func.isRequired,
    resetViewState: PropTypes.func.isRequired,
    style: PropTypes.object,
    showConfirmationDialog: PropTypes.func,
    router: PropTypes.object,
    // provided by withDatasetChanges
    getDatasetChangeDetails: PropTypes.func.isRequired
  };

  static defaultProps = {
    pageType: 'default'
  };

  discardUnsavedChangesConfirmed = false;

  constructor(props) {
    super(props);
    this.toggleRightTree = this.toggleRightTree.bind(this);
    const height = $('#root').height() - HEIGHT_OF_AREA_MARGIN;
    this.state = {
      tableHeight: height,
      dragType: 'groupBy',
      accessModalState: false,
      nextLocation: null,
      isUnsavedChangesModalShowing: false
    };
  }

  componentDidMount() {
    this.receiveProps(this.props);
  }

  componentWillReceiveProps(nextProps) {
    this.receiveProps(nextProps, this.props);
  }

  receiveProps(nextProps, prevProps = {}) {
    if (!PAGE_TYPES.has(nextProps.pageType)) {
      nextProps.router.push('/');
    }

    if (nextProps.route.path !== (prevProps.route && prevProps.route.path)) {
      this.props.router.setRouteLeaveHook(nextProps.route, this.routeWillLeave.bind(this));
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

  routeWillLeave(nextLocation) {
    const { isUnsavedChangesModalShowing } = this.state;
    if (this.shouldShowUnsavedChangesPopup(nextLocation) && !isUnsavedChangesModalShowing) {
      this.setState({nextLocation});
      this.setState({isUnsavedChangesModalShowing: true});

      this.props.showConfirmationDialog({
        title: la('Unsaved Changes Warning'),
        text: [
          la('Performing this action will cause you to lose changes applied to the dataset.'),
          la('Are you sure you want to continue?')
        ],
        confirmText: la('Continue'),
        cancelText: la('Cancel'),
        confirm: () => {
          this.setState({isUnsavedChangesModalShowing: false}, () => {
            this.didConfirmDiscardUnsavedChanges();
          });
        },
        cancel: () => {
          this.setState({isUnsavedChangesModalShowing: false});
        }
      });

      return false;
    }
    return true;
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

  shouldShowUnsavedChangesPopup(nextLocation) {
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
  }

  didConfirmDiscardUnsavedChanges() {
    const { nextLocation } = this.state;
    this.discardUnsavedChangesConfirmed = true;
    if (nextLocation) {
      this.props.router.push(nextLocation);
    }
  }

  render() {
    return (
      <div style={this.props.style}>
        <ExplorePage
          pageType={this.props.pageType}
          dataset={this.props.dataset}
          history={this.props.history}
          setResizeProgressState={this.props.setResizeProgressState}
          rightTreeVisible={this.props.rightTreeVisible}
          toggleRightTree={this.toggleRightTree}
          dragType={this.state.dragType}
          tableHeight={this.state.tableHeight}
          updateGridSizes={this.props.updateGridSizes}
          location={this.props.location}
          updateSqlPartSize={this.props.updateSqlPartSize}
          sqlState={this.props.sqlState}
          sqlSize={this.props.sqlSize}
          style={this.props.style}
          isResizeInProgress={this.props.isResizeInProgress}
          exploreViewState={this.props.exploreViewState}
        />
        <QlikStateModal />
      </div>
    );
  }
}

export function getNewDataset(location) {
  return Immutable.fromJS({
    isNewQuery: true,
    fullPath: ['tmp', 'UNTITLED'],
    displayFullPath: ['tmp', 'New Query'],
    //have to decode a context parameter. This should be consistent with NewQueryButton.getNewQueryHref
    context: location.query && location.query.context ? splitFullPath(location.query.context).map(decodeURIComponent) : [],
    sql: '',
    datasetType: 'VIRTUAL_DATASET',
    apiLinks: {
      self: '/dataset/tmp/UNTITLED/new_untitled_sql'
    }
  });
}

function getInitialDataset(location, routeParams, viewState) {
  const version = location.query.version;
  const displayFullPath = viewState.getIn(['error', 'details', 'displayFullPath']) ||
    [...splitFullPath(routeParams.resourceId), ...splitFullPath(routeParams.tableId)];
  const fullPath = location.query.mode === 'edit' ? displayFullPath : ['tmp', 'UNTITLED'];

  return Immutable.fromJS({
    fullPath,
    displayFullPath,
    sql: viewState.getIn(['error', 'details', 'sql']) || '',
    context: viewState.getIn(['error', 'details', 'context']) || [],
    datasetVersion: version,
    datasetType: viewState.getIn(['error', 'details', 'datasetType']),
    links: {
      self: location.pathname + '?version=' + version
    },
    apiLinks: {
      self: `/dataset/${constructFullPathAndEncode(fullPath)}` + (version ? `/version/${version}` : '')
    }
  });
}

function getExploreViewState(state, jobId) {
  // Runs each get their own viewId so you can navigate away and back and see it's still in progress
  if (jobId) {
    return getViewState(state, 'run-' + jobId);
  }
  return getViewState(state, EXPLORE_VIEW_ID);
}

function mapStateToProps(state, ownProps) {
  const { location, routeParams } = ownProps;
  const isNewQuery = location.pathname === '/new_query';
  const { query } = location || {};
  const { jobId } = query;
  const exploreViewState = getExploreViewState(state, jobId);

  let dataset;
  let needsLoad = false;


  if (isNewQuery) {
    dataset = getNewDataset(location);
  } else {
    dataset = getDataset(state, query.version);

    if (dataset) {
      const fullDataset = getEntity(state, query.version, 'fullDataset');
      if (fullDataset && fullDataset.get('error')) {
        needsLoad = true;
      }
    } else {
      needsLoad = true;
      dataset = getInitialDataset(location, routeParams, exploreViewState);
    }
  }
  dataset = dataset.set('needsLoad', needsLoad);

  if (query.jobId) {
    dataset = dataset.set('jobId', query.jobId);
  }

  dataset = dataset.set('tipVersion', query.tipVersion || dataset.get('datasetVersion'));

  return {
    pageType: routeParams.pageType,
    dataset,
    history: getHistory(state, dataset.get('tipVersion')),
    // in New Query, force sql open, but don't change state in localStorage
    sqlState: state.explore.ui.get('sqlState') || isNewQuery,
    sqlSize: state.explore.ui.get('sqlSize'),
    isResizeInProgress: state.explore.ui.get('isResizeInProgress'),
    initialDatasetVersion: state.explore.ui.get('initialDatasetVersion'),
    rightTreeVisible: state.ui.get('rightTreeVisible'),
    exploreViewState
  };
}

export const ExplorePageController = withRouter(ExplorePageControllerComponent);

export default connect(mapStateToProps, {
  performLoadDataset,
  setCurrentSql,
  resetViewState,
  updateSqlPartSize,
  updateGridSizes,
  setResizeProgressState,
  updateRightTreeVisibility,
  showConfirmationDialog
})(withDatasetChanges(ExplorePageController));
