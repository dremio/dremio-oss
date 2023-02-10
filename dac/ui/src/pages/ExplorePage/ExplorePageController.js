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
import { Component, Fragment } from "react";
import { compose } from "redux";
import PropTypes from "prop-types";
import Immutable from "immutable";
import { connect } from "react-redux";
import { withRouter } from "react-router";
import domHelpers from "dom-helpers";

import { getExploreViewState } from "selectors/resources";
import { getActiveScript } from "@app/selectors/scripts";
import { moduleStateHOC } from "@app/containers/ModuleStateContainer";
import explore from "@app/reducers/explore";
import {
  getHistory,
  exploreStateKey,
  getExploreState,
  getExplorePageDataset,
} from "selectors/explore";
import { performLoadDataset } from "actions/explore/dataset/get";
import { resetViewState } from "actions/resources";
import { withDatasetChanges } from "@app/pages/ExplorePage/DatasetChanges";
import {
  withRouteLeaveSubscription,
  withRouteLeaveEvent,
} from "@app/containers/RouteLeave.js";
import { initRefs as initRefsAction } from "@app/actions/nessie/nessie";
import exploreUtils from "@app/utils/explore/exploreUtils";
import { updateSqlPartSize } from "actions/explore/ui";
import { showConfirmationDialog } from "actions/confirmation";
import { updateRightTreeVisibility } from "actions/ui/ui";
import { hasDatasetChanged } from "utils/datasetUtils";
import { PageTypes, pageTypeValuesSet } from "@app/pages/ExplorePage/pageTypes";
import explorePageControllerConfig from "@inject/pages/ExplorePage/explorePageControllerConfig";
import QlikStateModal from "./components/modals/QlikStateModal";
import ExplorePage from "./ExplorePage";
import * as commonPaths from "dremio-ui-common/paths/common.js";
import { getSonarContext } from "dremio-ui-common/contexts/SonarContext.js";
import { rmProjectBase } from "dremio-ui-common/utilities/projectBase.js";
import { newQuery } from "@app/exports/paths";
import { isDcsEdition } from "dyn-load/utils/versionUtils";
import { fetchFeatureFlag } from "@inject/actions/featureFlag";
import { SQL_JOB_STATUS } from "@app/exports/flags/SQL_JOB_STATUS";

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
    isResizeInProgress: PropTypes.bool,
    updateSqlPartSize: PropTypes.func.isRequired,
    exploreViewState: PropTypes.instanceOf(Immutable.Map),
    performLoadDataset: PropTypes.func.isRequired,
    resetViewState: PropTypes.func.isRequired,
    showConfirmationDialog: PropTypes.func,
    router: PropTypes.object,
    addHasChangesHook: PropTypes.func, // (hasChangesCallback[: (nextLocation) => bool]) => void
    initRefs: PropTypes.func,
    // provided by withDatasetChanges
    getDatasetChangeDetails: PropTypes.func.isRequired,
    activeScript: PropTypes.object,
    currentSql: PropTypes.string,
    fetchFeatureFlag: PropTypes.func,
  };

  static defaultProps = {
    pageType: defaultPageType,
  };

  discardUnsavedChangesConfirmed = false;

  constructor(props) {
    super(props);
    this.toggleRightTree = this.toggleRightTree.bind(this);
    this.state = {
      dragType: "groupBy",
      accessModalState: false,
      nextLocation: null,
      isUnsavedChangesModalShowing: false,
    };
  }

  componentDidMount() {
    const { addHasChangesHook, initRefs } = this.props;
    initRefs(); //Initialize Nessie references to current browsing context before load
    this.receiveProps(this.props);
    if (addHasChangesHook) {
      addHasChangesHook(this.shouldShowUnsavedChangesPopup);
    }

    isDcsEdition() && this.props.fetchFeatureFlag(SQL_JOB_STATUS);
  }

  componentWillReceiveProps(nextProps) {
    this.receiveProps(nextProps, this.props);
  }

  isPageTypeValid(pageType) {
    return pageTypeValuesSet.has(pageType);
  }

  receiveProps(nextProps, prevProps = {}) {
    if (!this.isPageTypeValid(nextProps.pageType)) {
      const projectId = getSonarContext()?.getSelectedProjectId?.();
      nextProps.router.push(commonPaths.projectBase.link({ projectId }));
    }

    const datasetChanged = hasDatasetChanged(
      nextProps.dataset,
      prevProps.dataset
    );
    if (datasetChanged) {
      // reset the view state in case we had an error, but now we are navigating to a properly loaded and cached version
      // or a New Query.
      nextProps.resetViewState(nextProps.exploreViewState.get("viewId"));
    }

    const needsLoad = nextProps.dataset.get("needsLoad");
    const prevNeedsLoad = prevProps.dataset
      ? prevProps.dataset.get("needsLoad")
      : false;
    const { runPreviewOnDatasetSelect } = explorePageControllerConfig;
    if (
      runPreviewOnDatasetSelect &&
      needsLoad &&
      (needsLoad !== prevNeedsLoad || datasetChanged)
    ) {
      //todo move viewId handling in handlePerformLoadDataset saga. See /dac/ui/src/sagas/performLoadDataset.js
      const { exploreViewState, location } = nextProps;
      const viewId = exploreViewState.get("viewId");

      // Only execute the table results when opening from the job list page
      const isOpenResults = location?.query?.openResults;
      nextProps.performLoadDataset(nextProps.dataset, viewId, !!isOpenResults);
    }
  }

  shouldComponentUpdate(nextProps) {
    const propKeys = [
      "pageType",
      "location",
      "sqlState",
      "sqlSize",
      "rightTreeVisible",
      "isResizeInProgress",
      "exploreViewState",
    ];

    return (
      !nextProps.dataset.equals(this.props.dataset) ||
      propKeys.some((key) => nextProps[key] !== this.props[key])
    );
  }

  toggleRightTree() {
    this.props.updateRightTreeVisibility(!this.props.rightTreeVisible);
  }

  _areLocationsSameDataset(history, oldLocation, newLocation) {
    // eg /space/myspace/path.to.dataset
    // Compare fullPath in pathname. Ignore prefix/suffix like /details
    // urlability
    const newloc = rmProjectBase(newLocation.pathname);
    const oldLoc = rmProjectBase(oldLocation.pathname);

    const oldParts = oldLoc.split("/");
    const newParts = newloc.split("/");
    const hasMoreParts = newParts.length > 2 || oldParts.length > 2;
    if (hasMoreParts) {
      if (newParts[2] === oldParts[2] && newParts[3] === oldParts[3])
        return true;

      if (newParts[2] === "tmp" && newParts[3] === "UNTITLED") return true;
      const { version: newVersion } = newLocation.query || {};
      // special case to allow going back to previous version to handle back from New Query => physical dataset
      if (newVersion && history && newVersion === history.getIn(["items", 1])) {
        return true;
      }
    }
    return false;
  }

  shouldShowUnsavedChangesPopup = (nextLocation) => {
    const {
      dataset,
      location,
      history,
      getDatasetChangeDetails,
      activeScript,
    } = this.props;

    if (this.discardUnsavedChangesConfirmed) {
      this.discardUnsavedChangesConfirmed = false;
      return false;
    }

    const { sqlChanged, historyChanged } = getDatasetChangeDetails();

    const { tipVersion: nextTipVersion, version: nextVersion } =
      nextLocation.query || {};
    const historyTipVersion = history && history.get("tipVersion");
    const isDiscard = nextLocation.state && nextLocation.state.discard;
    const goToSqlRunner =
      nextLocation.state && nextLocation.state.renderScriptTab;

    if (isDiscard) {
      return false;
    }

    // Check if we are navigating within same history or this hook was called after saving dataset
    if (nextTipVersion && nextTipVersion === historyTipVersion) {
      // not actually leaving datasetVersion? eg moving to ./graph
      if (dataset.get("datasetVersion") === nextVersion) {
        return false;
      }
      return sqlChanged;
    }

    // Transforming navigates to the new version that is not in the current history yet,
    // so check if next location is related to current dataset, or new query.
    if (this._areLocationsSameDataset(history, location, nextLocation)) {
      return false;
    }

    if (goToSqlRunner && exploreUtils.isExploreDatasetPage(location)) {
      return false;
    }

    if (sqlChanged && !activeScript.id) {
      return true;
    }

    if (activeScript.id) {
      return activeScript.content !== this.props.currentSql;
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
      pageType,
      dataset,
      rightTreeVisible,
      location,
      updateSqlPartSize: updateSqlPartSizeFn,
      sqlState,
      sqlSize,
      isResizeInProgress,
    } = this.props;
    const nextPageType = this.isPageTypeValid(pageType)
      ? pageType
      : defaultPageType;

    return (
      <Fragment>
        <ExplorePage
          pageType={nextPageType}
          dataset={dataset}
          rightTreeVisible={rightTreeVisible}
          toggleRightTree={this.toggleRightTree}
          dragType={this.state.dragType}
          location={location}
          updateSqlPartSize={updateSqlPartSizeFn}
          sqlState={sqlState}
          sqlSize={sqlSize}
          isResizeInProgress={isResizeInProgress}
        />
        <QlikStateModal />
      </Fragment>
    );
  }
}

function mapStateToProps(state, ownProps) {
  const { location, routeParams } = ownProps;
  const isNewQuery = location.pathname.includes(newQuery());
  const dataset = getExplorePageDataset(state);
  const explorePageState = getExploreState(state);
  const sqlHeight = Math.min(
    explorePageState.ui.get("sqlSize"),
    domHelpers.ownerWindow().innerHeight - HEIGHT_AROUND_SQL_EDITOR
  );

  return {
    pageType: routeParams.pageType,
    dataset,
    history: getHistory(state, dataset.get("tipVersion")),
    // in New Query, force sql open, but don't change state in localStorage
    sqlState: explorePageState.ui.get("sqlState") || isNewQuery,
    sqlSize: sqlHeight,
    isResizeInProgress: explorePageState.ui.get("isResizeInProgress"),
    rightTreeVisible: state.ui.get("rightTreeVisible"),
    exploreViewState: getExploreViewState(state),
    activeScript: getActiveScript(state),
    currentSql: explorePageState.view.currentSql,
  };
}

export const ExplorePageController = withRouter(
  withRouteLeaveSubscription(ExplorePageControllerComponent)
);

const Connected = compose(
  withRouteLeaveEvent,
  connect(mapStateToProps, {
    performLoadDataset,
    resetViewState,
    updateSqlPartSize,
    updateRightTreeVisibility,
    showConfirmationDialog,
    initRefs: initRefsAction,
    fetchFeatureFlag,
  }),
  withDatasetChanges
)(ExplorePageController);

export default moduleStateHOC(exploreStateKey, explore)(Connected);
