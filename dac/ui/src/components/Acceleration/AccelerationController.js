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
import PropTypes from "prop-types";
import { connect } from "react-redux";
import Immutable from "immutable";

import sentryUtil from "@app/utils/sentryUtil";
import reflectionActions from "actions/resources/reflection";
import ApiPolling from "@app/utils/apiUtils/apiPollingUtils";
import { getViewState } from "selectors/resources";
import { loadDataset } from "actions/resources/dataset";
import ViewStateWrapper from "../ViewStateWrapper";
import AccelerationForm from "./AccelerationForm";

const VIEW_ID = "AccelerationModal";

export class AccelerationController extends Component {
  static propTypes = {
    isModal: PropTypes.bool,
    datasetId: PropTypes.string, // populated except during teardown
    dataset: PropTypes.instanceOf(Immutable.Map),
    reflections: PropTypes.instanceOf(Immutable.Map),
    canAlter: PropTypes.any,
    canSubmit: PropTypes.bool,
    location: PropTypes.object,

    getReflections: PropTypes.func.isRequired,
    getDataset: PropTypes.func.isRequired,

    onCancel: PropTypes.func,
    onDone: PropTypes.func,
    viewState: PropTypes.instanceOf(Immutable.Map),
    resetViewState: PropTypes.func,
    updateFormDirtyState: PropTypes.func,
  };

  constructor(props) {
    super(props);

    this.state = {
      canSubmit: true,
      getComplete: false, // need to track ourselves because viewState initial state looks the same as loaded-success
    };

    this.handleReflectionLoadFailure =
      this.handleReflectionLoadFailure.bind(this);
    this.handleReflectionLoadSuccess =
      this.handleReflectionLoadSuccess.bind(this);
    this.reloadReflections = this.reloadReflections.bind(this);
  }

  isUnmounting = false;
  restartPollingTimeoutInt;

  componentDidMount() {
    this.loadReflections();
  }

  componentDidUpdate(prevProps) {
    if (prevProps.datasetId !== this.props.datasetId) {
      this.loadReflections();
    }
  }

  componentWillUnmount() {
    clearTimeout(this.restartPollingTimeoutInt);
    this.isUnmounting = true;
  }

  handleReflectionLoadFailure = (error) => {
    const failedErrorLog = sentryUtil.logException(error);
    if (failedErrorLog) {
      console.error(
        "An error has occurred while making a call in the acceleration controller:",
        error
      ); //specifically for instances of logErrorsToSentry & outsideCommunicationDisabled not allowing a sentry error log to be created
    }
    if (error.payload instanceof Error) return; // legacy error handling
  };

  handleReflectionLoadSuccess = () => {
    // because reflections can continuously change their status, such as expiring after they were successful, the stop point for polling is removed DX-37046
    if (!document.hidden && !this.isUnmounting) {
      return false;
    } else {
      if (!this.isUnmounting) {
        this.restartPolling();
      }
      return true;
    }
  };

  restartPolling = () => {
    clearTimeout(this.restartPollingTimeoutInt);
    this.restartPollingTimeoutInt = setTimeout(() => {
      if (document.hidden) {
        this.restartPolling();
      } else {
        this.reloadReflections();
      }
    }, 5000);
  };

  makeReflectionsCall() {
    const { datasetId, getReflections, isModal = true, location } = this.props;
    const { modal } = location?.state || {};

    // only refreshes a dataset's reflection info if the modal is open
    // exception to this is the Reflections tab in the Explore Page since it doesn't have a modal
    if (modal || !isModal) {
      return getReflections(
        { viewId: VIEW_ID },
        { path: `dataset/${encodeURIComponent(datasetId)}/reflection` }
      );
    } else {
      return false;
    }
  }

  loadReflections = () => {
    const { datasetId, getDataset } = this.props;
    if (!datasetId) return;
    return this.makeReflectionsCall()
      .then((response) => {
        if (response && response.payload instanceof Error) return;
        const noReflectionsStillUpdating =
          this.handleReflectionLoadSuccess(response);
        if (!noReflectionsStillUpdating) {
          this.reloadReflections(5);
        }
        return getDataset(datasetId, VIEW_ID);
      })
      .then(() => this.setState({ getComplete: true }));
  };

  reloadReflections = () => {
    const { datasetId } = this.props;
    if (!datasetId) return;

    ApiPolling({
      apiCallFunc: () => this.makeReflectionsCall(),
      handleFailure: this.handleReflectionLoadFailure,
      handleSuccess: this.handleReflectionLoadSuccess,
      intervalSec: 5,
      timeoutSec: 600,
    });
  };

  handleSubmitSuccess = () => {
    this.props.onDone(null, true);
    // future: stay open with refresh (is that even needed?), OR close (user option)
  };

  renderContent() {
    const {
      viewState,
      reflections,
      dataset,
      onCancel,
      isModal = true,
      canAlter,
      canSubmit,
    } = this.props;

    if (!this.state.getComplete || viewState.get("isFailed")) {
      return null; // AccelerationForm expects to only be created after data is ready
    }

    if (!dataset || !reflections) return null; // teardown guard

    return (
      <AccelerationForm
        canAlter={canAlter}
        canSubmit={canSubmit}
        isModal={isModal}
        updateFormDirtyState={this.props.updateFormDirtyState}
        onCancel={onCancel}
        onSubmitSuccess={this.handleSubmitSuccess}
        dataset={dataset}
        reflections={reflections}
        reloadReflections={this.reloadReflections}
      />
    );
  }

  render() {
    const { viewState } = this.props;

    return (
      <ViewStateWrapper viewState={viewState} hideSpinner={true}>
        {this.renderContent()}
      </ViewStateWrapper>
    );
  }
}

function mapStateToProps(state, ownProps) {
  const reflections = state.resources.entities.get("reflection")
    ? state.resources.entities.get("reflection").filter((reflection) => {
        return reflection.get("datasetId") === ownProps.datasetId;
      })
    : new Immutable.Map();

  const canAlter =
    state.resources.entities.get("reflection") &&
    state.resources.entities.getIn(["reflection", "canAlter"]);

  const dataset =
    state.resources.entities.get("dataset") &&
    state.resources.entities.get("dataset").get(ownProps.datasetId);

  let viewState = getViewState(state, VIEW_ID);
  if (typeof dataset !== "undefined" && !dataset.has("fields")) {
    viewState = Immutable.fromJS({
      isFailed: true,
      error: {
        message: la("Dataset missing schema information."),
      },
    });
  }

  return {
    canAlter,
    reflections,
    dataset,
    viewState,
  };
}

export default connect(mapStateToProps, {
  getReflections: reflectionActions.getList.dispatch,
  getDataset: loadDataset,
})(AccelerationController);
