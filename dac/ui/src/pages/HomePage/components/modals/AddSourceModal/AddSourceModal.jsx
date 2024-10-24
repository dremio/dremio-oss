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
import Immutable from "immutable";
import { connect } from "react-redux";
import { compose } from "redux";

import { FormattedMessage, injectIntl } from "react-intl";

import ApiUtils from "utils/apiUtils/apiUtils";
import FormUtils from "utils/FormUtils/FormUtils";
import SourceFormJsonPolicy from "utils/FormUtils/SourceFormJsonPolicy";
import {
  createSampleSource,
  createSampleDbSource,
  createSource,
} from "actions/resources/sources";

import Modal from "components/Modals/Modal";
import ViewStateWrapper from "components/ViewStateWrapper";
import FormUnsavedWarningHOC from "components/Modals/FormUnsavedWarningHOC";

import SelectSourceType from "@inject/pages/HomePage/components/modals/AddSourceModal/SelectSourceType";
import ConfigurableSourceForm from "pages/HomePage/components/modals/ConfigurableSourceForm";
import { isCME } from "dyn-load/utils/versionUtils";
import { loadGrant } from "dyn-load/actions/resources/grant";
import EnginePreRequisiteHOC from "@inject/pages/HomePage/components/modals/PreviewEngineCheckHOC.tsx";

import AddSourceModalMixin, {
  mapStateToProps,
  additionalMapDispatchToProps,
} from "@inject/pages/HomePage/components/modals/AddSourceModal/AddSourceModalMixin";
import { processUiConfig } from "#oss/pages/HomePage/components/modals/EditSourceView";
import { passDataBetweenTabs } from "actions/modals/passDataBetweenTabs";
import { trimObjectWhitespace } from "pages/HomePage/components/modals/utils";
import * as classes from "./AddSourceModal.module.less";
import { isVersionedReflectionsEnabled } from "../AddEditSourceUtils";
import {
  isVersionedSource,
  getAddSourceModalTitle,
} from "@inject/utils/sourceUtils";
import { addProjectBase as wrapBackendLink } from "dremio-ui-common/utilities/projectBase.js";
import { isMetastoreSourceType } from "@inject/constants/sourceTypes";

const VIEW_ID = "ADD_SOURCE_MODAL";
const TIME_BEFORE_MESSAGE = 5000;

const DEFAULT_VLHF_LIST = require("customData/vlhfList");

@injectIntl
@AddSourceModalMixin
export class AddSourceModal extends Component {
  static contextTypes = {
    router: PropTypes.object.isRequired,
  };

  static propTypes = {
    location: PropTypes.object.isRequired,
    isOpen: PropTypes.bool,
    hide: PropTypes.func,
    source: PropTypes.object,
    updateFormDirtyState: PropTypes.func,
    createSource: PropTypes.func,
    initialFormValues: PropTypes.object,
    createSampleSource: PropTypes.func.isRequired,
    createSampleDbSource: PropTypes.func.isRequired,
    intl: PropTypes.object.isRequired,
    dispatchPassDataBetweenTabs: PropTypes.func,
    loadGrant: PropTypes.func,
  };

  state = {
    isTypeSelected: false,
    selectedFormType: {},
    isSubmitTakingLong: false,
    submitTimer: null,
    sourceTypes: [],
    isAddingSampleSource: false,
    didSourceTypeLoadFail: false,
    errorMessage: "Failed to load source list.",
  };

  componentDidMount() {
    this.isPreviewEngineRequired = false;
    if (this.props.location.state.selectedSourceType) {
      this.setStateWithSourceTypeConfigFromServer(
        this.props.location.state.selectedSourceType,
      );
    }

    this.setStateWithSourceTypeListFromServer();
    this.fetchData();
    if (isCME && !isCME()) {
      this.props.loadGrant(
        { name: "PUBLIC", type: "ROLE" },
        { viewId: VIEW_ID, entityClears: ["grant"] },
      );
    }
    this.sendAddStartEvent();
  }

  setStateWithSourceTypeListFromServer() {
    ApiUtils.fetchJson(
      "source/type",
      (result) => {
        const combinedListConfig =
          SourceFormJsonPolicy.combineDefaultAndLoadedList(
            result.data,
            DEFAULT_VLHF_LIST,
          );
        this.setState({ sourceTypes: combinedListConfig });
      },
      () => {
        this.setState({ didSourceTypeLoadFail: true });
      },
    );
  }

  setStateWithSourceTypeConfigFromServer(typeCode) {
    const { dispatchPassDataBetweenTabs } = this.props;
    return ApiUtils.fetchJson(
      `source/type/${typeCode}`,
      async (json) => {
        const combinedConfig = SourceFormJsonPolicy.getCombinedConfig(
          typeCode,
          processUiConfig(json),
          {
            reflectionsEnabled: isVersionedSource(typeCode)
              ? await isVersionedReflectionsEnabled()
              : true,
          },
        );
        const isFileSystemSource = combinedConfig.metadataRefresh;
        if (this.props.setPreviewEngine) {
          this.props.setPreviewEngine(json.previewEngineRequired, "Add");
          this.isPreviewEngineRequired = json.previewEngineRequired;
        }

        this.setState({
          isTypeSelected: true,
          selectedFormType: combinedConfig,
        });
        dispatchPassDataBetweenTabs({
          isFileSystemSource: isFileSystemSource.isFileSystemSource,
          isExternalQueryAllowed: json.externalQueryAllowed,
          isMetaStore: isMetastoreSourceType(combinedConfig.sourceType),
          sourceType: combinedConfig.sourceType,
        });
      },
      () => {
        this.setState({ didSourceTypeLoadFail: true });
      },
    );
  }

  UNSAFE_componentWillReceiveProps(nextProps) {
    if (!this.props.isOpen && nextProps.isOpen) {
      this.setState({ isTypeSelected: false, selectedFormType: {} });
      this.props.updateFormDirtyState(false); // mark form not dirty to avoid unsaved prompt
    }
  }

  hide = (...args) => {
    this.props.hide(...args);
  };

  getTitle() {
    const { intl } = this.props;
    /*eslint no-nested-ternary: "off"*/
    const title = getAddSourceModalTitle(
      this.props.location.state?.selectedSourceType,
    );
    if (title) {
      return title;
    }
    return this.state.isTypeSelected
      ? intl.formatMessage(
          { id: "Source.NewSourceStep2" },
          { sourceLabel: this.state.selectedFormType.label },
        )
      : intl.formatMessage({
          id: "Source.AddDataSource",
        });
  }

  handleSelectSource = (source) => {
    switch (source.sourceType) {
      case "SAMPLEDB":
        this.handleAddSampleDb();
        break;
      case "SampleSource":
        this.handleAddSampleSource();
        break;
      default:
        this.setStateWithSourceTypeConfigFromServer(source.sourceType);
    }
  };

  handleAddSampleDb = () => {
    return this.props.createSampleDbSource().then(() => {
      this.hide();
      return;
    });
  };

  handleAddSampleSource = () => {
    this.setState({ isAddingSampleSource: true });
    return this.props
      .createSampleSource({ viewId: VIEW_ID })
      .then((response) => {
        if (response && !response.error) {
          const nextSource = ApiUtils.getEntityFromResponse("source", response);
          this.context.router.push(nextSource.getIn(["links", "self"]));
        }
        this.setState({ isAddingSampleSource: false });
        this.hide();
        return null;
      });
  };

  startTrackSubmitTime = () => {
    const submitTimer = setTimeout(() => {
      this.setState({
        isSubmitTakingLong: true,
      });
    }, TIME_BEFORE_MESSAGE);
    this.setState({
      submitTimer,
    });
  };

  stopTrackSubmitTime = () => {
    clearTimeout(this.state.submitTimer);
    this.setState({
      isSubmitTakingLong: false,
      submitTimer: null,
    });
  };

  handleCustomSubmit = (values) => {
    const submitFn = this.handleAddSourceSubmit;
    const closeFn = this.hide;
    this.props.handleSubmit(values, submitFn, closeFn).catch((err) => {
      return err;
    });
  };

  handleAddSourceSubmit = (values) => {
    const data = trimObjectWhitespace(this.mutateFormValues(values), [
      "appSecret",
      "accessSecret",
      "password",
    ]);

    this.startTrackSubmitTime();

    return ApiUtils.attachFormSubmitHandlers(
      this.props.createSource(data, this.state.selectedFormType.sourceType),
    )
      .then((response) => {
        this.stopTrackSubmitTime();
        if (response && !response.error) {
          const nextSource = ApiUtils.getEntityFromResponse("source", response);
          this.context.router.push(
            wrapBackendLink(nextSource.getIn(["links", "self"])),
          );
        }
        this.sendAddCompleteEvent();
        return Promise.resolve("done");
      })
      .catch((error) => {
        this.stopTrackSubmitTime();
        throw error;
      });
  };

  renderLongSubmitLabel = () => {
    return this.state.isSubmitTakingLong ? (
      <span>
        <FormattedMessage id="Source.LargerSourcesWarning" />
      </span>
    ) : null;
  };

  render() {
    const { isOpen, updateFormDirtyState, location, initialFormValues } =
      this.props;
    const { state: { isExternalSource, isDataPlaneSource } = {} } = location;
    const { isAddingSampleSource, errorMessage, didSourceTypeLoadFail } =
      this.state;

    //HOC Props
    const {
      iconDisabled,
      hideCancel,
      confirmText,
      canSubmit,
      hasError,
      isSubmitting,
      confirmButtonStyle,
      onDismissError,
    } = this.props;

    const viewState =
      didSourceTypeLoadFail || hasError
        ? new Immutable.fromJS({
            isFailed: true,
            error: { message: hasError || errorMessage },
          })
        : new Immutable.Map({ isInProgress: isAddingSampleSource });

    return (
      <Modal
        size="large"
        title={this.getTitle(isExternalSource, isDataPlaneSource)}
        isOpen={isOpen}
        confirm={this.state.isTypeSelected && this.confirm}
        hide={!iconDisabled && this.hide}
        iconDisabled={iconDisabled}
        className={
          !this.state.isTypeSelected ? classes["add-source-modal"] : ""
        }
      >
        <ViewStateWrapper
          viewState={viewState}
          hideChildrenWhenFailed={false}
          onDismissError={onDismissError}
        >
          {!this.state.isTypeSelected ? (
            <SelectSourceType
              isExternalSource={isExternalSource}
              sourceTypes={this.state.sourceTypes}
              isDataPlaneSource={isDataPlaneSource}
              onSelectSource={this.handleSelectSource}
            />
          ) : (
            <ConfigurableSourceForm
              hideCancel={hideCancel}
              confirmText={confirmText}
              canSubmit={canSubmit}
              isSubmitting={isSubmitting}
              sourceFormConfig={this.state.selectedFormType}
              onFormSubmit={
                this.isPreviewEngineRequired
                  ? this.handleCustomSubmit
                  : this.handleAddSourceSubmit
              }
              onCancel={this.hide}
              updateFormDirtyState={updateFormDirtyState}
              footerChildren={this.renderLongSubmitLabel()}
              fields={FormUtils.getFieldsFromConfig(
                this.state.selectedFormType,
              )}
              validate={FormUtils.getValidationsFromConfig(
                this.state.selectedFormType,
              )}
              EntityType="source"
              initialValues={initialFormValues}
              confirmButtonStyle={confirmButtonStyle}
            />
          )}
        </ViewStateWrapper>
      </Modal>
    );
  }
}

export default compose(
  EnginePreRequisiteHOC,
  connect(mapStateToProps, {
    createSource,
    createSampleSource,
    createSampleDbSource,
    dispatchPassDataBetweenTabs: passDataBetweenTabs,
    loadGrant,
    ...additionalMapDispatchToProps,
  }),
)(FormUnsavedWarningHOC(AddSourceModal));
