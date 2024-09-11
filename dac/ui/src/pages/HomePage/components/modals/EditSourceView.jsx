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
import { connect } from "react-redux";
import Immutable from "immutable";
import mergeWith from "lodash/mergeWith";

import {
  createSource,
  loadSource,
  removeSource,
  updateSourcePrivileges,
} from "actions/resources/sources";
import sourcesMapper from "utils/mappers/sourcesMapper";
import ApiUtils from "utils/apiUtils/apiUtils";
import FormUtils from "utils/FormUtils/FormUtils";
import SourceFormJsonPolicy from "utils/FormUtils/SourceFormJsonPolicy";
import { showConfirmationDialog } from "actions/confirmation";
import { passDataBetweenTabs } from "actions/modals/passDataBetweenTabs";
import ViewStateWrapper from "components/ViewStateWrapper";
import Message from "components/Message";
import ConfigurableSourceForm from "pages/HomePage/components/modals/ConfigurableSourceForm";

import EditSourceViewMixin, {
  mapStateToProps,
  additionalMapDispatchToProps,
  getFinalSubmit,
  ENABLE_USE_LEGACY_DIALECT_OPTION,
} from "@inject/pages/HomePage/components/modals/EditSourceViewMixin";
import { USE_LEGACY_DIALECT_PROPERTY_NAME } from "@app/constants/sourceTypes";

import { viewStateWrapper } from "uiTheme/less/forms.less";
import { trimObjectWhitespace } from "./utils";
import { isVersionedReflectionsEnabled } from "./AddEditSourceUtils";
import { isVersionedSource } from "@app/utils/sourceUtils";
import { getJSONElementOverrides } from "@inject/utils/FormUtils/formOverrideUtils";
import clsx from "clsx";
import * as classes from "./EditSourceView.module.less";

export const VIEW_ID = "EditSourceView";

export const processUiConfig = (uiConfig) => {
  if (!uiConfig || !uiConfig.elements) return uiConfig;
  let elements = uiConfig.elements;
  if (!ENABLE_USE_LEGACY_DIALECT_OPTION) {
    elements = elements.filter(
      (el) => el.propertyName !== USE_LEGACY_DIALECT_PROPERTY_NAME,
    );
  }
  elements = getJSONElementOverrides(uiConfig.elements, uiConfig.sourceType);
  return {
    ...uiConfig,
    elements,
  };
};

@EditSourceViewMixin
export class EditSourceView extends PureComponent {
  static propTypes = {
    sourceName: PropTypes.string.isRequired,
    sourceType: PropTypes.string.isRequired,
    hide: PropTypes.func.isRequired,
    createSource: PropTypes.func.isRequired,
    updateSourcePrivileges: PropTypes.func,
    removeSource: PropTypes.func.isRequired,
    loadSource: PropTypes.func,
    messages: PropTypes.instanceOf(Immutable.List),
    viewState: PropTypes.instanceOf(Immutable.Map),
    source: PropTypes.instanceOf(Immutable.Map),
    initialFormValues: PropTypes.object,
    updateFormDirtyState: PropTypes.func,
    showConfirmationDialog: PropTypes.func,
    dispatchPassDataBetweenTabs: PropTypes.func,
    isSchedulerEnabled: PropTypes.bool,
    isLiveReflectionsEnabled: PropTypes.bool,
  };

  constructor() {
    super();
    this.state = {
      isConfigLoaded: false,
      selectedFormType: {},
      didLoadFail: false,
      errorMessage: "Failed to load source configuration.",
    };
  }

  static contextTypes = {
    location: PropTypes.object.isRequired,
    router: PropTypes.object.isRequired,
  };

  async componentDidMount() {
    this.isPreviewEngineRequired = false;
    const { sourceName, sourceType } = this.props;
    await this.props.loadSource(sourceName, VIEW_ID);
    this.fetchData();
    this.setStateWithSourceTypeConfigFromServer(sourceType);
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
          this.props.setPreviewEngine(json.previewEngineRequired, "Edit");
          this.isPreviewEngineRequired = json.previewEngineRequired;
        }

        this.setState({
          isTypeSelected: true,
          isConfigLoaded: true,
          selectedFormType: combinedConfig,
        });
        dispatchPassDataBetweenTabs({
          isFileSystemSource: isFileSystemSource.isFileSystemSource,
          isExternalQueryAllowed: json.externalQueryAllowed,
          isHive:
            combinedConfig.sourceType === "HIVE3" ||
            combinedConfig.sourceType === "HIVE",
          isGlue: combinedConfig.sourceType === "AWSGLUE",
        });
      },
      () => {
        this.setState({ didLoadFail: true });
      },
    );
  }

  reallySubmitEdit = (form, sourceType) => {
    if (!this.props.isSchedulerEnabled) {
      delete form.accelerationActivePolicyType;
      delete form.accelerationRefreshSchedule;
    }
    if (!this.props.isLiveReflectionsEnabled) {
      delete form.accelerationRefreshOnDataChanges;
    }
    return ApiUtils.attachFormSubmitHandlers(
      getFinalSubmit(form, sourceType, this.props),
    ).then(() => {
      this.context.router.replace(window.location.pathname);
      return null;
    });
  };

  checkIsMetadataImpacting = (sourceModel) => {
    return ApiUtils.fetch(
      "sources/isMetadataImpacting",
      {
        method: "POST",
        body: JSON.stringify(sourceModel),
      },
      2,
    )
      .then((response) => response.json())
      .catch((response) => {
        return response.json().then((error) => {
          throw error;
        });
      });
  };

  handleCustomSubmit = (values) => {
    const submitFn = this.submitEdit;
    const closeFn = this.props.hide;
    this.props.handleSubmit(values, submitFn, closeFn).catch((err) => {
      return err;
    });
  };

  submitEdit = (form) => {
    const { sourceType } = this.props;

    const formData = trimObjectWhitespace(this.mutateFormValues(form), [
      "appSecret",
      "accessSecret",
      "password",
    ]);

    return ApiUtils.attachFormSubmitHandlers(
      new Promise((resolve, reject) => {
        const sourceModel = sourcesMapper.newSource(sourceType, formData);
        this.checkIsMetadataImpacting(sourceModel)
          .then((data) => {
            if (data && data.isMetadataImpacting) {
              this.props.showConfirmationDialog({
                title: laDeprecated("Warning"),
                text: laDeprecated(
                  "You made a metadata impacting change.  This change will cause Dremio to clear permissions, formats and reflections on all datasets in this source.",
                ),
                confirmText: laDeprecated("Confirm"),
                dataQa: "metadata-impacting",
                confirm: () => {
                  this.reallySubmitEdit(formData, sourceType)
                    .then(resolve)
                    .catch(reject);
                },
                cancel: reject,
              });
            } else {
              this.reallySubmitEdit(formData, sourceType)
                .then(resolve)
                .catch(reject);
            }
            return null;
          })
          .catch(reject);
      }),
    );
  };

  renderMessages() {
    const { messages } = this.props;
    return (
      messages &&
      messages.map((message, i) => {
        const type =
          message.get("level") === "WARN"
            ? "warning"
            : message.get("level").toLowerCase();
        return (
          <Message
            messageType={type}
            message={message.get("message")}
            messageId={i.toString()}
            key={i.toString()}
            style={{ width: "100%" }}
          />
        );
      })
    );
  }

  render() {
    const { didLoadFail } = this.state;
    const {
      updateFormDirtyState,
      initialFormValues,
      source,
      sourceType,
      viewState,
      hide,
    } = this.props;

    //HOC Props
    const {
      hideCancel,
      hasError,
      confirmButtonStyle,
      confirmText,
      canSubmit,
      isSubmitting,
      onDismissError,
    } = this.props;

    const initValues = mergeWith(
      source && source.size > 1 ? source.toJS() : {},
      initialFormValues,
      arrayAsPrimitiveMerger,
    );

    let vs = viewState;

    if (didLoadFail || hasError) {
      vs = new Immutable.fromJS({
        isFailed: true,
        // eslint-disable-next-line no-undef
        error: { message: hasError || errorMessage },
      });
    }

    return (
      <ViewStateWrapper
        viewState={vs}
        className={clsx(viewStateWrapper, classes["edit-source-form"])}
        hideChildrenWhenFailed={false}
        onDismissError={onDismissError}
      >
        {this.renderMessages()}
        {this.state.isConfigLoaded && (
          <ConfigurableSourceForm
            hideCancel={hideCancel}
            confirmText={confirmText}
            canSubmit={canSubmit}
            isSubmitting={isSubmitting}
            sourceFormConfig={this.state.selectedFormType}
            onFormSubmit={
              this.isPreviewEngineRequired
                ? this.handleCustomSubmit
                : this.submitEdit
            }
            onCancel={hide}
            key={sourceType}
            editing
            updateFormDirtyState={updateFormDirtyState}
            fields={FormUtils.getFieldsFromConfig(this.state.selectedFormType)}
            validate={FormUtils.getValidationsFromConfig(
              this.state.selectedFormType,
            )}
            initialValues={initValues}
            permissions={source.get("permissions")}
            EntityType="source"
            confirmButtonStyle={confirmButtonStyle}
          />
        )}
      </ViewStateWrapper>
    );
  }
}

export default connect(mapStateToProps, {
  loadSource,
  createSource,
  updateSourcePrivileges,
  removeSource,
  showConfirmationDialog,
  dispatchPassDataBetweenTabs: passDataBetweenTabs,
  ...additionalMapDispatchToProps,
})(EditSourceView);

function arrayAsPrimitiveMerger(objValue, srcValue) {
  if (Array.isArray(objValue)) {
    return srcValue;
  }
}
