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
import Immutable from "immutable";
import PropTypes from "prop-types";
import FormUtils from "utils/FormUtils/FormUtils";
import { change } from "redux-form";

import { FormBody, ModalForm, modalFormProps } from "components/Forms";
import { connectComplexForm } from "components/Forms/connectComplexForm";
import FormTab from "components/Forms/FormTab";

import NavPanel from "components/Nav/NavPanel";
import { getFormTabs } from "@inject/pages/HomePage/components/modals/utils";
import { sourceFormWrapper } from "uiTheme/less/forms.less";
import { scrollRightContainerWithHeader } from "uiTheme/less/layout.less";
import { FormContext } from "./formContext";

const SOURCE_FIELDS = [
  "id",
  "name",
  "description",
  "allowCrossSourceSelection",
  "disableMetadataValidityCheck",
];

const FORM_NAME = "source";

class ConfigurableSourceForm extends Component {
  static propTypes = {
    onFormSubmit: PropTypes.func.isRequired,
    onCancel: PropTypes.func.isRequired,
    handleSubmit: PropTypes.func.isRequired,
    handleChangeTab: PropTypes.func.isRequired,
    selectedTabName: PropTypes.string,
    navTabs: PropTypes.instanceOf(Immutable.OrderedMap),
    editing: PropTypes.bool,
    fields: PropTypes.object,
    sourceFormConfig: PropTypes.object,
    footerChildren: PropTypes.node,
    EntityType: PropTypes.string,
    permissions: PropTypes.object,
    dispatch: PropTypes.any,
  };

  render() {
    const {
      fields,
      handleSubmit,
      onFormSubmit,
      sourceFormConfig,
      handleChangeTab,
      navTabs,
      selectedTabName,
      footerChildren,
      permissions,
      editing,
      submitting, //redux form prop
      isSubmitting,
      confirmButtonStyle,
    } = this.props;

    const { tabs: customTabs, tabSelected } = getFormTabs(
      navTabs,
      fields,
      permissions,
      selectedTabName
    );

    const tabConfig =
      sourceFormConfig.form.findTabByName(tabSelected) ||
      sourceFormConfig.form.getDefaultTab();

    const finalTabs =
      permissions &&
      customTabs.filter((t) => {
        switch (t) {
          case "Privileges":
            return permissions.get("canEditAccessControlList") ? t : null;
          default:
            return t;
        }
      });

    return (
      <ModalForm
        {...modalFormProps(this.props)}
        submitting={submitting || isSubmitting}
        onSubmit={handleSubmit(onFormSubmit)}
        footerChildren={footerChildren}
        showSpinnerAndText={this.props.showSpinnerAndText}
        confirmButtonStyle={confirmButtonStyle}
      >
        <div className={sourceFormWrapper} data-qa="configurable-source-form">
          {sourceFormConfig.form.getTabs().length > 1 && (
            <NavPanel
              showSingleTab
              changeTab={handleChangeTab}
              activeTab={tabConfig.getName()}
              tabs={finalTabs ? finalTabs : customTabs}
            />
          )}
          <div className={scrollRightContainerWithHeader}>
            <FormContext.Provider
              value={{
                editing,
                sourceType: sourceFormConfig.sourceType,
                change: (fieldName, fieldValue) => {
                  this.props.dispatch(change(FORM_NAME, fieldName, fieldValue));
                },
              }}
            >
              <FormBody>
                {tabConfig && (
                  <FormTab
                    fields={fields}
                    tabConfig={tabConfig}
                    formConfig={sourceFormConfig}
                    EntityType={this.props.EntityType}
                    accessControlId={this.props.fields.id.initialValue}
                  />
                )}
              </FormBody>
            </FormContext.Provider>
          </div>
        </div>
      </ModalForm>
    );
  }
}

export default class ConfigurableSourceFormWrapper extends Component {
  static propTypes = {
    sourceFormConfig: PropTypes.object,
  };

  constructor(props) {
    super(props);
    const { sourceFormConfig } = this.props;
    const tabs = sourceFormConfig.form.getTabs();
    const navTabs = Immutable.OrderedMap(
      tabs.map((tab) => [tab.getName(), tab.getName()])
    );
    this.state = {
      selectedTabName: "",
      navTabs,
    };
    this.wrappedComponent = connectComplexForm(
      {
        form: FORM_NAME,
        onSubmitFail: this.handleSyncValidationFailure,
        fields: SOURCE_FIELDS,
      },
      [],
      mapStateToProps,
      null
    )(ConfigurableSourceForm);
  }

  handleSyncValidationFailure = (err) => {
    const { sourceFormConfig } = this.props;
    const fieldsWithError = FormUtils.findFieldsWithError(err);
    const tabConfig = FormUtils.findTabWithError(
      sourceFormConfig.form,
      fieldsWithError,
      this.state.selectedTabName
    );
    this.setState({ selectedTabName: tabConfig.getName() });
  };

  handleChangeTab = (tab) => {
    this.setState({ selectedTabName: tab });
  };

  render() {
    const WrappedForm = this.wrappedComponent;
    return (
      <WrappedForm
        {...this.props}
        selectedTabName={this.state.selectedTabName}
        navTabs={this.state.navTabs}
        handleChangeTab={this.handleChangeTab}
      />
    );
  }
}

function mapStateToProps(state, props) {
  const initialValues = FormUtils.mergeInitValuesWithConfig(
    props.initialValues,
    state,
    props
  );
  return {
    initialValues,
  };
}
