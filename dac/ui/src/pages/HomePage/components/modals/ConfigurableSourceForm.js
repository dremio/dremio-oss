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
import Immutable from 'immutable';
import PropTypes from 'prop-types';
import FormUtils from 'utils/FormUtils/FormUtils';

import { FormBody, ModalForm, modalFormProps } from 'components/Forms';
import { connectComplexForm } from 'components/Forms/connectComplexForm';
import FormTab from 'components/Forms/FormTab';

import NavPanel from 'components/Nav/NavPanel';

import { sourceFormWrapper } from 'uiTheme/less/forms.less';
import { scrollRightContainerWithHeader } from 'uiTheme/less/layout.less';


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
    sourceFormConfig: PropTypes.object
  };

  getChildContext() {
    return { editing: this.props.editing };
  }

  render() {
    const {fields, handleSubmit, onFormSubmit, sourceFormConfig, handleChangeTab, navTabs, selectedTabName} = this.props;
    const tabConfig = sourceFormConfig.form.findTabByName(selectedTabName)
      || sourceFormConfig.form.getDefaultTab();

    return (
      <ModalForm {...modalFormProps(this.props)} onSubmit={handleSubmit(onFormSubmit)}>
        <div className={sourceFormWrapper} data-qa='configurable-source-form'>
          {sourceFormConfig.form.getTabs().length > 1 &&
          <NavPanel
            changeTab={handleChangeTab}
            activeTab={tabConfig.getName()}
            tabs={navTabs}/>}
          <div className={scrollRightContainerWithHeader}>
            <FormBody>
              {tabConfig && <FormTab fields={fields} tabConfig={tabConfig} formConfig={sourceFormConfig}/>}
            </FormBody>
          </div>
        </div>
      </ModalForm>
    );
  }
}

ConfigurableSourceForm.childContextTypes = {
  editing: PropTypes.bool
};

export default class ConfigurableSourceFormWrapper extends Component {
  static propTypes = {
    sourceFormConfig: PropTypes.object
  };

  constructor(props) {
    super(props);
    const {sourceFormConfig} = this.props;
    const tabs = sourceFormConfig.form.getTabs();
    const navTabs = Immutable.OrderedMap(tabs.map((tab) => [tab.getName(), tab.getName()]));
    this.state = {
      selectedTabName: '',
      navTabs
    };
    this.wrappedComponent = connectComplexForm({
      form: 'source',
      onSubmitFail: this.handleSyncValidationFailure,
      fields: ['id', 'name', 'description']
    }, [], mapStateToProps, null)(ConfigurableSourceForm);
  }

  handleSyncValidationFailure = (err) => {
    const { sourceFormConfig } = this.props;
    const fieldsWithError = FormUtils.findFieldsWithError(err);
    const tabConfig = FormUtils.findTabWithError(sourceFormConfig.form, fieldsWithError, this.state.selectedTabName);
    this.setState({ selectedTabName: tabConfig.getName() });

    const errorIcon = {name: 'Error.svg', alt: 'Errors', style: {height: 19, width: 19}};
    const updatedTabs = this.state.navTabs.update(tabConfig.getName(), value => {
      return (FormUtils.tabHasError(tabConfig, fieldsWithError)) ?
        {text: tabConfig.getName(), icon: errorIcon} :
        {text: tabConfig.getName()};
    });
    this.setState({ navTabs: updatedTabs });
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
        handleChangeTab={this.handleChangeTab}/>
    );
  }
}

function mapStateToProps(state, props) {
  const initialValues = FormUtils.mergeInitValuesWithConfig(props.initialValues, state, props);
  return {
    initialValues
  };
}
