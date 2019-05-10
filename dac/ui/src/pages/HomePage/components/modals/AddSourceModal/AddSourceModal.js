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
import { connect } from 'react-redux';
import { FormattedMessage, injectIntl } from 'react-intl';

import ApiUtils from 'utils/apiUtils/apiUtils';
import FormUtils from 'utils/FormUtils/FormUtils';
import SourceFormJsonPolicy from 'utils/FormUtils/SourceFormJsonPolicy';
import { createSampleSource, createSource } from 'actions/resources/sources';

import Modal from 'components/Modals/Modal';
import ViewStateWrapper from 'components/ViewStateWrapper';
import FormUnsavedWarningHOC from 'components/Modals/FormUnsavedWarningHOC';

import SelectSourceType from 'pages/HomePage/components/modals/AddSourceModal/SelectSourceType';
import ConfigurableSourceForm from 'pages/HomePage/components/modals/ConfigurableSourceForm';

import AddSourceModalMixin from 'dyn-load/pages/HomePage/components/modals/AddSourceModal/AddSourceModalMixin';


const VIEW_ID = 'ADD_SOURCE_MODAL';
const TIME_BEFORE_MESSAGE = 5000;

const DEFAULT_VLHF_LIST = require('customData/vlhfList');

@injectIntl
@AddSourceModalMixin
export class AddSourceModal extends Component {

  static contextTypes = {
    router: PropTypes.object.isRequired
  };

  static propTypes = {
    location: PropTypes.object.isRequired,
    isOpen: PropTypes.bool,
    hide: PropTypes.func,
    source: PropTypes.object,
    updateFormDirtyState: PropTypes.func,
    createSource: PropTypes.func,
    createSampleSource: PropTypes.func.isRequired,
    intl: PropTypes.object.isRequired
  };

  state = {
    isTypeSelected: false,
    selectedFormType: {},
    isSubmitTakingLong: false,
    submitTimer: null,
    sourceTypes: [],
    isAddingSampleSource: false,
    didSourceTypeLoadFail: false
  };

  componentDidMount() {
    this.setStateWithSourceTypeListFromServer();
  }

  setStateWithSourceTypeListFromServer() {
    ApiUtils.fetch('source/type').then(response => {
      response.json().then((result) => {
        const combinedListConfig = SourceFormJsonPolicy.combineDefaultAndLoadedList(result.data, DEFAULT_VLHF_LIST);
        this.setState({sourceTypes: combinedListConfig});
      });
    }, () => {
      this.setState({didSourceTypeLoadFail: true});
    });
  }

  setStateWithSourceTypeConfigFromServer(typeCode) {
    ApiUtils.fetch(`source/type/${typeCode}`).then(response => {
      response.json().then((result) => {
        const conbinedConfig = SourceFormJsonPolicy.getCombinedConfig(typeCode, result);
        this.setState({isTypeSelected: true, selectedFormType: conbinedConfig});
      });
    }, () => {
      this.setState({didSourceTypeLoadFail: true});
    });
  }

  componentWillReceiveProps(nextProps) {
    if (!this.props.isOpen && nextProps.isOpen) {
      this.setState({isTypeSelected: false, selectedFormType: {}});
      this.props.updateFormDirtyState(false); // mark form not dirty to avoid unsaved prompt
    }
  }

  hide = (...args) => {
    this.props.hide(...args);
  };

  getTitle() {
    const { intl } = this.props;
    return this.state.isTypeSelected
      ? intl.formatMessage({ id: 'Source.NewSourceStep2'}, {sourceLabel: this.state.selectedFormType.label})
      : intl.formatMessage({ id: 'Source.NewSourceStep1' });
  }

  handleSelectSource = (source) => {
    if (source.sourceType === 'SampleSource') {
      this.handleAddSampleSource();
    } else {
      this.setStateWithSourceTypeConfigFromServer(source.sourceType);
    }
  };

  handleAddSampleSource = () => {
    this.setState({isAddingSampleSource: true});
    return this.props.createSampleSource({viewId: VIEW_ID}).then((response) => {
      if (response && !response.error) {
        const nextSource = ApiUtils.getEntityFromResponse('source', response);
        this.context.router.push(nextSource.getIn(['links', 'self']));
      }
      this.setState({isAddingSampleSource: false});
      this.hide();
    });
  };

  startTrackSubmitTime = () => {
    const submitTimer = setTimeout(() => {
      this.setState({
        isSubmitTakingLong: true
      });
    }, TIME_BEFORE_MESSAGE);
    this.setState({
      submitTimer
    });
  };

  stopTrackSubmitTime = () => {
    clearTimeout(this.state.submitTimer);
    this.setState({
      isSubmitTakingLong: false,
      submitTimer: null
    });
  };

  handleAddSourceSubmit = (values) => {
    this.mutateFormValues(values);
    this.startTrackSubmitTime();

    return ApiUtils.attachFormSubmitHandlers(
      this.props.createSource(values, this.state.selectedFormType.sourceType)
    ).then((response) => {
      this.stopTrackSubmitTime();
      if (response && !response.error) {
        const nextSource = ApiUtils.getEntityFromResponse('source', response);
        this.context.router.push(nextSource.getIn(['links', 'self']));
      }
    }).catch((error) => {
      this.stopTrackSubmitTime();
      throw error;
    });
  };

  renderLongSubmitLabel = () => {
    return this.state.isSubmitTakingLong ? (
      <span>
        <FormattedMessage id='Source.LargerSourcesWarning' />
      </span>
    ) : null;
  };

  render() {
    const { isOpen, updateFormDirtyState } = this.props;

    const viewState = this.state.didSourceTypeLoadFail ?
      new Immutable.fromJS({isFailed: true, error: {message: la('Failed to load source list.')}}) :
      new Immutable.Map({isInProgress: this.state.isAddingSampleSource});

    return (
      <Modal
        size='medium'
        title={this.getTitle()}
        isOpen={isOpen}
        confirm={this.state.isTypeSelected && this.confirm}
        hide={this.hide}>
        <ViewStateWrapper viewState={viewState}>
          {!this.state.isTypeSelected
            ? <SelectSourceType sourceTypes={this.state.sourceTypes}
                                onSelectSource={this.handleSelectSource}/>
            : <ConfigurableSourceForm sourceFormConfig={this.state.selectedFormType}
                                      ref='form'
                                      onFormSubmit={this.handleAddSourceSubmit}
                                      onCancel={this.hide}
                                      updateFormDirtyState={updateFormDirtyState}
                                      footerChildren={this.renderLongSubmitLabel()}
                                      fields={FormUtils.getFieldsFromConfig(this.state.selectedFormType)}
                                      validate={FormUtils.getValidationsFromConfig(this.state.selectedFormType)}
            />
          }
        </ViewStateWrapper>
      </Modal>
    );
  }
}

export default connect(null, {
  createSource,
  createSampleSource
})(FormUnsavedWarningHOC(AddSourceModal));
