/*
 * Copyright (C) 2017 Dremio Corporation
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
import React, { Component, PropTypes } from 'react';
import { connect } from 'react-redux';
import { createSource } from 'actions/resources/sources';
import Modal from 'components/Modals/Modal';
import SelectSourceType from 'pages/NewResourcePage/subpages/SelectSourceType';
import * as sourceForms from 'components/sourceForms';
import ApiUtils from 'utils/apiUtils/apiUtils';

import DataFreshnessSection from 'components/Forms/DataFreshnessSection';
import MetadataRefresh from 'components/Forms/MetadataRefresh';
import FormUnsavedWarningHOC from 'components/Modals/FormUnsavedWarningHOC';
import AddSourceModalMixin from 'dyn-load/pages/HomePage/components/modals/AddSourceModalMixin';

const TIME_BEFORE_MESSAGE = 5000;

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
    createSource: PropTypes.func
  };

  constructor(props) {
    super(props);

    this.state = {
      lastSource: props.source,
      isSubmitTakingLong: false,
      submitTimer: null
    };
  }

  componentWillReceiveProps(nextProps) {
    if (nextProps.source !== undefined) {
      this.setState({lastSource: nextProps.source});
    }
  }

  hide = (...args) => {
    this.stopTrackSubmitTime();
    this.props.hide(...args);
  }

  getTitle(source) { // todo: loc
    return source
      ? `New ${source.label} Source: Step 2 of 2`
      : 'New Source: Step 1 of 2';
  }

  confirm = () => {
    this.refs.form.submit();
  }

  handleSelectSource = (source) => {
    const { router } = this.context;
    const { location } = this.props;
    router.push({...location, state: {...location.state, source}});
  }

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

  submit = (values) => {
    this.mutateFormValues(values);
    this.startTrackSubmitTime();

    return ApiUtils.attachFormSubmitHandlers(
      this.props.createSource(values, this.props.source.sourceType)
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
  }

  renderLongSubmitLabel = () => {
    return this.state.isSubmitTakingLong ? <span>
      {la('Retrieving details for larger sources may take a few minutes…')}
    </span> : null;
  }

  render() {
    const { isOpen, updateFormDirtyState } = this.props;
    const { lastSource } = this.state;
    return (
      <Modal
        size='large'
        title={this.getTitle(lastSource)}
        isOpen={isOpen}
        confirm={lastSource && this.confirm}
        hide={this.hide}>
        { lastSource
          ? React.createElement(sourceForms[lastSource.sourceType], {
            ref: 'form',
            onFormSubmit: this.submit,
            onCancel: this.hide,
            updateFormDirtyState,
            formBodyStyle: styles.formBody,
            footerChildren: this.renderLongSubmitLabel(),
            initialValues: {
              accelerationTTL: DataFreshnessSection.defaultFormValue(),
              ...MetadataRefresh.defaultFormValues()
            }
          })
          : <SelectSourceType onSelectSource={this.handleSelectSource}/> }
      </Modal>
    );
  }
}

export default connect(null, {
  createSource
})(FormUnsavedWarningHOC(AddSourceModal));


export const styles = {
  formBody: {
    margin: '0 auto',
    // form needs extra whitespace on the right for (i) and x icons.
    // so use width smaller than large modal width
    width: 670
  }
};
