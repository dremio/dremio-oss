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
import React, { Component } from 'react';
import PropTypes from 'prop-types';
import Immutable from 'immutable';
import { connect } from 'react-redux';
import { injectIntl, FormattedMessage } from 'react-intl';
import { createSource, createSampleSource } from 'actions/resources/sources';
import Modal from 'components/Modals/Modal';
import ViewStateWrapper from 'components/ViewStateWrapper';
import SelectSourceType from 'pages/HomePage/components/modals/AddSourceModal/SelectSourceType';
import sourceForms from 'components/sourceForms/index';
import ApiUtils from 'utils/apiUtils/apiUtils';

import DataFreshnessSection from 'components/Forms/DataFreshnessSection';
import MetadataRefresh from 'components/Forms/MetadataRefresh';
import FormUnsavedWarningHOC from 'components/Modals/FormUnsavedWarningHOC';
import AddSourceModalMixin from 'dyn-load/pages/HomePage/components/modals/AddSourceModal/AddSourceModalMixin';

import { getSortedSpaces, getSortedSources } from 'selectors/resources';

const TIME_BEFORE_MESSAGE = 5000;

const VIEW_ID = 'ADD_SOURCE_MODAL';

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
    spaces: PropTypes.instanceOf(Immutable.List).isRequired,
    sources: PropTypes.instanceOf(Immutable.List).isRequired,
    createSampleSource: PropTypes.func.isRequired,
    intl: PropTypes.object.isRequired
  };

  constructor(props) {
    super(props);

    this.state = {
      lastSource: props.source,
      isSubmitTakingLong: false,
      submitTimer: null,
      isAddingSampleSource: false
    };
  }

  componentWillReceiveProps(nextProps) {
    // we want to update to show the picked source
    // but only reset it when re-shown (otherwise it pops back to the picker screen while animating out on hide)
    if (nextProps.source !== undefined || (nextProps.isOpen && !this.props.isOpen)) {
      this.setState({lastSource: nextProps.source});
    }
  }

  hide = (...args) => {
    this.stopTrackSubmitTime();
    this.props.hide(...args);
  }

  getTitle(source) {
    const { intl } = this.props;
    return source
      ? intl.formatMessage({ id: 'Source.NewSourceStep2'}, {sourceLabel: source.label})
      : intl.formatMessage({ id: 'Source.NewSourceStep1' });
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

  handleAddSampleSource = () => {
    this.setState({isAddingSampleSource: true});
    return this.props.createSampleSource(this.props.sources, this.props.spaces, {viewId: VIEW_ID}).then((response) => {
      if (response && !response.error) {
        const nextSource = ApiUtils.getEntityFromResponse('source', response);
        this.context.router.push(nextSource.getIn(['links', 'self']));
      }
      this.setState({isAddingSampleSource: false});
      this.hide();
    });
  }

  renderLongSubmitLabel = () => {
    return this.state.isSubmitTakingLong ? (
      <span>
        <FormattedMessage id='Source.LargerSourcesWarning' />
      </span>
    ) : null;
  }

  render() {
    const { isOpen, updateFormDirtyState } = this.props;
    const { lastSource } = this.state;
    const sourceForm = lastSource && Object.values(sourceForms).find((sf) => sf.sourceType === lastSource.sourceType);

    return (
      <Modal
        size='large'
        title={this.getTitle(lastSource)}
        isOpen={isOpen}
        confirm={lastSource && this.confirm}
        hide={this.hide}>
        {/* use state isAddingSampleSource instead of a viewId because we just want to block on progress - other UI kicks in for failure */}
        <ViewStateWrapper viewState={new Immutable.Map({isInProgress: this.state.isAddingSampleSource})}>
          { !lastSource
            ? <SelectSourceType onSelectSource={this.handleSelectSource} onAddSampleSource={this.handleAddSampleSource}/>
            : React.createElement(sourceForm, {
              ref: 'form',
              onFormSubmit: this.submit,
              onCancel: this.hide,
              updateFormDirtyState,
              formBodyStyle: styles.formBody,
              footerChildren: this.renderLongSubmitLabel(),
              initialValues: {
                accelerationRefreshPeriod: DataFreshnessSection.defaultFormValueRefreshInterval(),
                accelerationGracePeriod: DataFreshnessSection.defaultFormValueGracePeriod(),
                ...MetadataRefresh.defaultFormValues()
              }
            })
          }
        </ViewStateWrapper>
      </Modal>
    );
  }
}

function mapStateToProps(state) {
  return {
    sources: getSortedSources(state),
    spaces: getSortedSpaces(state)
  };
}

export default connect(mapStateToProps, {
  createSource,
  createSampleSource
})(FormUnsavedWarningHOC(AddSourceModal));

export const styles = {
  formBody: {
    margin: '0 auto',
    // form needs extra whitespace on the right for (i) and x icons.
    // so use width smaller than large modal width
    width: 670
  }
};
