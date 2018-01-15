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
import pureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import { connect } from 'react-redux';
import Immutable from 'immutable';
import mergeWith from 'lodash/mergeWith';

import { createSource, removeSource, loadSource } from 'actions/resources/sources';
import sourceForms from 'components/sourceForms';
import ApiUtils from 'utils/apiUtils/apiUtils';
import ViewStateWrapper from 'components/ViewStateWrapper';
import Message from 'components/Message';

import EditSourceViewMixin,
  { mapStateToProps } from 'dyn-load/pages/HomePage/components/modals/EditSourceViewMixin';

import { styles as addSourceStyles } from './AddSourceModal/AddSourceModal';

export const VIEW_ID = 'EditSourceView';

@pureRender
@EditSourceViewMixin
export class EditSourceView extends Component {
  static propTypes = {
    sourceName: PropTypes.string.isRequired,
    sourceType: PropTypes.string.isRequired,
    hide: PropTypes.func.isRequired,
    createSource: PropTypes.func.isRequired,
    removeSource: PropTypes.func.isRequired,
    loadSource: PropTypes.func,
    messages: PropTypes.instanceOf(Immutable.List),
    viewState: PropTypes.instanceOf(Immutable.Map),
    source: PropTypes.instanceOf(Immutable.Map),
    initialFormValues: PropTypes.object,
    updateFormDirtyState: PropTypes.func
  };

  static contextTypes = {
    location: PropTypes.object.isRequired,
    router: PropTypes.object.isRequired
  }

  componentWillMount() {
    const { sourceName } = this.props;
    this.props.loadSource(sourceName, VIEW_ID);
  }

  submitEdit = (form) => {
    const { sourceType } = this.props;

    this.mutateFormValues(form);

    return ApiUtils.attachFormSubmitHandlers(
      this.props.createSource(form, sourceType)
    ).then(() => {
      this.context.router.replace('/sources/list');
    });
  }

  renderMessages() {
    const { messages } = this.props;
    return messages && messages.map((message, i) => {
      const type = message.get('level') === 'WARN' ? 'warning' : message.get('level').toLowerCase();
      return (
        <Message
          messageType={type}
          message={message.get('message')}
          messageId={i.toString()}
          style={{ width: '100%' }}
        />
      );
    });
  }

  render() {
    const { updateFormDirtyState, initialFormValues, source, sourceType, viewState, hide } = this.props;
    const sourceForm = Object.values(sourceForms).find((sf) => sf.sourceType === sourceType);
    return <ViewStateWrapper viewState={viewState} style={styles.viewWrapper}>
      {this.renderMessages()}
      {
        sourceForm &&
        React.createElement(sourceForm, {
          onFormSubmit: this.submitEdit,
          onCancel: hide,
          editing: true,
          key: sourceType,
          updateFormDirtyState,
          formBodyStyle: styles.formBody,
          initialValues: mergeWith(
            source && source.size > 1 ? source.toJS() : {}, initialFormValues,
            arrayAsPrimitiveMerger
          )
        })
      }
    </ViewStateWrapper>;
  }
}

const styles = {
  ...addSourceStyles,
  viewWrapper: { overflow: 'hidden', display: 'flex', flexDirection: 'column' }
};

export default connect(mapStateToProps, {
  loadSource,
  createSource,
  removeSource
})(EditSourceView);

function arrayAsPrimitiveMerger(objValue, srcValue) {
  if (Array.isArray(objValue)) {
    return srcValue;
  }
}

