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
import Radium from 'radium';
import PropTypes from 'prop-types';
import Message from 'components/Message';
import ConfirmCancelFooter from 'components/Modals/ConfirmCancelFooter';
import FormProgressWrapper from 'components/FormProgressWrapper';

import { modalForm, modalFormBody, modalFormWrapper } from 'uiTheme/radium/forms';
import Keys from 'constants/Keys.json';

export function modalFormProps(props) {
  return {
    onCancel: props.onCancel,
    error: props.error,
    submitting: props.submitting,
    footerChildren: props.footerChildren,
    done: props.done
  };
}
@Radium
export default class ModalForm extends Component {
  static propTypes = {
    confirmText: PropTypes.string,
    cancelText: PropTypes.string,
    onSubmit: PropTypes.func.isRequired,
    onCancel: PropTypes.func,
    error: PropTypes.object,
    submitting: PropTypes.bool,
    canSubmit: PropTypes.bool,
    done: PropTypes.bool,
    children: PropTypes.node,
    style: PropTypes.object,
    confirmStyle: PropTypes.object,
    wrapperStyle: PropTypes.object,
    footerChildren: PropTypes.node,
    formBodyStyle: PropTypes.object,
    isNestedForm: PropTypes.bool.isRequired // <form> not allowed in <form> in html
  };

  static defaultProps = { // todo: loc
    canSubmit: true,
    confirmText: 'Save',
    cancelText: 'Cancel',
    isNestedForm: false
  };

  state = {
    messageDismissed: false
  }

  componentWillReceiveProps(nextProps) {
    if (nextProps.error !== this.props.error) {
      this.setState({ messageDismissed: false });
    }
  }

  handleDismissMessage = () =>  {
    this.setState({ messageDismissed: true });
  }

  handleSubmissionEvent = (evt) => {
    if (evt) {
      if (evt.type === 'keydown' && evt.keyCode !== Keys.ENTER) { // todo: switch to KeyboardEvent.code or key (w/ polyfill) depends on what React supports
        return;
      }
      evt.preventDefault();
    }

    if (this.props.canSubmit) this.props.onSubmit();
  }

  render() {
    const {
      confirmText, cancelText, onCancel, error, submitting, canSubmit, style, wrapperStyle, children,
      footerChildren, isNestedForm
    } = this.props;

    const internalChildren = [
      error && <Message
        messageType='error'
        message={error.message}
        messageId={error.id}
        onDismiss={this.handleDismissMessage}
        dismissed={this.state.messageDismissed}
        detailsStyle={{maxHeight: 100}}
        style={styles.message}
      />,
      <div style={[modalFormBody, this.props.formBodyStyle]}>
        <FormProgressWrapper submitting={submitting}>
          <div className='modal-form-wrapper' style={{ ...modalFormWrapper, ...wrapperStyle }}>
            {children}
          </div>
        </FormProgressWrapper>
      </div>,
      <ConfirmCancelFooter
        style={this.props.confirmStyle}
        footerChildren={footerChildren}
        confirmText={confirmText}
        cancelText={cancelText}
        cancel={onCancel}
        submitting={submitting}
        canSubmit={canSubmit}
        confirm={this.handleSubmissionEvent}
      />
    ];

    const sharedProps = {
      style: {...modalForm, ...style},
      children: internalChildren
    };

    if (isNestedForm) {
      // can't wrap in <form> as it would be a nested <form>, so do own key listening
      return <div onKeyDown={this.handleSubmissionEvent} {...sharedProps} />;
    }
    return <form onSubmit={this.handleSubmissionEvent} {...sharedProps} />;

  }
}

const styles = {
  message: {
    zIndex: 1000,
    flexShrink: 0,
    minHeight: 0,
    position: 'absolute'
  }
};
