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
import { Component, PropTypes } from 'react';
import Radium from 'radium';
import Message from 'components/Message';
import ConfirmCancelFooter from 'components/Modals/ConfirmCancelFooter';
import FormProgressWrapper from 'components/FormProgressWrapper';

import { modalForm, modalFormBody, modalFormWrapper } from 'uiTheme/radium/forms';

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
    onCancel: PropTypes.func.isRequired,
    error: PropTypes.object,
    submitting: PropTypes.bool,
    canSubmit: PropTypes.bool,
    done: PropTypes.bool,
    children: PropTypes.node,
    style: PropTypes.object,
    confirmStyle: PropTypes.object,
    wrapperStyle: PropTypes.object,
    footerChildren: PropTypes.node,
    formBodyStyle: PropTypes.object
  };

  static defaultProps = { // todo: loc
    canSubmit: true,
    confirmText: 'Save',
    cancelText: 'Cancel'
  };

  constructor(props) {
    super(props);
    this.handleDismissMessage = this.handleDismissMessage.bind(this);
    this.state = { messageDismissed: false };
  }

  componentWillReceiveProps(nextProps) {
    if (nextProps.error !== this.props.error) {
      this.setState({ messageDismissed: false });
    }
  }

  handleDismissMessage() {
    this.setState({ messageDismissed: true });
  }

  render() {
    const {
      confirmText, cancelText, onSubmit, onCancel, error, submitting, canSubmit, style, wrapperStyle, children,
      footerChildren
    } = this.props;

    return (
      <form onSubmit={onSubmit} style={{...modalForm, ...style}}>
        {error && <Message
          messageType='error'
          message={error.message}
          messageId={error.id}
          onDismiss={this.handleDismissMessage}
          dismissed={this.state.messageDismissed}
          detailsStyle={{maxHeight: 100}}
          style={styles.message}
        />}
        <div style={[modalFormBody, this.props.formBodyStyle]}>
          <FormProgressWrapper submitting={submitting}>
            <div className='modal-form-wrapper' style={{ ...modalFormWrapper, ...wrapperStyle }}>
              {children}
            </div>
          </FormProgressWrapper>
        </div>
        <ConfirmCancelFooter
          style={this.props.confirmStyle}
          submitForm
          footerChildren={footerChildren}
          confirmText={confirmText}
          cancelText={cancelText}
          cancel={onCancel}
          submitting={submitting}
          canSubmit={canSubmit}
        />
      </form>
    );
  }
}

const styles = {
  message: {
    zIndex: 1000,
    flexShrink: 0,
    minHeight: 0
  }
};
