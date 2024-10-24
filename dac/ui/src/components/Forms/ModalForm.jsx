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
import Message from "components/Message";
import ConfirmCancelFooter from "components/Modals/ConfirmCancelFooter";
import FormProgressWrapper from "components/FormProgressWrapper";

import {
  modalForm,
  modalFormBody,
  modalFormWrapper,
} from "uiTheme/radium/forms";
import Keys from "#oss/constants/Keys.json";
import { FLEX_WRAP_COL_START } from "#oss/uiTheme/radium/flexStyle";

export function modalFormProps(props) {
  return {
    onCancel: props.onCancel,
    error: props.error,
    submitting: props.submitting,
    done: props.done,
    hideCancel: props.hideCancel,
    confirmText: props.confirmText,
    canSubmit: props.canSubmit,
  };
}
class ModalForm extends Component {
  static propTypes = {
    confirmText: PropTypes.string,
    cancelText: PropTypes.string,
    onSubmit: PropTypes.func.isRequired,
    onCancel: PropTypes.func,
    error: PropTypes.object,
    submitting: PropTypes.bool,
    canSubmit: PropTypes.bool,
    canCancel: PropTypes.bool,
    done: PropTypes.bool,
    children: PropTypes.node,
    style: PropTypes.object,
    confirmStyle: PropTypes.object,
    wrapperStyle: PropTypes.object,
    footerChildren: PropTypes.node,
    formBodyStyle: PropTypes.object,
    wantsEventReturned: PropTypes.bool,
    isNestedForm: PropTypes.bool.isRequired, // <form> not allowed in <form> in html
    // styling
    isModal: PropTypes.bool,
    hideError: PropTypes.bool,
    leftAlignFooter: PropTypes.bool,
    hideCancel: PropTypes.bool,

    renderFooter: PropTypes.func,
    customMessage: PropTypes.object,
  };

  static defaultProps = {
    // todo: loc
    hideCancel: false,
    canSubmit: true,
    canCancel: true,
    confirmText: "Save",
    cancelText: "Cancel",
    isNestedForm: false,
    isModal: true,
  };

  state = {
    messageDismissed: false,
  };

  UNSAFE_componentWillReceiveProps(nextProps) {
    if (nextProps.error !== this.props.error && !this.props.hideError) {
      this.setState({ messageDismissed: false });
    }
  }

  handleDismissMessage = () => {
    this.setState({ messageDismissed: true });
  };

  handleSubmissionEvent = (evt) => {
    if (evt) {
      if (evt.type === "keydown" && evt.keyCode !== Keys.ENTER) {
        // todo: switch to KeyboardEvent.code or key (w/ polyfill) depends on what React supports
        return;
      }
      evt.preventDefault();
    }
    if (this.props.wantsEventReturned) {
      return this.props.onSubmit(evt);
    }

    if (this.props.canSubmit) {
      this.props.onSubmit();
    }
  };

  render() {
    const {
      confirmText,
      cancelText,
      onCancel,
      error,
      submitting,
      canSubmit,
      canCancel,
      style,
      wrapperStyle,
      children,
      footerChildren,
      isNestedForm,
      isModal,
      leftAlignFooter,
      hideCancel,
      confirmButtonStyle,
      renderFooter,
    } = this.props;

    const formBodyStyle = this.props.formBodyStyle || {};
    const internalChildren = [
      this.props.customMessage,
      error && !this.props.hideError && (
        <Message
          messageType="error"
          message={error.message}
          messageId={error.id}
          onDismiss={this.handleDismissMessage}
          dismissed={this.state.messageDismissed}
          detailsStyle={{ maxHeight: 100 }}
          style={{
            ...styles.message,
            ...(this.props.customMessage && styles.relativeMessage),
          }}
        />
      ),
      <div key="modal-form" style={{ ...modalFormBody, ...formBodyStyle }}>
        <FormProgressWrapper submitting={submitting}>
          <div
            className="modal-form-wrapper"
            style={{ ...modalFormWrapper, ...wrapperStyle }}
          >
            {children}
          </div>
        </FormProgressWrapper>
      </div>,
      renderFooter ? (
        renderFooter()
      ) : (
        <ConfirmCancelFooter
          key="cancel-footer"
          modalFooter={isModal}
          style={this.props.confirmStyle}
          footerChildren={footerChildren}
          confirmText={confirmText}
          cancelText={cancelText}
          cancel={onCancel}
          submitting={submitting}
          canSubmit={canSubmit}
          canCancel={canCancel}
          confirm={this.handleSubmissionEvent}
          leftAlign={leftAlignFooter}
          hideCancel={hideCancel}
          confirmButtonStyle={confirmButtonStyle}
        />
      ),
    ];

    const formStyle = isModal ? modalForm : styles.nonModalForm;

    const sharedProps = {
      style: { ...formStyle, ...style },
      children: internalChildren,
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
    position: "absolute",
  },
  relativeMessage: {
    position: "relative",
  },
  nonModalForm: {
    ...FLEX_WRAP_COL_START,
    width: 640,
    position: "relative", // to not allow error message overflow a form
  },
};
export default ModalForm;
