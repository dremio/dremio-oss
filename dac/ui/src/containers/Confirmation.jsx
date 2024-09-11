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

import { connect } from "react-redux";
import { hideConfirmationDialog } from "actions/confirmation";

import ConfirmModal from "components/Modals/ConfirmModal";

export class ConfirmationContainer extends Component {
  static propTypes = {
    confirmation: PropTypes.object,
    hideConfirmationDialog: PropTypes.func,
  };

  state = {
    submitting: false,
  };

  onConfirm = async (promptValue) => {
    const { confirm, isAsyncAction } = this.props.confirmation;
    const isFunction = confirm && typeof confirm === "function";
    if (!isAsyncAction) {
      this.props.hideConfirmationDialog();
      if (isFunction) {
        confirm(promptValue);
      }
    } else {
      if (isFunction) {
        this.setState({ submitting: true });
        await confirm(promptValue);
      }
      this.setState({ submitting: false });
      this.props.hideConfirmationDialog();
    }
  };

  onCancel = () => {
    this.props.hideConfirmationDialog();
    const { cancel } = this.props.confirmation;
    cancel && cancel();
  };

  render() {
    const {
      isOpen,
      text,
      title,
      confirmText,
      confirmButtonStyle,
      cancelText,
      style,
      hideCancelButton,
      showOnlyConfirm,
      doNotAskAgainText,
      doNotAskAgainKey,
      showPrompt,
      promptLabel,
      promptFieldProps,
      dataQa,
      validatePromptText,
      isCentered,
      className,
      headerIcon,
      size,
    } = this.props.confirmation;
    if (!isOpen) {
      return null;
    }
    return (
      <ConfirmModal
        hideCancelButton={hideCancelButton}
        isOpen={isOpen}
        title={title}
        text={text}
        confirmText={confirmText}
        confirmButtonStyle={confirmButtonStyle}
        cancelText={cancelText}
        style={style}
        showOnlyConfirm={showOnlyConfirm}
        doNotAskAgainText={doNotAskAgainText}
        doNotAskAgainKey={doNotAskAgainKey}
        showPrompt={showPrompt}
        promptLabel={promptLabel}
        promptFieldProps={promptFieldProps}
        onCancel={this.onCancel}
        onConfirm={this.onConfirm}
        dataQa={dataQa}
        validatePromptText={validatePromptText}
        isCentered={isCentered}
        className={className}
        headerIcon={headerIcon}
        size={size}
        asyncSubmitting={this.state.submitting}
      />
    );
  }
}

function mapStateToProps(state) {
  return {
    confirmation: state.confirmation,
  };
}

export default connect(mapStateToProps, { hideConfirmationDialog })(
  ConfirmationContainer,
);
