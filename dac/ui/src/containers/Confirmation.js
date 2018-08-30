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

import { connect } from 'react-redux';
import { hideConfirmationDialog } from 'actions/confirmation';

import ConfirmModal from 'components/Modals/ConfirmModal';

export class ConfirmationContainer extends Component {
  static propTypes = {
    confirmation: PropTypes.object,
    hideConfirmationDialog: PropTypes.func
  }

  onConfirm = (promptValue) => {
    this.props.hideConfirmationDialog();
    const { confirm } = this.props.confirmation;
    if (confirm && typeof confirm === 'function') {
      confirm(promptValue);
    }
  }

  onCancel = () => {
    this.props.hideConfirmationDialog();
    const { cancel } = this.props.confirmation;
    cancel && cancel();
  }

  render() {
    const {
      isOpen,
      text,
      title,
      confirmText,
      cancelText,
      hideCancelButton,
      showOnlyConfirm,
      doNotAskAgainText,
      doNotAskAgainKey,
      showPrompt,
      promptFieldProps,
      dataQa
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
        cancelText={cancelText}
        showOnlyConfirm={showOnlyConfirm}
        doNotAskAgainText={doNotAskAgainText}
        doNotAskAgainKey={doNotAskAgainKey}
        showPrompt={showPrompt}
        promptFieldProps={promptFieldProps}
        onCancel={this.onCancel}
        onConfirm={this.onConfirm}
        dataQa={dataQa}
      />
    );
  }
}

function mapStateToProps(state) {
  return {
    confirmation: state.confirmation
  };
}

export default connect(mapStateToProps, {hideConfirmationDialog})(ConfirmationContainer);
