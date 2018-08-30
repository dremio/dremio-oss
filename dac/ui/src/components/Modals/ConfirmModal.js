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
import invariant from 'invariant';
import Modal from 'components/Modals/Modal';
import ConfirmCancelFooter from 'components/Modals/ConfirmCancelFooter';
import { Checkbox, TextField } from 'components/Fields';

import { confirmBodyText, modalContent } from 'uiTheme/radium/modal';
import localStorageUtils from 'utils/storageUtils/localStorageUtils';
import Keys from 'constants/Keys.json';

export default class ConfirmModal extends Component {

  static propTypes = {
    isOpen: PropTypes.bool,
    hideCancelButton: PropTypes.bool,
    hideCloseButton: PropTypes.bool,
    showOnlyConfirm: PropTypes.bool,
    onConfirm: PropTypes.func.isRequired,
    onCancel: PropTypes.func.isRequired,
    title: PropTypes.string.isRequired,
    text: PropTypes.node.isRequired,
    confirmText: PropTypes.string,
    cancelText: PropTypes.string,
    doNotAskAgainText: PropTypes.string,
    doNotAskAgainKey: PropTypes.string,
    showPrompt: PropTypes.bool,
    promptFieldProps: PropTypes.object,
    dataQa: PropTypes.string
  };

  static defaultProps = {
    hideCancelButton: false,
    showOnlyConfirm: false,
    hideCloseButton: false
  };

  state = {
    promptValue: undefined,
    doNotAskAgain: false
  };

  componentWillMount() {
    const { promptFieldProps, showPrompt, doNotAskAgainKey } = this.props;

    invariant(
      promptFieldProps ? showPrompt : true, 'must set showPrompt if setting promptFieldProps'
    );
    invariant(!showPrompt || !doNotAskAgainKey, 'prompt and doNotAskAgain are mutually exclusive');

    const stored = localStorageUtils.getCustomValue('doNotAskAgain-' + this.props.doNotAskAgainKey);
    if (this.props.doNotAskAgainKey && stored) {
      this.props.onConfirm();
    }
  }

  onConfirm = () => {
    if (this.props.doNotAskAgainKey && this.state.doNotAskAgain) {
      localStorageUtils.setCustomValue('doNotAskAgain-' + this.props.doNotAskAgainKey, true);
    }

    this.props.onConfirm(this.state.promptValue);
  }

  render() {
    const {
      isOpen,
      title,
      text,
      onCancel,
      confirmText,
      cancelText,
      showOnlyConfirm,
      doNotAskAgainText,
      doNotAskAgainKey,
      showPrompt,
      promptFieldProps,
      dataQa
    } = this.props;
    const hideCancel = this.props.hideCancelButton || showOnlyConfirm;
    const onHide = showOnlyConfirm ? () => {} : onCancel;
    const hideCloseButton = showOnlyConfirm || this.props.hideCloseButton;

    let bodyElements = Array.isArray(text) ? [...text] : [text];

    if (showPrompt) {
      bodyElements.push(
        <TextField initialFocus {...promptFieldProps}
          onChange={(event) => {
            this.setState({ promptValue: event.target.value });
          }}
          onKeyDown={(event) => {
            if (event.keyCode === Keys.ENTER) {
              this.onConfirm();
            }
          }}
        />
      );
    }

    if (doNotAskAgainKey && doNotAskAgainText) {
      bodyElements.push(<Checkbox
        checked={this.state.doNotAskAgain}
        onChange={() => {
          this.setState({ doNotAskAgain: !this.state.doNotAskAgain });
        }}
        label={doNotAskAgainText}
      />);
    }

    bodyElements = bodyElements.map((item, i) => {
      return <p key={i} style={{marginBottom: i < bodyElements.length - 1 ? '1em' : 0}}>{item}</p>;
    });
    bodyElements = showPrompt ? <label>{bodyElements}</label> : bodyElements;
    return (
      <Modal
        isOpen={isOpen}
        hide={onHide}
        hideCloseButton={hideCloseButton}
        classQa='confirm-modal'
        dataQa={dataQa}
        size='smallest'
        title={title || la('Confirm')}
      >
        <div style={{...modalContent, ...confirmBodyText}}>
          {bodyElements}
        </div>
        <ConfirmCancelFooter
          hideCancel={hideCancel}
          confirm={this.onConfirm}
          confirmText={confirmText || la('OK')}
          cancelText={cancelText || la('Cancel')}
          cancel={onCancel}
        />
      </Modal>
    );
  }
}
