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
import { Component } from 'react';
import PropTypes from 'prop-types';
import invariant from 'invariant';
import Grid from '@material-ui/core/Grid';
import Modal from 'components/Modals/Modal';
import ConfirmCancelFooter from 'components/Modals/ConfirmCancelFooter';
import { Checkbox, TextField } from 'components/Fields';
import { Label } from 'dremio-ui-lib';

import { confirmBodyText, modalContent } from 'uiTheme/radium/modal';
import localStorageUtils from 'utils/storageUtils/localStorageUtils';
import Keys from '@app/constants/Keys.json';

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
    confirmButtonStyle: PropTypes.string,
    cancelText: PropTypes.string,
    doNotAskAgainText: PropTypes.string,
    doNotAskAgainKey: PropTypes.string,
    promptLabel: PropTypes.string,
    showPrompt: PropTypes.bool,
    promptFieldProps: PropTypes.object,
    dataQa: PropTypes.string,
    validatePromptText: PropTypes.func
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
    const {
      state: {
        promptValue,
        doNotAskAgain
      },
      props: {
        validatePromptText,
        onConfirm,
        doNotAskAgainKey
      }
    } = this;
    if (doNotAskAgainKey && doNotAskAgain) {
      localStorageUtils.setCustomValue('doNotAskAgain-' + doNotAskAgainKey, true);
    }

    if (validatePromptText && !validatePromptText(promptValue)) {
      return;
    }
    onConfirm(promptValue);
  }

  renderPrompt() {
    const {
      promptFieldProps,
      promptLabel
    } = this.props;
    return (
      <>
        {promptLabel && <Label value={promptLabel} />}
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
      </>
    );
  }

  renderDonotAskAgainCheckbox() {
    const {
      doNotAskAgainText
    } = this.props;

    return (
      <Checkbox
        checked={this.state.doNotAskAgain}
        onChange={() => {
          this.setState({ doNotAskAgain: !this.state.doNotAskAgain });
        }}
        label={doNotAskAgainText}
      />
    );
  }

  renderBody() {
    const {
      text,
      doNotAskAgainText,
      doNotAskAgainKey,
      showPrompt
    } = this.props;

    let textRenderer = text;
    if (Array.isArray(text)) {
      textRenderer = text.map((textVal, index) => {
        return (
          <p className={index < text.length - 1 ? 'margin-bottom--double' : ''}>
            {textVal}
          </p>
        );
      });
    }

    return (
      <Grid
        container
        direction='column'
        alignItems='stretch'
        justify='space-evenly'
        classes={{ root: 'full-height' }}
      >
        <Grid item>
          {textRenderer}
        </Grid>
        {showPrompt &&
          <Grid item>
            {this.renderPrompt()}
          </Grid>
        }
        {doNotAskAgainKey && doNotAskAgainText &&
          <Grid item>
            {this.renderDonotAskAgainCheckbox()}
          </Grid>
        }
      </Grid>
    );
  }

  render() {
    const {
      isOpen,
      title,
      onCancel,
      confirmText,
      confirmButtonStyle,
      cancelText,
      showOnlyConfirm,
      showPrompt,
      dataQa,
      validatePromptText
    } = this.props;
    const hideCancel = this.props.hideCancelButton || showOnlyConfirm;
    const onHide = showOnlyConfirm ? () => {} : onCancel;
    const hideCloseButton = showOnlyConfirm || this.props.hideCloseButton;
    let canSubmit = true;

    if (showPrompt && validatePromptText) {
      canSubmit =  validatePromptText(this.state.promptValue);
    }

    return (
      <Modal
        isOpen={isOpen}
        hide={onHide}
        hideCloseButton={hideCloseButton}
        classQa='confirm-modal'
        dataQa={dataQa}
        size='smallest'
        title={title || la('Confirm')}
        style={showPrompt ? { height: '275px' } : {}}
      >
        <div style={{...modalContent, ...confirmBodyText}}>
          {this.renderBody()}
        </div>
        <ConfirmCancelFooter
          hideCancel={hideCancel}
          confirm={this.onConfirm}
          confirmText={confirmText || la('OK')}
          confirmButtonStyle={confirmButtonStyle}
          cancelText={cancelText || la('Cancel')}
          cancel={onCancel}
          canSubmit={canSubmit}
        />
      </Modal>
    );
  }
}
