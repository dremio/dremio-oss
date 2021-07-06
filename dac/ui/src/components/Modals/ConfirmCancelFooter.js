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
import Radium from 'radium';

import SimpleButton from 'components/Buttons/SimpleButton';

import { modalFooter } from 'uiTheme/radium/modal';
import { ConfirmCancelFooterWithMixin } from '@inject/components/Modals/ConfirmCancelFooterMixin.js';

@Radium
export class ConfirmCancelFooter extends Component {

  static defaultProps = {
    confirmText: 'Save',
    cancelText: 'Cancel',
    canSubmit: true,
    canCancel: true,
    modalFooter: true,
    confirmButtonStyle: 'primary'
  };

  static propTypes = {
    confirmText: PropTypes.string,
    cancelText: PropTypes.string,
    submitForm: PropTypes.bool,
    submitting: PropTypes.bool,
    canSubmit: PropTypes.bool,
    canCancel: PropTypes.bool,
    hideCancel: PropTypes.bool,
    confirm: PropTypes.func, // optional because you can use type=submit
    cancel: PropTypes.func,
    modalFooter: PropTypes.bool,
    footerChildren: PropTypes.node,
    style: PropTypes.object,
    confirmButtonStyle: PropTypes.string
  };

  onCancel = (e) => {
    if (this.props.canCancel) {
      e.preventDefault();
      this.props.cancel();
    }
  };

  onConfirm = (e) => {
    if (this.props.confirm) {
      e.preventDefault();
      this.props.confirm();
    }
  };

  render() {
    const {
      confirmText,
      confirmButtonStyle,
      cancel,
      cancelText,
      submitForm,
      submitting,
      canSubmit,
      canCancel,
      hideCancel,
      footerChildren
    } = this.props;
    const conditionalRenderingButtonStyling = this.checkToRenderSaveAndCancelButtons();
    return (
      <div className='confirm-cancel-footer'
        style={[this.props.modalFooter ? modalFooter : styles.nonModalFooter, styles.base, this.props.style]}>
        <div style={{alignSelf: 'center'}}>{footerChildren}</div>
        {
          cancel && !hideCancel &&
            <SimpleButton
              data-qa='cancel'
              type='button'
              buttonStyle='secondary'
              disabled={!canCancel}
              // style={conditionalRenderingButtonStyling}  // Uncomment this line in the case you want conditional rendering of 'cancel' button based on canAlter permissions
              onClick={this.onCancel}>{this.checkCancelText(cancelText)}
            </SimpleButton>
        }
        <SimpleButton
          data-qa='confirm'
          type={submitForm ? 'submit' : undefined}
          buttonStyle={confirmButtonStyle}
          submitting={submitting}
          disabled={!canSubmit}
          style={conditionalRenderingButtonStyling}
          onClick={this.onConfirm}>{confirmText}</SimpleButton>
      </div>
    );
  }
}

const styles = {
  base: {
    display: 'flex',
    justifyContent: 'flex-end'
  },
  nonModalFooter: {
    marginTop: 20,
    marginRight: 11
  }
};

export default ConfirmCancelFooterWithMixin(ConfirmCancelFooter);
