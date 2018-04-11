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

import pureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import Radium from 'radium';

import SimpleButton from 'components/Buttons/SimpleButton';

import { modalFooter } from 'uiTheme/radium/modal';

@pureRender
@Radium
export default class ConfirmCancelFooter extends Component {

  static defaultProps = {
    confirmText: 'Save',
    cancelText: 'Cancel',
    canSubmit: true,
    modalFooter: true
  };

  static propTypes = {
    confirmText: PropTypes.string,
    cancelText: PropTypes.string,
    submitForm: PropTypes.bool,
    submitting: PropTypes.bool,
    canSubmit: PropTypes.bool,
    hideCancel: PropTypes.bool,
    confirm: PropTypes.func, // optional because you can use type=submit
    cancel: PropTypes.func,
    modalFooter: PropTypes.bool,
    footerChildren: PropTypes.node,
    style: PropTypes.object
  };

  onCancel = (e) => {
    e.preventDefault();
    this.props.cancel();
  }

  onConfirm = (e) => {
    if (this.props.confirm) {
      e.preventDefault();
      this.props.confirm();
    }
  }

  render() {
    const { confirmText, cancel, cancelText, submitForm, submitting, canSubmit, hideCancel, footerChildren } = this.props;
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
              onClick={this.onCancel}>{cancelText}</SimpleButton>
        }
        <SimpleButton
          data-qa='confirm'
          type={submitForm ? 'submit' : undefined}
          buttonStyle='primary'
          submitting={submitting}
          disabled={!canSubmit}
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
