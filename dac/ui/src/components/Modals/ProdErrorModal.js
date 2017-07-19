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
import Modal from 'components/Modals/Modal';
import { modalContent } from 'uiTheme/radium/modal';
import fileABug from 'utils/fileABug';
import FontIcon from 'components/Icon/FontIcon';
import SimpleButton from 'components/Buttons/SimpleButton';

import { modalFooter } from 'uiTheme/radium/modal';

export default class ProdErrorModal extends Component {

  static propTypes = {
    error: PropTypes.object.isRequired,
    onHide: PropTypes.func,
    showGoHome: PropTypes.bool,
    showFileABug: PropTypes.bool
  };

  render() {
    const {
      error,
      showGoHome,
      showFileABug
    } = this.props;

    return (
      <Modal
        isOpen
        onClickCloseButton={this.props.onHide /* restrict closing to clicking close, instead of clicking off modal */}
        classQa='prod-error-modal'
        size='smallest'
        title={la('An Unexpected Error Occurred')}
      >
        <div style={{...modalContent, ...styles.wrapper}}>
          <div style={styles.leftSide}>
            <FontIcon type='Error' iconStyle={{width: 60, height: 60}}/>
          </div>
          <div style={styles.content}>
            {la('If the problem persists, please contact support.')}
          </div>
        </div>

        <div style={modalFooter}>
          {
            showGoHome &&
              <SimpleButton
                data-qa='goHome'
                type='button'
                buttonStyle='secondary'
                onClick={() => window.location = '/'}>{la('Go Home')}</SimpleButton>
          }
          {
            showFileABug &&
              <SimpleButton
                data-qa='fileABug'
                type='button'
                buttonStyle='secondary'
                onClick={() => fileABug(error)}>{la('File a Bug')}</SimpleButton>
          }
          <SimpleButton
            data-qa='reload'
            type='button'
            buttonStyle='primary'
            onClick={() => window.location.reload()}>{la('Reload')}</SimpleButton>
        </div>
      </Modal>
    );
  }
}

const styles = {
  wrapper: {
    flexDirection: 'row',
    alignItems: 'center'
  },
  leftSide: {
    padding: 10,
    width: 80
  }
};
