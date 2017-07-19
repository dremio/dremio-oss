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
import { connect } from 'react-redux';
import { Link } from 'react-router';
import Immutable from 'immutable';
import FontIcon from 'components/Icon/FontIcon';
import { CENTER } from 'uiTheme/radium/flexStyle';

import { PALE_NAVY } from 'uiTheme/radium/colors';

import { openQlikSense } from 'actions/explore/download';
import { getQlikAppUrl } from 'sagas/qlik';

import SimpleButton from 'components/Buttons/SimpleButton';
import Modal from 'components/Modals/Modal';
import ModalFooter from 'components/Modals/components/ModalFooter';
import ConfirmCancelFooter from 'components/Modals/ConfirmCancelFooter';
import { hideQlikModal } from 'actions/explore/ui';
import browserUtils from 'utils/browserUtils';

export const VIEW_ID = 'QlikStateModal';

export class QlikStateModal extends Component {

  static propTypes = {
    hide: PropTypes.func,
    qlikDialogVisible: PropTypes.bool,
    qlikShowDialogDataset: PropTypes.instanceOf(Immutable.Map),
    qlikError: PropTypes.instanceOf(Immutable.Map),
    qlikInProgress: PropTypes.bool,
    qlikAppCreationSuccess: PropTypes.bool,
    qlikAppInfo: PropTypes.object,
    showQlikProgress: PropTypes.func,
    openQlikSense: PropTypes.func,
    hideQlikModal: PropTypes.func
  };

  static defaultProps = {
    qlikDialogVisible: false,
    qlikInProgress: false,
    qlikAppCreationSuccess: false
  };

  static ERROR_MESSAGES = {
    //TODO wrap in 'la' loc
    QLIK_GET_APP: 'Failed to get the dataset information from Dremio',
    QLIK_CONNECT_FAILED:
      'We could not connect to Qlik Sense. Please make sure the application is open before retrying.',
    QLIK_DSN:
      'Dremio ODBC DSN could not be found. Make sure that DSN “Dremio Connector” is configured before retrying.',
    QLIK_UNKNOWN: 'We could not connect to Qlik Sense. Please check your settings and try again.',
    QLIK_CUSTOM_ERROR: 'An error occurred while communicating with Qlik Sense:'
  };

  renderErrorInfo() {
    const error = this.props.qlikError.get('error');
    let errorMessage;

    const code = error.get('code');
    errorMessage = QlikStateModal.ERROR_MESSAGES[code];

    if (code === 'QLIK_CUSTOM_ERROR') {
      errorMessage += '\n' + error.get('moreInfo');
    }

    const showIEMessage =
      (code === 'QLIK_CONNECT_FAILED' && ['IE', 'Microsoft Edge'].includes(browserUtils.getPlatform().name));

    return (
      <div style={styles.contentStyle}>
        <FontIcon type='ErrorSolid' iconStyle={{width: 120, height: 120}} style={styles.iconWrap}/>
        <div>
          {errorMessage && errorMessage.split('\n').map((line) => <p>{line}</p>)}

          {
            // TODO: loc
            showIEMessage ? (
              <div style={{marginTop: '1em'}}>
                Internet Explorer and Microsoft Edge may not be able to connect to Qlik Sense without a Windows
                configuration change. {' '}
                <a href='https://docs.dremio.com/client-applications/qlik-sense.html'
                  target='_blank'>
                  {la('Learn more…')}
                </a>
              </div>
            ) : ''
          }
        </div>
      </div>
    );
  }

  renderProgressInfo() {
    return (
      <div style={{...styles.contentStyle, ...styles.contentProgressStyle}}>
        <FontIcon type='NarwhalLogo' iconStyle={{width: 90, height: 90}} style={styles.iconWrapLeft}/>
        <div style={{position: 'relative', flex: 1}}>
          <div style={styles.progressMessage}>{la('Establishing connection to Qlik Sense…')}</div>
          <div style={styles.dashedBehind}></div>
        </div>
        <div style={styles.rightSide}>
          <FontIcon type='Qlik-logo' iconStyle={{width: 80, height: 80}} style={styles.iconWrapRight}/>
        </div>
      </div>
    );
  }

  renderSuccess() {
    const { qlikAppInfo } = this.props;
    const qlikUrl = getQlikAppUrl(qlikAppInfo.appId);

    // TODO: loc with substitution patterns
    return (
      <div style={styles.contentStyle}>
        <FontIcon type='NarwhalLogo' iconStyle={{width: 90, height: 90}} style={styles.iconWrapLeft}/>
        <div style={{position: 'relative'}}>
          <p style={{marginBottom: '1em'}}>
            {la('Your Qlik Sense app')} {' “' + qlikAppInfo.appName + '” '} {la('is ready.')}
          </p>
          <ol style={styles.list}>
            <li>Start Qlik Sense Desktop (or {' '}
              <Link to={qlikUrl} target='_blank'>open in your browser</Link>).</li>
            <li>When you first open the app, click <b>Open</b> to view the <b>Data load editor</b>.</li>
            <li>{la('Confirm the dimensions and measures.')}</li>
            <li>Click on <b>Load data</b>. Once done, you are ready to work with your data.</li>
          </ol>
        </div>
      </div>
    );
  }

  renderModalContent() {
    const { qlikInProgress, qlikError, qlikAppCreationSuccess } = this.props;

    if (qlikInProgress) {
      return this.renderProgressInfo();
    } else if (qlikAppCreationSuccess) {
      return this.renderSuccess();
    } else if (qlikError) {
      return this.renderErrorInfo();
    }
  }

  renderModalFooter() {
    const { qlikInProgress, qlikAppCreationSuccess } = this.props;

    if (qlikAppCreationSuccess) {
      return (
        <ModalFooter>
          <SimpleButton
            data-qa='confirm'
            type='button'
            buttonStyle='primary'
            onClick={this.hide}>{la('Done')}</SimpleButton>
        </ModalFooter>
      );
    }

    return (
      <ConfirmCancelFooter
        canSubmit={!qlikInProgress}
        confirmText={la('Retry')}
        cancelText={la('Cancel')}
        cancel={this.hide}
        confirm={this.retry}/>
    );
  }

  retry = () => {
    this.props.openQlikSense(this.props.qlikShowDialogDataset);
  }

  hide = () => {
    this.props.hideQlikModal();

    if (this.props.hide) {
      this.props.hide();
    }
  }

  render() {
    const { qlikDialogVisible } = this.props;
    return (
      <Modal
        hide={this.hide}
        size='small'
        isOpen={qlikDialogVisible}
        title={la('Connect to Qlik Sense')}>
        <div className='qlik-sense-error' style={styles.base}>
          {this.renderModalContent()}
          {this.renderModalFooter()}
        </div>
      </Modal>
    );
  }
}

function mapStateToProps(state, props) {
  return {
    qlikError: state.explore.ui.get('qlikError'),
    qlikInProgress: state.explore.ui.get('qlikInProgress'),
    qlikDialogVisible: state.explore.ui.get('qlikDialogVisible'),
    qlikShowDialogDataset: state.explore.ui.get('qlikShowDialogDataset'),
    qlikAppCreationSuccess: state.explore.ui.get('qlikAppCreationSuccess'),
    qlikAppInfo: state.explore.ui.get('qlikAppInfo')
  };
}

export default connect(mapStateToProps, {openQlikSense, hideQlikModal})(QlikStateModal);

const styles = {
  base: {
    display: 'flex',
    flexGrow: 1,
    flexDirection: 'column'
  },
  contentStyle: {
    display: 'flex',
    alignItems: 'center',
    height: '100%',
    margin: '0 40px',
    flexGrow: 1
  },
  contentProgressStyle: {
    margin: '30px 40px 0 40px'
  },
  iconWrapLeft: {
    marginRight: 20
  },
  iconWrapRight: {
    marginLeft: 20
  },
  progressMessage: {
    position: 'relative',
    zIndex: 1,
    borderRadius: 10,
    backgroundColor: PALE_NAVY,
    padding: '3px 30px'
  },
  dashedBehind: {
    left: '-10%',
    width: '120%',
    top: 12,
    borderBottom: 'dashed 2px',
    position: 'absolute',
    borderColor: PALE_NAVY
  },
  rightSide: {
    ...CENTER,
    width: 120,
    height: 120
  },
  list: {
    lineHeight: 1.5,
    listStyle: 'decimal outside',
    marginLeft: 20
  }
};
