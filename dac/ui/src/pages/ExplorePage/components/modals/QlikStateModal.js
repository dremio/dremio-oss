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
import { Link } from 'react-router';
import Immutable from 'immutable';
import { injectIntl, FormattedMessage } from 'react-intl';
import FontIcon from 'components/Icon/FontIcon';
import { CENTER } from 'uiTheme/radium/flexStyle';
import { getExploreState } from '@app/selectors/explore';

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

@injectIntl
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
    hideQlikModal: PropTypes.func,
    intl: PropTypes.object.isRequired
  };

  static defaultProps = {
    qlikDialogVisible: false,
    qlikInProgress: false,
    qlikAppCreationSuccess: false
  };

  static ERROR_MESSAGES = {
    QLIK_GET_APP: 'GetAppFailed',
    QLIK_CONNECT_FAILED: 'Qlik.ConnectFailed1',
    QLIK_DSN: 'DSNNotFound',
    QLIK_UNKNOWN: 'Qlik.ConnectFailed2',
    QLIK_CUSTOM_ERROR: 'Qlik.CustomError'
  };

  renderErrorInfo() {
    const error = this.props.qlikError.get('error');
    const code = error.get('code');
    const errorMessage = (code === 'QLIK_CUSTOM_ERROR')
      ? <div>
        <p><FormattedMessage id={QlikStateModal.ERROR_MESSAGES[code]} /></p>
        <p>{error.get('moreInfo')}</p>
      </div>
      : <FormattedMessage id={QlikStateModal.ERROR_MESSAGES[code]} />;

    const showIEMessage =
      (code === 'QLIK_CONNECT_FAILED' && browserUtils.isMSBrowser());

    return (
      <div style={styles.contentStyle}>
        <FontIcon type='ErrorSolid' iconStyle={{width: 120, height: 120}} style={styles.iconWrap}/>
        <div>
          {errorMessage}
          {
            showIEMessage ? (
              <div style={{marginTop: '1em'}}>
                <FormattedMessage id='Qlik.IEEdgeErrors' />
                <a href='https://docs.dremio.com/client-applications/qlik-sense.html'
                  target='_blank'>
                  {this.props.intl.formatMessage({ id: 'Common.LearnMore' })}
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
          <div style={styles.progressMessage}>
            {this.props.intl.formatMessage({ id: 'Qlik.EstablishingConnection' })}
          </div>
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
            <FormattedMessage id='Qlik.AppReady' values={{ appName: qlikAppInfo.appName }} />
          </p>
          <ol style={styles.list}>
            <li>Start Qlik Sense Desktop (or {' '}
              <Link to={qlikUrl} target='_blank'>open in your browser</Link>).</li>
            <li>When you first open the app, click <b>Open</b> to view the <b>Data load editor</b>.</li>
            <li><FormattedMessage id='Qlik.ConfirmDimAndMes' /></li>
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
    const { qlikInProgress, qlikAppCreationSuccess, intl } = this.props;

    if (qlikAppCreationSuccess) {
      return (
        <ModalFooter>
          <SimpleButton
            data-qa='confirm'
            type='button'
            buttonStyle='primary'
            onClick={this.hide}
          >
            <FormattedMessage id='Common.Done' />
          </SimpleButton>
        </ModalFooter>
      );
    }

    return (
      <ConfirmCancelFooter
        canSubmit={!qlikInProgress}
        confirmText={intl.formatMessage({ id: 'Common.Retry' })}
        cancelText={intl.formatMessage({ id: 'Common.Cancel' })}
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
    const { qlikDialogVisible, intl } = this.props;
    return (
      <Modal
        hide={this.hide}
        size='small'
        isOpen={qlikDialogVisible}
        title={intl.formatMessage({ id: 'Qlik.Connect' })}>
        <div className='qlik-sense-error' style={styles.base}>
          {this.renderModalContent()}
          {this.renderModalFooter()}
        </div>
      </Modal>
    );
  }
}

function mapStateToProps(state, props) {
  const data = getExploreState(state);
  const ui = data ? data.ui : new Immutable.Map(); //todo explore page state should not be here
  return {
    qlikError: ui.get('qlikError'),
    qlikInProgress: ui.get('qlikInProgress'),
    qlikDialogVisible: ui.get('qlikDialogVisible'),
    qlikShowDialogDataset: ui.get('qlikShowDialogDataset'),
    qlikAppCreationSuccess: ui.get('qlikAppCreationSuccess'),
    qlikAppInfo: ui.get('qlikAppInfo')
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
