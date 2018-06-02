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

import { FormattedMessage, injectIntl } from 'react-intl';

import { formDescription } from 'uiTheme/radium/typography';

import Modal from 'components/Modals/Modal';
import Art from 'components/Art';

import { getEdition } from 'dyn-load/utils/versionUtils';
import config from 'utils/config';
import timeUtils from 'utils/timeUtils';

@injectIntl
export default class AboutModal extends Component {

  static propTypes = {
    isOpen: PropTypes.bool,
    hide: PropTypes.func,
    intl: PropTypes.object.isRequired
  };

  renderVersion() {
    const buildTime = timeUtils.formatTime(config.versionInfo.buildTime);
    const commitTime = timeUtils.formatTime(config.versionInfo.commitTime);

    return <dl className='largerFontSize'>
      <dt style={styles.dtStyle}><FormattedMessage id='App.Build'/></dt>
      <dd>{config.versionInfo.version}</dd>

      <dt style={styles.dtStyle}><FormattedMessage id='App.Edition'/></dt>
      <dd>{getEdition()}</dd>

      <dt style={styles.dtStyle}><FormattedMessage id='App.BuildTime'/></dt>
      <dd>{buildTime}</dd>

      <dt style={styles.dtStyle}><FormattedMessage id='App.ChangeHash'/></dt>
      <dd>{config.versionInfo.commitHash}</dd>

      <dt style={styles.dtStyle}><FormattedMessage id='App.ChangeTime'/></dt>
      <dd>{commitTime}</dd>
    </dl>;
  }

  render() {
    const { isOpen, hide } = this.props;

    return (
      <Modal
        size='small'
        title={this.props.intl.formatMessage({id: 'App.AboutHeading'})}
        isOpen={isOpen}
        hide={hide}>
        <div style={styles.container}>
          <div style={styles.logoPane}>
            <Art className='dremioLogo' src='NarwhalLogo.svg' style={{width: 150}} alt={la('Dremio Narwhal')} />
          </div>
          <div style={styles.pane}>
            <div style={{flex: 1}}>
              <div style={{fontSize: '2em', marginBottom: 10}}><FormattedMessage id='App.Dremio' /></div>
              <div>
                {this.renderVersion()}
              </div>
            </div>
            <div style={formDescription}>
              {<FormattedMessage id='App.Copyright' />}
            </div>
          </div>
        </div>
      </Modal>
    );
  }
}

const styles = {
  container: {
    display: 'flex',
    width: '100%',
    height: '100%',
    padding: 20
  },

  pane: {
    flex: 2,
    marginLeft: '20px',
    display: 'flex',
    flexDirection: 'column'
  },

  logoPane: {
    flex: 1,
    alignItems: 'center',
    justifyContent: 'center',
    display: 'flex'
  },

  dtStyle: {
    fontWeight: 'bold',
    marginTop: 15,
    fontSize: '14px'
  }
};
