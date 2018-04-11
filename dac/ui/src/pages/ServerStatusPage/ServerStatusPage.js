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
import Immutable from 'immutable';
import { connect }   from 'react-redux';
import { replace } from 'react-router-redux';
import urlParse from 'url-parse';
import moment from 'moment';

import Radium from 'radium';
import pureRender from 'pure-render-decorator';

import PropTypes from 'prop-types';

import LoginTitle from 'pages/AuthenticationPage/components/LoginTitle';
import SimpleButton from 'components/Buttons/SimpleButton';

import { getViewState } from 'selectors/resources';

import { SERVER_STATUS_OK } from 'constants/serverStatus';

import {LIGHT_GREY} from 'uiTheme/radium/colors';

import {
  scheduleCheckServerStatus,
  unscheduleCheckServerStatus,
  manuallyCheckServerStatus,
  CHECK_SERVER_STATUS_VIEW_ID
} from 'actions/serverStatus';

const COUNTDOWN_UPDATE_INTERVAL = 100;

@Radium
@pureRender
export class ServerStatusPage extends Component {
  static propTypes = {
    serverStatus: PropTypes.instanceOf(Immutable.Map),
    scheduleCheckServerStatus: PropTypes.func.isRequired,
    unscheduleCheckServerStatus: PropTypes.func.isRequired,
    manuallyCheckServerStatus: PropTypes.func.isRequired,
    checkViewState: PropTypes.instanceOf(Immutable.Map)
  };

  static redirectIfStatusOk(props) {
    if (props.serverStatus.get('status') === SERVER_STATUS_OK) {
      if (typeof window !== 'undefined') {
        const query = urlParse(window.location.href, true).query;
        const url = query.redirect;
        if (url) {
          props.replace(url);
        }
      }
    }
  }

  constructor(props) {
    super(props);
    ServerStatusPage.redirectIfStatusOk(this.props);
  }

  componentDidMount() {
    this.props.scheduleCheckServerStatus();
    this.interval = setInterval(() => {
      this.forceUpdate();
    }, COUNTDOWN_UPDATE_INTERVAL);
  }

  componentWillReceiveProps(nextProps) {
    ServerStatusPage.redirectIfStatusOk(nextProps);
  }

  componentWillUnmount() {
    this.props.unscheduleCheckServerStatus();
    clearInterval(this.interval);
  }

  onUpdateClick = () => {
    this.props.manuallyCheckServerStatus();
  };

  getSubTitle() {
    if (this.props.serverStatus.get('status') === SERVER_STATUS_OK) {
      return la('Everything is OK. Carry on.');
    }
    return la('Oops, something is wrong with the server.');
  }

  renderCheckTime() {
    const {serverStatus} = this.props;
    const lastCheckMoment = serverStatus.get('lastCheckMoment');
    if (lastCheckMoment) {
      const d = Math.round(moment.duration(serverStatus.get('delay') - moment().diff(lastCheckMoment)).asSeconds());
      if (d) {
        return <div style={styles.checking}>Checking in {d} seconds.</div>;
      }
      return <div style={styles.checking}>Checking...</div>;
    }
  }

  render() {
    return (
      <div id='status-page' style={styles.base}>
        <div style={styles.content}>
          <LoginTitle subTitle={this.getSubTitle()}/>
          <SimpleButton
            disabled={this.props.checkViewState.get('isInProgress')}
            buttonStyle='primary'
            onClick={this.onUpdateClick}>
            Check Now
          </SimpleButton>
          {this.renderCheckTime()}
        </div>
      </div>
    );
  }
}

function mapStateToProps(state) {
  return {
    serverStatus: state.serverStatus,
    checkViewState: getViewState(CHECK_SERVER_STATUS_VIEW_ID)
  };
}

const styles = {
  base: {
    display: 'flex',
    justifyContent: 'center',
    alignItems: 'center',
    backgroundColor: '#2A394A',
    width: '100vw',
    height: '100vh',
    overflow: 'hidden'
  },
  content: {
    position: 'relative',
    backgroundColor: '#344253',
    minWidth: 775,
    minHeight: 430,
    maxWidth: 775,
    maxHeight: 430,
    overflow: 'hidden',
    padding: 40
  },
  subtitle: {
    color: '#43B8C9',
    fontSize: 27
  },
  mainTitle: {
    display: 'flex',
    alignItems: 'center'
  },
  theme: {
    Icon: {
      width: 240,
      height: 75
    }
  },
  checking: {
    color: LIGHT_GREY,
    marginTop: 10
  }
};

export default connect(
  mapStateToProps, {
    replace,
    scheduleCheckServerStatus,
    unscheduleCheckServerStatus,
    manuallyCheckServerStatus
  }
)(ServerStatusPage);
