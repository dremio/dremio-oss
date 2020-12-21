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
import Immutable from 'immutable';
import { Component, Fragment } from 'react';
import { compose } from 'redux';
import PropTypes from 'prop-types';
import { connect } from 'react-redux';
import { MuiThemeProvider, createMuiTheme } from '@material-ui/core/styles';
import { replace } from 'react-router-redux';
import DocumentTitle from 'react-document-title';
import urlParse from 'url-parse';

import {showAppError} from '@app/actions/prodError';
import { DnDContextDecorator } from '@app/components/DragComponents/DnDContextDecorator';
import { ErrorBoundary } from '@app/components/ErrorBoundary';
import { Suspense } from '@app/components/Lazy';

import socket from '@app/utils/socket';
import sentryUtil from '@app/utils/sentryUtil';
import {formatMessage} from '@app/utils/locale';

import { SERVER_STATUS_OK } from '@app/constants/serverStatus';
import config from 'dyn-load/utils/config';
import enableFatalPropTypes from '@app/enableFatalPropTypes';

import ModalsContainer from '@app/components/Modals/ModalsContainer';
import AboutModal from '@app/pages/HomePage/components/modals/AboutModal/AboutModal';
import NotificationContainer from '@app/containers/Notification';
import ConfirmationContainer from '@app/containers/Confirmation';
import ProdErrorContainer from '@app/containers/ProdError';
import DevErrorContainer from '@app/containers/DevError';
import {LocationProvider} from '@app/containers/dremioLocation';
import { withHookProvider } from '@app/containers/RouteLeave';

DocumentTitle.join = (tokens) => {
  return [...tokens, formatMessage('App.Dremio')].filter(Boolean).join(' - ');
};

const theme = createMuiTheme({
  palette: {
    primary: {
      main: 'rgb(0, 188, 212)'
    }
  },
  overrides: {
    MuiSwitch: {
      switchBase: {
        height: 'auto'
      }
    }
  }
});

export class App extends Component {

  static propTypes = {
    location: PropTypes.object,
    params: PropTypes.object,
    //connected
    user: PropTypes.object,
    serverStatus: PropTypes.instanceOf(Immutable.Map),
    shouldEnableRSOD: PropTypes.bool,
    dispatch: PropTypes.func.isRequired
  };

  static childContextTypes = {
    location: PropTypes.object,
    routeParams: PropTypes.object,
    username: PropTypes.string,
    loggedInUser: PropTypes.object
  };

  static redirectForServerStatus(props) {
    const {location} = props;
    if (props.serverStatus.get('status') !== SERVER_STATUS_OK && location.pathname !== '/status') {
      props.dispatch(
        replace('/status?redirect=' + encodeURIComponent(props.location.pathname + props.location.search))
      );
    }
  }

  constructor(props) {
    super(props);

    // use window.onerror here instead of addEventListener('error') because ErrorEvent.error is
    // experimental according to mdn. Can get both file url and error from either.
    window.onerror = this.handleGlobalError.bind(this, window.onerror);
    window.onunhandledrejection = this.handleUnhandledRejection;

    if (config.shouldEnableBugFiling || props.shouldEnableRSOD) {
      enableFatalPropTypes();
    }
  }

  getChildContext() {
    return {
      location: this.props.location, // todo remove
      routeParams: this.props.params,
      username: this.props.user.userName,
      loggedInUser: this.props.user
    };
  }

  componentDidMount() {
    socket.dispatch = this.props.dispatch;
  }

  componentWillReceiveProps(props) {
    App.redirectForServerStatus(props);
  }

  handleGlobalError = (prevOnerror, msg, url, lineNo, columnNo, error) => {
    prevOnerror && prevOnerror.call(window, msg, url, lineNo, columnNo, error);

    // there is no URL for external scripts (at least in Chrome)
    if (!url || urlParse(url).origin !== this._getWindowOrigin()) return;

    console.error('Uncaught Error', error || msg);
    this.displayError(error || msg);
  };

  handleUnhandledRejection = (rejectionEvent) => {
    const error = rejectionEvent.reason;
    if (!error) return;

    if (error.stack && this._shouldIgnoreExternalStack(error.stack)) return;

    //By default, Raven.js does not capture unhandled promise rejections.
    sentryUtil.logException(error);

    console.error('UnhandledRejection', error);
    this.displayError(error);
  };

  displayError(error) {
    this.props.dispatch(showAppError(error));
  }

  _getWindowOrigin() {
    return window.location.origin;
  }

  _shouldIgnoreExternalStack(stack) {
    const stackOrigin = this._getSourceOriginFromStack(stack);
    if (stackOrigin && stackOrigin !== this._getWindowOrigin()) {
      console.warn('Ignoring js error from origin: ' + stackOrigin);
      return true;
    }
    return false;
  }

  /**
   * Returns the top-most origin found in the stack. We assume we have correctly used remotely included scripts and any
   * errors they throw are not our fault (or at least not catastrophic)
   * @param stack string of stack trace
   * @private
   */
  _getSourceOriginFromStack(stack) {
    if (!stack) return;

    // This works in Chrome, Firefox, IE and Edge.
    const lines = stack.split('\n');
    for (let i = 0; i < lines.length; i++) {
      const m = lines[i].match(/(https?:\/\/.*?)\//);
      if (m) {
        return m[1];
      }
    }
  }

  render() {
    const { children, shouldEnableRSOD } = this.props;
    return (
      <Fragment>
        <ErrorBoundary>
          <Suspense>
            <LocationProvider location={this.props.location}>
              <div style={{height: '100%'}}>
                <MuiThemeProvider theme={theme}>
                  {children}
                </MuiThemeProvider>
                <NotificationContainer/>
                <ConfirmationContainer/>
                <ModalsContainer modals={{AboutModal}} />
              </div>
            </LocationProvider>
          </Suspense>
        </ErrorBoundary>
        {
          shouldEnableRSOD ?
            <DevErrorContainer /> :
            <ProdErrorContainer />
        }
      </Fragment>
    );
  }
}

App.propTypes = {
  children: PropTypes.node,
  routeParams: PropTypes.object
};

function mapStateToProps(state) {
  const user = state.account ? state.account.get('user').toJS() : {};

  // quick workaround pending server moving this value to window.config
  config.showUserAndUserProperties = user.showUserAndUserProperties;

  return {
    user,
    serverStatus: state.serverStatus,
    shouldEnableRSOD: config.shouldEnableRSOD
  };
}

export default compose(
  withHookProvider,
  connect(mapStateToProps),
  DnDContextDecorator
)(App);
