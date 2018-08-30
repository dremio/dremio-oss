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
import Immutable from 'immutable';
import { Component } from 'react';
import PropTypes from 'prop-types';
import { connect } from 'react-redux';
import HTML5Backend from 'react-dnd-html5-backend';
import { DragDropContext } from 'react-dnd';
import MuiThemeProvider from 'material-ui/styles/MuiThemeProvider';
import getMuiTheme from 'material-ui/styles/getMuiTheme';
import { replace } from 'react-router-redux';
import DocumentTitle from 'react-document-title';
import urlParse from 'url-parse';
import { showProdError } from 'actions/prodError';
import { boot } from 'actions/app';

import socket from 'utils/socket';
import sentryUtil from 'utils/sentryUtil';

import { SERVER_STATUS_OK } from 'constants/serverStatus';
import config from 'utils/config';
import enableFatalPropTypes from 'enableFatalPropTypes';

import ModalsContainer from 'components/Modals/ModalsContainer';
import AboutModal from 'pages/HomePage/components/modals/AboutModal';
import NotificationContainer from 'containers/Notification';
import ConfirmationContainer from 'containers/Confirmation';
import ProdErrorContainer from 'containers/ProdError';
import DevErrorContainer from 'containers/DevError';
import { LocationProvider } from 'containers/dremioLocation';
import { formatMessage } from '../utils/locale';

DocumentTitle.join = (tokens) => {
  return [...tokens, formatMessage('App.Dremio')].filter(Boolean).join(' - ');
};

function getError(e) {
  if (e instanceof Error) {
    return e;
  }
  if (e.message) {
    return new Error(e.message + '\n\n' + e.stack + '\n\n(non-Error instance)');
  }
  return new Error(e); // error components expect objects
}

export class App extends Component {

  static propTypes = {
    user: PropTypes.object,
    location: PropTypes.object,
    params: PropTypes.object,
    dispatch: PropTypes.func,
    serverStatus: PropTypes.instanceOf(Immutable.Map)
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

    socket.dispatch = props.dispatch;

    props.dispatch(boot());

    this.state = {
      hasShownError: false,
      rsodError: undefined
    };
    // use window.onerror here instead of addEventListener('error') because ErrorEvent.error is
    // experimental according to mdn. Can get both file url and error from either.
    window.onerror = this.handleGlobalError.bind(this, window.onerror);
    window.onunhandledrejection = this.handleUnhandledRejection;

    if (config.shouldEnableBugFiling || config.shouldEnableRSOD) {
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
    // once we've shown one error, the app is unstable - don't bother showing more
    // also prevents render feedback loop from re-triggering the error because we re-render
    if (this.state.hasShownError) return;
    this.setState({hasShownError: true});

    const errorObject = getError(error);

    if (config.shouldEnableRSOD) return this.setState({rsodError: errorObject});

    this.props.dispatch(showProdError(errorObject));
  }

  handleDismissError = () => {
    this.setState({rsodError: undefined});
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
    const { children } = this.props;
    return (
      <LocationProvider location={this.props.location}>
        <div style={{height: '100%'}}>
          <MuiThemeProvider muiTheme={getMuiTheme()}>
            {children}
          </MuiThemeProvider>
          {
            config.shouldEnableRSOD &&
              <DevErrorContainer error={this.state.rsodError} onDismiss={this.handleDismissError}/>
          }
          <NotificationContainer/>
          <ConfirmationContainer/>
          <ProdErrorContainer/>
          <ModalsContainer modals={{AboutModal}} style={{height: 0}}/>
        </div>
      </LocationProvider>
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
    serverStatus: state.serverStatus
  };
}

// don't add mapDispatchToProps, because we require to have dispatch in component props
export default connect(mapStateToProps)(DragDropContext(HTML5Backend)(App));
