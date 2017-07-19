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
import Immutable from 'immutable';
import { Component, PropTypes } from 'react';
import { connect } from 'react-redux';
import HTML5Backend from 'react-dnd-html5-backend';
import { DragDropContext } from 'react-dnd';
import MuiThemeProvider from 'material-ui/styles/MuiThemeProvider';
import getMuiTheme from 'material-ui/styles/getMuiTheme';
import { replace } from 'react-router-redux';
import DocumentTitle from 'react-document-title';
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

DocumentTitle.join = (tokens) => {
  return [...tokens, la('Dremio')].filter(Boolean).join(' - ');
};

function getError(e) {
  if (e instanceof Error) {
    return e;
  }
  // if e is ErrorEvent and error is Error
  if (e.error) {
    return getError(e.error);
  }
  if (e.reason) {
    return getError(e.reason);
  }
  if (e.message) {
    return new Error(e.message + '\n\n' + e.stack + '\n\n(non-Error instance)');
  }
  return new Error(e); // error components expect objects
}

@DragDropContext(HTML5Backend)
class App extends Component {

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
    window.addEventListener('error', this.handleGlobalError);
    window.onunhandledrejection = this.handleUnhandledRejection;

    if (config.shouldEnableBugFiling || config.shouldEnableRSOD) {
      enableFatalPropTypes();
    }
  }

  getChildContext() {
    return {
      location: this.props.location,
      routeParams: this.props.params,
      username: this.props.user.userName,
      loggedInUser: this.props.user
    };
  }

  componentWillReceiveProps(props) {
    App.redirectForServerStatus(props);
  }

  handleGlobalError = (e) => {
    const error = getError(e);
    console.error('Uncaught Error', error);
    this.displayError(error);
  };

  handleUnhandledRejection = (e) => {
    const error = getError(e);

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

    if (config.shouldEnableRSOD) return this.setState({rsodError: error});

    this.props.dispatch(showProdError(error));
  }

  handleDismissError = () => {
    this.setState({rsodError: undefined});
  }

  render() {
    const { children } = this.props;
    return (
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

export default connect(mapStateToProps)(App); // don't add mapDispatchToProps, because we require to have dispatch in component props
