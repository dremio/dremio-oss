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
import Immutable from "immutable";
import { Component, Fragment } from "react";
import { compose } from "redux";
import PropTypes from "prop-types";
import { connect } from "react-redux";
import {
  ThemeProvider,
  StyledEngineProvider,
  createTheme,
} from "@mui/material/styles";
import { replace } from "react-router-redux";
import DocumentTitle from "react-document-title";

import { showAppError } from "@app/actions/prodError";
import { DnDContextDecorator } from "@app/components/DragComponents/DnDContextDecorator";
import { Suspense } from "@app/components/Lazy";

import socket from "@inject/utils/socket";
import sentryUtil from "@app/utils/sentryUtil";
import { formatMessage } from "@app/utils/locale";

import { SERVER_STATUS_OK } from "@app/constants/serverStatus";

import ModalsContainer from "@app/components/Modals/ModalsContainer";
import PATModalContainer from "dyn-load/containers/PATModalContainer";
import AccountSettingsModalContainer from "@app/containers/AccountSettingsModalContainer";
import AboutModal from "@app/pages/HomePage/components/modals/AboutModal/AboutModal";
import AppHOC from "@inject/containers/AppHOC";
import NotificationContainer from "@app/containers/Notification";
import ConfirmationContainer from "@app/containers/Confirmation";
import ProdErrorContainer from "@app/containers/ProdError";
import { LocationProvider } from "@app/containers/dremioLocation";
import { withHookProvider } from "@app/containers/RouteLeave";
import { isNotSoftware } from "dyn-load/utils/versionUtils";

import { themeStyles } from "dremio-ui-lib";
import "../uiTheme/css/react-datepicker.css";
import "../uiTheme/css/dialog-polyfill.css";
import "../uiTheme/css/leantable.css";
import "../uiTheme/css/fa.css";

DocumentTitle.join = (tokens) => {
  return [...tokens, formatMessage("App.Dremio")].filter(Boolean).join(" - ");
};

const { components: componentOverrides, palette, ...otherStyles } = themeStyles;

const theme = createTheme({
  ...otherStyles,
  palette: {
    ...palette,
    primary: {
      main: "#43B8C9",
    },
  },
  components: {
    ...componentOverrides,
    MuiSwitch: {
      styleOverrides: {
        switchBase: {
          height: "auto",
        },
      },
    },
  },
  typography: {
    fontSize: 12,
    fontFamily: "var(--dremio--font-family)",
  },
});

export class App extends Component {
  static propTypes = {
    location: PropTypes.object,
    params: PropTypes.object,
    //connected
    user: PropTypes.object,
    serverStatus: PropTypes.instanceOf(Immutable.Map),
    dispatch: PropTypes.func.isRequired,
  };

  static childContextTypes = {
    location: PropTypes.object,
    routeParams: PropTypes.object,
    username: PropTypes.string,
    loggedInUser: PropTypes.object,
  };

  static redirectForServerStatus(props) {
    if (!isNotSoftware()) {
      const { location } = props;
      if (
        props.serverStatus.get("status") !== SERVER_STATUS_OK &&
        location.pathname !== "/status"
      ) {
        props.dispatch(
          replace(
            "/status?redirect=" +
              encodeURIComponent(
                props.location.pathname + props.location.search
              )
          )
        );
      }
    }
  }

  constructor(props) {
    super(props);

    window.onunhandledrejection = this.handleUnhandledRejection;
  }

  getChildContext() {
    return {
      location: this.props.location, // todo remove
      routeParams: this.props.params,
      username: this.props.user.userName,
      loggedInUser: this.props.user,
    };
  }

  componentDidMount() {
    socket.dispatch = this.props.dispatch;
  }

  componentWillReceiveProps(props) {
    App.redirectForServerStatus(props);
  }

  handleUnhandledRejection = (rejectionEvent) => {
    const error = rejectionEvent.reason;
    if (!error) return;

    if (error.stack && this._shouldIgnoreExternalStack(error.stack)) return;

    sentryUtil.logException(error);
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
      console.warn("Ignoring js error from origin: " + stackOrigin);
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
    const lines = stack.split("\n");
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
      <Fragment>
        <Suspense>
          <LocationProvider location={this.props.location}>
            <StyledEngineProvider injectFirst>
              <ThemeProvider theme={theme}>{children}</ThemeProvider>
            </StyledEngineProvider>
            <AccountSettingsModalContainer />
            <NotificationContainer />
            <ConfirmationContainer />
            <PATModalContainer />
            <ModalsContainer modals={{ AboutModal }} />
            <div className="popup-notifications" />
            <div className="conifrmation-container" />
          </LocationProvider>
        </Suspense>
        <ProdErrorContainer />
      </Fragment>
    );
  }
}

App.propTypes = {
  children: PropTypes.node,
  routeParams: PropTypes.object,
};

function mapStateToProps(state) {
  const user = state.account ? state.account.get("user").toJS() : {};

  return {
    user,
    serverStatus: state.serverStatus,
  };
}

export default compose(
  AppHOC,
  withHookProvider,
  connect(mapStateToProps),
  DnDContextDecorator
)(App);
