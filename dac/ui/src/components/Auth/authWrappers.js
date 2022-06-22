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
import { Component } from "react";
import PropTypes from "prop-types";
import { connect } from "react-redux";
import { replace } from "react-router-redux";
import { UserAuthWrapper } from "redux-auth-wrapper";

import userUtils from "utils/userUtils";
import { initApp } from "@app/actions/app";
import LoadingOverlay from "../LoadingOverlay";

export const UserIsAuthenticated = UserAuthWrapper({
  authSelector: (state) => state.account.get("user"),
  predicate: userUtils.isAuthenticated,
  redirectAction: replace,
  wrapperDisplayName: "UserIsAuthenticated",
});

export const UserIsAdmin = function () {
  const isAdmin = UserAuthWrapper({
    authSelector: (state) => state.account.get("user"),
    predicate: userUtils.isAdmin,
    redirectAction: replace,
    failureRedirectPath: "/",
    wrapperDisplayName: "UserIsAdmin",
    // In UserIsAuthenticated we should allow to redirect to some page from login page.
    // But if already some authenticated user will try access an admin page it will
    // redirect him to / and then he won't be able to redirect back the to admin page.
    allowRedirectBack: false,
  });

  return isAdmin(UserIsAuthenticated(...arguments));
};

const mapDispatchToProps = {
  initApp,
};

const mapStateToProps = (state) => {
  return {
    initComplete: state.init.complete,
  };
};

/**
 * component that send a request to a server to check whether or not user is authenticated and
 * his session is not expired
 */
@connect(mapStateToProps, mapDispatchToProps)
export class CheckUserAuthentication extends Component {
  static propTypes = {
    // connected
    initApp: PropTypes.func.isRequired,
    initComplete: PropTypes.bool,
    children: PropTypes.node,
  };

  componentDidMount() {
    this.props.initApp();
  }

  render() {
    const { initComplete } = this.props;
    return !initComplete ? <LoadingOverlay showSpinner /> : this.props.children;
  }
}
