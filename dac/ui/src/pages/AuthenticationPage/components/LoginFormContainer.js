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
import { PureComponent } from "react";
import { compose } from "redux";
import { withRouter } from "react-router";

import LoginFormMixin from "@inject/pages/AuthenticationPage/components/LoginFormMixin";
import localStorageUtils from "dyn-load/utils/storageUtils/localStorageUtils";
import { renderSSOLoginToggleLink } from "dyn-load/utils/loginUtils.js";
import LoginForm from "./LoginForm";
import LoginTitle from "./LoginTitle";

export class LoginFormContainer extends PureComponent {
  constructor(props) {
    super(props);
    this.state = {
      loginScreen: localStorageUtils.renderSSOLoginScreen(),
    };
  }

  componentDidMount() {
    this.state.loginScreen === null ? this.setLoginScreen() : null;
  }

  renderForm() {
    return <LoginForm {...this.props} />;
  }

  setLoginScreen() {
    localStorageUtils.setSSOLoginChoice();
    this.setState({
      loginScreen: localStorageUtils.renderSSOLoginScreen(),
    });
  }

  render() {
    return (
      <div id="login-form" style={styles.base}>
        <LoginTitle
          style={{ marginBottom: 10 }}
          subTitle={laDeprecated("Welcome to Dremio, please log in.")}
        />
        {this.renderForm(this.state.loginScreen)}
        {renderSSOLoginToggleLink(this.setLoginScreen.bind(this))}
      </div>
    );
  }
}
export default compose(withRouter, LoginFormMixin)(LoginFormContainer);

const styles = {
  base: {
    position: "relative",
    backgroundColor: "#344253",
    minWidth: 775,
    height: 430,
    maxWidth: 775,
    maxHeight: 430,
    overflow: "hidden",
    padding: 40,
    display: "flex",
    flexDirection: "column",
  },
};
