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
import { PureComponent } from 'react';
import { compose } from 'redux';
import Radium from 'radium';
import { withRouter } from 'react-router';

import LoginFormMixin from '@inject/pages/AuthenticationPage/components/LoginFormMixin';
import localStorageUtils from 'dyn-load/utils/storageUtils/localStorageUtils';

@Radium
export class LoginFormContainer extends PureComponent {
  constructor(props) {
    super(props);
    this.state = {
      loginScreen: localStorageUtils.renderSSOLoginScreen()
    };
  }

  componentDidMount() {
    this.state.loginScreen === null ? this.setLoginScreen() : null;
    window.location.href = '/apiv2/sso_redirect';
  }

  setLoginScreen() {
    localStorageUtils.setSSOLoginChoice();
    this.setState({
      loginScreen: localStorageUtils.renderSSOLoginScreen()
    });
  }

  render() {
    return '';
  }
}

export default compose(
  withRouter,
  LoginFormMixin
)(LoginFormContainer);
