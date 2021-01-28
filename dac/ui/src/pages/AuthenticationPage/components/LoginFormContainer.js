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

import { lightLink } from 'uiTheme/radium/typography';

import LoginForm from './LoginForm';
import LoginTitle from './LoginTitle';

@Radium
export class LoginFormContainer extends PureComponent {
  renderForm() {
    return <LoginForm {...this.props} />;
  }

  render() {
    return (
      <div id='login-form' style={[styles.base]}>
        <LoginTitle
          style={{marginBottom: 10}}
          subTitle={la('Welcome to Dremio, please log in.')}
        />
        {this.renderForm()}
      </div>
    );
  }
}

export default compose(
  withRouter,
  LoginFormMixin
)(LoginFormContainer);

const styles = {
  base: {
    position: 'relative',
    backgroundColor: '#344253',
    minWidth: 775,
    height: 430,
    maxWidth: 775,
    maxHeight: 430,
    overflow: 'hidden',
    padding: 40,
    display: 'flex',
    flexDirection: 'column'
  },
  link: {
    flexGrow: 1,
    textAlign: 'right',
    alignSelf: 'center',
    ...lightLink
  }
};
