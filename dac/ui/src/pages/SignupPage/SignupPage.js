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
import PropTypes from 'prop-types';
import browserUtils from 'utils/browserUtils';
import UnsupportedBrowserForm from 'components/UnsupportedBrowserForm';

import SignupForm from './components/SignupForm';

export default class SignupPage extends PureComponent {
  static propTypes = {
    location: PropTypes.object.isRequired
  }
  state = {
    showSignupForm: browserUtils.hasSupportedBrowserVersion() || browserUtils.isApprovedUnsupportedBrowser()
  }

  approveBrowser = () => {
    browserUtils.approveUnsupportedBrowser();
    this.setState({
      showSignupForm: true
    });
  }
  renderSignupForm() {
    return (
      <div id='signup-page' className='page' style={styles.base}>
        <div className='main-header' style={styles.header} />
        <div style={styles.form}>
          <SignupForm location={this.props.location}/>
        </div>
      </div>
    );
  }

  render() {
    return (
      this.state.showSignupForm ? this.renderSignupForm()
        : <UnsupportedBrowserForm approveBrowser={this.approveBrowser} />
    );
  }
}

const styles = {
  base: {
    alignItems: 'center'
  },
  header: {
    width: '100%'
  },
  form: {
    display: 'flex',
    alignItems: 'center',
    height: '80%'
  }
};
