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
import { connect }   from 'react-redux';
import { replace } from 'react-router-redux';
import Immutable from 'immutable';

import Radium from 'radium';
import pureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import { checkForFirstUser, logoutUser } from 'actions/account';
import browserUtils from 'utils/browserUtils';
import UnsupportedBrowserForm from 'components/UnsupportedBrowserForm';
import LoginForm from './components/LoginForm';

@Radium
@pureRender
export class AuthenticationPage extends Component {
  static propTypes = {
    style: PropTypes.object,
    user: PropTypes.instanceOf(Immutable.Map),
    checkForFirstUser: PropTypes.func.isRequired,
    logoutUser: PropTypes.func.isRequired,
    location: PropTypes.object.isRequired
  };

  state = {
    showLoginForm: browserUtils.hasSupportedBrowserVersion() || browserUtils.isApprovedUnsupportedBrowser()
  }

  componentDidMount() {
    this.props.logoutUser();
    this.props.checkForFirstUser(); // need to check here, because normally the API call will force a logout
  }

  approveBrowser = () => {
    browserUtils.approveUnsupportedBrowser();
    this.setState({
      showLoginForm: true
    });
  }

  render() {
    const { style, location, user } = this.props;

    return (
      this.state.showLoginForm ?
        <div id='authentication-page' style={[style, styles.base]}>
          <LoginForm user={user} location={location}/>
        </div>
        : <UnsupportedBrowserForm approveBrowser={this.approveBrowser} style={style}/>
    );
  }
}

function mapStateToProps(state) {
  return {
    user: state.account.get('user')
  };
}

const styles = {
  base: {
    justifyContent: 'center',
    alignItems: 'center',
    backgroundColor: '#2A394A',
    overflow: 'hidden'
  }
};

export default connect(mapStateToProps, {
  replace,
  checkForFirstUser,
  logoutUser
})(AuthenticationPage);
