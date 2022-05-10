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
import { connect } from 'react-redux';
import Radium from 'radium';
import { loginUser } from '@app/actions/account';

const mapDispatchToProps = {
  logIn: loginUser
};

@Radium
export class SSOLandingPage extends PureComponent {
  static propTypes = {
    logIn: PropTypes.func,
    location: PropTypes.any,
    query: PropTypes.any,
    code: PropTypes.any
  };

  componentDidMount() {
    const { logIn } = this.props;
    console.log(this.props);
    const form = { userName:this.props.location.query.code, password:'Letmein123' };
    logIn(form);
  }

  render() {
    return (
      <div id='sso'>
      </div>
    );
  }
}

export default connect(undefined, mapDispatchToProps)(SSOLandingPage);
