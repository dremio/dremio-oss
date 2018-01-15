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
import { Component } from 'react';
import pureRender from 'pure-render-decorator';

import PropTypes from 'prop-types';

import MainHeader from 'components/MainHeader';
import UserNavigation from 'components/UserNavigation';

import './AccountPage.less';

@pureRender
export default class AccountPage extends Component {
  static propTypes = {
    children: PropTypes.node,
    style: PropTypes.object
  };

  constructor(props) {
    super(props);
    this.menuItems = Immutable.fromJS([
      {name: la('General Information'), url: '/account/info'}
    ]);
  }

  render() {
    return (
      <div id='account-page' style={this.props.style}>
        <MainHeader />
        <div className='page-content'>
          <UserNavigation
            menuItems={this.menuItems}
            title={la('Account Settings')}/>
          <div className='main-content'>
            {this.props.children}
          </div>
        </div>
      </div>
    );
  }
}
