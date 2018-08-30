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
import Immutable from 'immutable';
import pureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import DocumentTitle from 'react-document-title';

import { page } from 'uiTheme/radium/general';
import config from 'utils/config';

import Acceleration from 'dyn-load/pages/AdminPage/subpages/acceleration/Acceleration';
import Roles from 'dyn-load/pages/AdminPage/subpages/Roles';
import Votes from 'dyn-load/pages/AdminPage/subpages/Votes';

import AdminPageView from './AdminPageView';

@pureRender
class AdminPage extends Component {

  static propTypes = {
    location: PropTypes.object.isRequired,
    routeParams: PropTypes.object,
    children: PropTypes.node
  }

  getMenuItems() {
    return Immutable.fromJS([
      { name: la('Node Activity'), url: '/admin/nodeActivity' },
      { name: la('Provisioning'), url: '/admin/provisioning' },
      Votes && { name: la('Dataset Votes'), url: '/admin/votes' },
      Acceleration && { name: la('Reflections'), url: '/admin/acceleration' },
      config.showUserAndUserProperties && { name: la('Users'), url: '/admin/users' },
      Roles && { name: la('Administrators'), url: '/admin/roles' },
      { name: la('Support Settings'), url: '/admin/support' },
      { name: la('Advanced Settings'), url: '/admin/advanced' }
    ].filter(Boolean));
  }

  render() {
    const { routeParams, location, children } = this.props;
    return (
      <DocumentTitle title={la('Admin')}>
        <AdminPageView
          routeParams={routeParams}
          menuItems={this.getMenuItems()}
          style={page}
          location={location}
          children={children} />
      </DocumentTitle>
    );
  }
}

export default AdminPage;
