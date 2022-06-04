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

import PropTypes from 'prop-types';
import { compose } from 'redux';
import { withRouter } from 'react-router';
import '@app/components/IconFont/css/DremioIcons.css';
import '@app/components/SideNav/SideNav.less';
import {isActive} from '@app/components/SideNav/SideNavUtils';
import {TopAction} from '@app/components/SideNav/components/TopAction';



const SideNavAdmin = (props) => {
  const {location} = props;
  const loc = location.pathname;

  return (
    <TopAction active={isActive({ name: '/admin', loc, admin: true})} url='/admin' icon='SideNav-gear.svg' alt='SideNav.Admin' />
  );
};

SideNavAdmin.propTypes = {
  location: PropTypes.object,
  router: PropTypes.shape({
    isActive: PropTypes.func,
    push: PropTypes.func
  })
};

export default compose(withRouter)(SideNavAdmin);
