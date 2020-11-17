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
import { Component } from 'react';
import PropTypes from 'prop-types';
import { connect } from 'react-redux';
import { injectIntl } from 'react-intl';

import { logoutUser } from 'actions/account';


import Menu from 'components/Menus/Menu';
import MenuItem from 'components/Menus/MenuItem';

const mapDispatchToProps = {
  logoutUser
};

@injectIntl
export class AdminMenu extends Component {
  static propTypes = {
    closeMenu: PropTypes.func.isRequired,
    intl: PropTypes.object.isRequired,
    //connected
    logoutUser: PropTypes.func.isRequired
  };

  static contextTypes = {
    router: PropTypes.object.isRequired
  };

  onAdminSystemSettings = () => {
    this.props.closeMenu();
    this.context.router.push({pathname: '/admin'});
  }

  onAdminProjectSetting = () => {
    this.props.closeMenu();
    // eslint-disable-next-line no-alert
    alert('Comming Soon');
  }

  render() {
    const { intl } = this.props;

    return (
      <Menu>
        <MenuItem onClick={this.onAdminSystemSettings}>
          {intl.formatMessage({ id: 'HeaderMenu.AdminSystem' })}
        </MenuItem>
        <MenuItem onClick={() => this.onAdminProjectSetting}>
          {intl.formatMessage({ id: 'HeaderMenu.AdminProject' })}
        </MenuItem>
      </Menu>
    );
  }
}

export default connect(null, mapDispatchToProps)(AdminMenu);
