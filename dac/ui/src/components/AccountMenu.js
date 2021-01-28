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
import config from 'dyn-load/utils/config';
import { injectIntl } from 'react-intl';

import fileABug from 'utils/fileABug';
import { logoutUser } from '@inject/actions/account';


import Menu from 'components/Menus/Menu';
import MenuItem from 'components/Menus/MenuItem';
import DividerHr from 'components/Menus/DividerHr';
import { HookConsumer } from '@app/containers/RouteLeave';

const mapDispatchToProps = {
  logoutUser
};

@injectIntl
export class AccountMenu extends Component {
  static propTypes = {
    closeMenu: PropTypes.func.isRequired,
    intl: PropTypes.object.isRequired,
    //connected
    logoutUser: PropTypes.func.isRequired
  };

  static contextTypes = {
    router: PropTypes.object.isRequired
  };

  state = {
    showIntercom: false
  }

  onAccountSettings = () => {
    this.props.closeMenu();
    this.context.router.push({pathname: '/account/info'});
  }

  onLogOut = (doChangesCheckFn) => {
    this.props.closeMenu();
    const { hasChanges, userChoiceToLeaveOrStayPromise } = doChangesCheckFn();
    if (hasChanges) {
      userChoiceToLeaveOrStayPromise.then(leaveTheChanges => {
        if (leaveTheChanges) {
          this.props.logoutUser();
        }
      });
    } else {
      this.props.logoutUser();
    }
  }

  onFileABug = () => {
    this.props.closeMenu();
    fileABug();
  }

  render() {
    const { intl } = this.props;

    return <Menu>
      {config.shouldEnableBugFiling
        && <MenuItem isInformational>
          <span style={styles.menuInformation}>{intl.formatMessage({ id: 'HeaderMenu.InternalBuild' })}</span>
        </MenuItem>}
      {config.shouldEnableBugFiling
        && <MenuItem onClick={this.onFileABug}>{intl.formatMessage({ id: 'HeaderMenu.FileABug' })}</MenuItem>}
      {config.shouldEnableBugFiling && <DividerHr/>}
      <MenuItem onClick={this.onAccountSettings}>
        {intl.formatMessage({ id: 'HeaderMenu.AccountSettings' })}
      </MenuItem>
      <HookConsumer>
        {
          ({ doChangesCheck }) => (
            <MenuItem onClick={() => this.onLogOut(doChangesCheck)}>
              {intl.formatMessage({ id: 'HeaderMenu.LogOut' })}
            </MenuItem>
          )
        }
      </HookConsumer>
    </Menu>;
  }
}

export default connect(null, mapDispatchToProps)(AccountMenu);

const styles = {
  menuInformation: {
    fontStyle: 'italic',
    color: '#999'
  }
};
