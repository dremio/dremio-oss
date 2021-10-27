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
import { connect } from 'react-redux';
import config from 'dyn-load/utils/config';
import { FormattedMessage } from 'react-intl';
import { compose } from 'redux';
import { withRouter } from 'react-router';

import fileABug from 'utils/fileABug';
import { logoutUser } from '@inject/actions/account';


import Menu from 'components/Menus/Menu';
import MenuItem from 'components/Menus/MenuItem';
import DividerHr from 'components/Menus/DividerHr';
import { HookConsumer } from '@app/containers/RouteLeave';
import {menuListStyle} from '@app/components/SideNav/SideNavConstants';

import './AccountMenu.less';

const AccountMenu = (props) => {
  // eslint-disable-next-line no-shadow
  const {closeMenu, router, logoutUser} = props;

  const onAccountSettings = () => {
    closeMenu();
    router.push({pathname: '/account/info'});
  };

  const onLogOut = (doChangesCheckFn) => {
    closeMenu();
    const { hasChanges, userChoiceToLeaveOrStayPromise } = doChangesCheckFn();
    if (hasChanges) {
      userChoiceToLeaveOrStayPromise.then(leaveTheChanges => {
        if (leaveTheChanges) {
          logoutUser();
        }
      });
    } else {
      logoutUser();
    }
  };

  const onFileABug = () => {
    closeMenu();
    fileABug();
  };

  return (<Menu style={menuListStyle}>
    {config.shouldEnableBugFiling &&
      <MenuItem isInformational>
        <span className={'menuInformation'}>
          <FormattedMessage id='HeaderMenu.InternalBuild'/>
        </span>
      </MenuItem>
    }
    {config.shouldEnableBugFiling &&
      <MenuItem onClick={onFileABug}>
        <FormattedMessage id='HeaderMenu.FileABug'/>
      </MenuItem>
    }
    {config.shouldEnableBugFiling &&
      <DividerHr/>
    }
    <MenuItem onClick={onAccountSettings}>
      <FormattedMessage id='HeaderMenu.AccountSettings'/>
    </MenuItem>
    <HookConsumer>
      {
        ({ doChangesCheck }) => (
          <MenuItem onClick={() => onLogOut(doChangesCheck)}>
            <FormattedMessage id='HeaderMenu.LogOut'/>
          </MenuItem>
        )
      }
    </HookConsumer>
  </Menu>
  );
};

AccountMenu.propTypes = {
  router: PropTypes.shape({
    isActive: PropTypes.func,
    push: PropTypes.func
  }),
  closeMenu: PropTypes.func.isRequired,
  logoutUser: PropTypes.func.isRequired
};

const mapDispatchToProps = {
  logoutUser
};

export default compose(withRouter, connect(null, mapDispatchToProps))(AccountMenu);
