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
import { useRef } from "react";
import PropTypes from "prop-types";
import { connect } from "react-redux";
import config from "dyn-load/utils/config";
import { FormattedMessage } from "react-intl";
import { compose } from "redux";
import { withRouter } from "react-router";

import { logoutUser } from "@inject/actions/account";
import accountMenuConfig from "@inject/components/SideNav/accountMenuConfig";

import DividerHr from "components/Menus/DividerHr";
import { HookConsumer } from "@app/containers/RouteLeave";
import { showAccountSettingsModal } from "@app/actions/modals/accountSettingsActions";

import * as classes from "./AccountMenu.module.less";

const AccountMenu = (props) => {
  // eslint-disable-next-line no-shadow
  const { close, openAccountSettingsModal, logoutUser } = props;

  const onAccountSettings = () => {
    close();
    openAccountSettingsModal();
  };

  const onLogOut = (doChangesCheckFn) => {
    close();
    const { hasChanges, userChoiceToLeaveOrStayPromise } = doChangesCheckFn();
    if (hasChanges) {
      return userChoiceToLeaveOrStayPromise.then((leaveTheChanges) => {
        if (leaveTheChanges) {
          logoutUser();
        }
        return null;
      });
    } else {
      logoutUser();
    }
  };

  const menuRef = useRef(null);

  const onBlur = (e) => {
    if (!menuRef.current?.contains(e.relatedTarget)) {
      close();
    }
  };

  return (
    <ul ref={menuRef} className={classes["account-menu"]}>
      {config.shouldEnableBugFiling && (
        <span className={classes["account-menu-info"]}>
          <FormattedMessage id="HeaderMenu.InternalBuild" />
        </span>
      )}
      {config.shouldEnableBugFiling && <DividerHr />}
      {accountMenuConfig.enableAccountSettings && (
        <li
          className={classes["account-menu-item"]}
          onClick={onAccountSettings}
          onKeyPress={(e) => {
            if (e.code === "Enter" || e.code === "Space") {
              onAccountSettings();
            }
          }}
          tabIndex={0}
        >
          <FormattedMessage id="HeaderMenu.AccountSettings" />
        </li>
      )}
      <HookConsumer>
        {({ doChangesCheck }) => (
          <li
            className={classes["account-menu-item"]}
            onClick={() => onLogOut(doChangesCheck)}
            tabIndex={0}
            onKeyPress={(e) => {
              if (e.code === "Enter" || e.code === "Space") {
                onLogOut(doChangesCheck);
              }
            }}
            onBlur={onBlur}
          >
            <FormattedMessage id="HeaderMenu.LogOut" />
          </li>
        )}
      </HookConsumer>
    </ul>
  );
};

AccountMenu.propTypes = {
  router: PropTypes.shape({
    isActive: PropTypes.func,
    push: PropTypes.func,
  }),
  closeMenu: PropTypes.func.isRequired,
  logoutUser: PropTypes.func.isRequired,
  openAccountSettingsModal: PropTypes.any,
};

const mapDispatchToProps = {
  logoutUser,
  openAccountSettingsModal: showAccountSettingsModal,
};

export default compose(
  connect(null, mapDispatchToProps),
  withRouter,
)(AccountMenu);
