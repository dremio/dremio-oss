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
import PropTypes from "prop-types";
import { injectIntl } from "react-intl";

import { SonarSideNav } from "@app/exports/components/SideNav/SonarSideNav";
import SettingPage from "@app/containers/SettingPage";
import UserNavigation from "components/UserNavigation";
import { accountSection } from "dyn-load/pages/AccountPage/AccountPageConstants";
import "./AccountPage.less";

const AccountPage = (props) => {
  const {
    style,
    children,
    intl: { formatMessage },
  } = props;

  return (
    <SettingPage id="account-page" style={style}>
      <div className="page-content">
        <SonarSideNav />
        <UserNavigation
          title={formatMessage({ id: "SideNav.AccountSettings" })}
          sections={[accountSection]}
        />
        <div className="main-content">{children}</div>
      </div>
    </SettingPage>
  );
};

AccountPage.propTypes = {
  children: PropTypes.node,
  style: PropTypes.object,
  intl: PropTypes.object,
};

export default injectIntl(AccountPage);
