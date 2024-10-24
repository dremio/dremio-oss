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
import { compose } from "redux";
import { withRouter } from "react-router";
import "#oss/components/IconFont/css/DremioIcons.css";
import "#oss/components/SideNav/SideNav.less";
import { isActive } from "#oss/components/SideNav/SideNavUtils";
import { TopAction } from "#oss/components/SideNav/components/TopAction";
import * as adminPaths from "dremio-ui-common/paths/admin.js";

const SideNavAdmin = (props) => {
  const { location } = props;
  const loc = location.pathname;

  return (
    <TopAction
      active={isActive({ name: "/admin", loc, admin: true })}
      url={adminPaths.admin.link()}
      icon="interface/settings"
      alt="SideNav.Admin"
    />
  );
};

SideNavAdmin.propTypes = {
  location: PropTypes.object,
  router: PropTypes.shape({
    isActive: PropTypes.func,
    push: PropTypes.func,
  }),
};

export default compose(withRouter)(SideNavAdmin);
