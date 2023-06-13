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

import SideNav from "@app/components/SideNav/SideNav";
import clsx from "clsx";
import { Link } from "react-router";
import * as orgPaths from "dremio-ui-common/paths/organization.js";
import { getSessionContext } from "dremio-ui-common/contexts/SessionContext.js";
import { TopAction } from "@app/components/SideNav/components/TopAction";
import { browserHistory } from "react-router";

export const OrganizationSideNav = (props: any) => {
  const { className, ...rest } = props;

  const organizationLanding =
    typeof getSessionContext().getOrganizationId === "function";
  const pathname = browserHistory.getCurrentLocation().pathname;

  return (
    <SideNav
      className={clsx(className, "sideNav--organization")}
      headerAction={
        organizationLanding ? (
          <div className="sideNav-item">
            <Link to={orgPaths.organization.link()}>
              <div className={`sideNav-item__link`}>
                <div className="sideNav-item__logo">
                  <dremio-icon name="corporate/dremio" alt=""></dremio-icon>
                </div>
              </div>
            </Link>
          </div>
        ) : (
          <div className="sideNav-item">
            {/** Urlability */}
            <Link to="/">
              <div className={`sideNav-item__link`}>
                <div className="sideNav-item__logo">
                  <dremio-icon name="corporate/dremio" alt=""></dremio-icon>
                </div>
              </div>
            </Link>
          </div>
        )
      }
      actions={
        <TopAction
          url={orgPaths.general.link()}
          icon="interface/settings"
          alt="Common.Settings"
          active={
            pathname.startsWith("/organization/settings")
              ? "--active-light"
              : ""
          }
          tooltipProps={{ placement: "right" }}
        />
      }
      showOrganization={false}
      {...rest}
    />
  );
};
