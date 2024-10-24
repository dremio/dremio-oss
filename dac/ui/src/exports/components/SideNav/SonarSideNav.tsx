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
import { useMemo } from "react";
import SideNav from "#oss/components/SideNav/SideNav";
import clsx from "clsx";
import { Link } from "react-router";
// @ts-ignore
import { getSonarContext } from "dremio-ui-common/contexts/SonarContext.js";
import { getSessionContext } from "dremio-ui-common/contexts/SessionContext.js";
import * as commonPaths from "dremio-ui-common/paths/common.js";

const RenderHeaderAction = ({ logo = "sonar" }: { logo?: string }) => {
  const isOSS = !getSonarContext()?.getSelectedProjectId;
  const projectId = getSonarContext()?.getSelectedProjectId?.();

  const getLinkForLogo = useMemo(() => {
    if (isOSS || projectId) return commonPaths.projectBase.link({ projectId });
    if (!projectId) return commonPaths.projectsList.link();
  }, [projectId, isOSS]);

  return (
    <div
      className={clsx("sideNav-item", isOSS && "dremioLogoWithTextContainer")}
      role="listitem"
    >
      <Link to={getLinkForLogo} aria-label="home">
        <div className={`sideNav-item__link`}>
          <div className="sideNav-item__logo">
            <dremio-icon
              name={`corporate/${logo}`}
              alt={logo}
              class={isOSS ? "dremioLogoWithText" : ""}
            ></dremio-icon>
          </div>
        </div>
      </Link>
    </div>
  );
};

export const SonarSideNav = (props: any) => {
  const { className, ...rest } = props;
  const organizationLanding =
    typeof getSessionContext().getOrganizationId === "function";
  const headerAction = organizationLanding ? (
    <RenderHeaderAction />
  ) : (
    <RenderHeaderAction logo="dremio" />
  );
  return (
    <SideNav
      className={clsx(className, "sideNav--sonar")}
      headerAction={headerAction}
      {...rest}
    />
  );
};
