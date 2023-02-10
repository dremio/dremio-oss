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
import SideNav from "@app/components/SideNav/SideNav";
import clsx from "clsx";
import { Link } from "react-router";
// @ts-ignore
import { FeatureSwitch } from "@app/exports/components/FeatureSwitch/FeatureSwitch";
// @ts-ignore
import { ORGANIZATION_LANDING } from "@app/exports/flags/ORGANIZATION_LANDING";
import { getSonarContext } from "dremio-ui-common/contexts/SonarContext.js";
import * as commonPaths from "dremio-ui-common/paths/common.js";

const RenderHeaderAction = ({ logo = "sonar" }: { logo?: string }) => {
  const isOSS = !getSonarContext()?.getSelectedProjectId;
  const projectId = getSonarContext()?.getSelectedProjectId?.();

  const getLinkForLogo = useMemo(() => {
    if (isOSS || projectId) return commonPaths.projectBase.link({ projectId });
    if (!projectId) return commonPaths.projectsList.link();
  }, [projectId, isOSS]);

  return (
    <div className="sideNav-item">
      <Link to={getLinkForLogo}>
        <div className={`sideNav-item__link`}>
          <div className="sideNav-item__logo">
            <dremio-icon name={`corporate/${logo}`} alt={logo}></dremio-icon>
          </div>
        </div>
      </Link>
    </div>
  );
};

const headerAction = (
  <FeatureSwitch
    flag={ORGANIZATION_LANDING}
    renderEnabled={() => <RenderHeaderAction />}
    renderDisabled={() => <RenderHeaderAction logo="dremio" />}
  />
);

export const SonarSideNav = (props: any) => {
  const { className, ...rest } = props;
  return (
    <SideNav
      className={clsx(className, "sideNav--sonar")}
      headerAction={headerAction}
      {...rest}
    />
  );
};
