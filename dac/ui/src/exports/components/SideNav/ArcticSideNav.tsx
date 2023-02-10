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
import { browserHistory } from "react-router";
import * as PATHS from "../../paths";
import { getArcticUrlForCatalog } from "@app/exports/pages/ArcticCatalog/arctic-catalog-utils";
import { TopAction } from "@app/components/SideNav/components/TopAction";
import { isActive } from "@app/components/SideNav//SideNavUtils";
import { rmProjectBase } from "dremio-ui-common/utilities/projectBase.js";

export const ArcticSideNav = (props: any) => {
  const { className, ...rest } = props;
  const pathname =
    rmProjectBase(browserHistory.getCurrentLocation().pathname) || "/";

  return (
    <SideNav
      className={clsx(className, "sideNav--arctic")}
      headerAction={
        <TopAction
          url={PATHS.arcticCatalogs()}
          icon="corporate/arctic"
          alt="logo"
          tooltip={false}
          logo
        />
      }
      actions={
        <>
          <TopAction
            url={getArcticUrlForCatalog(pathname, "data", "")}
            icon="brand/arctic-catalog"
            alt="Common.Catalog"
            active={isActive({
              loc: pathname,
              name: "catalog",
              isArctic: true,
            })}
            tooltipProps={{ placement: "right" }}
          />
          <TopAction
            url={getArcticUrlForCatalog(pathname, "settings", "")}
            icon="interface/settings"
            alt="Settings.Catalog"
            active={isActive({
              loc: pathname,
              name: "settings",
              isArctic: true,
            })}
            tooltipProps={{ placement: "right" }}
          />
        </>
      }
      {...rest}
    />
  );
};
