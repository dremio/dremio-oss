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
import * as PATHS from "../../paths";

const headerAction = (
  <div className="sideNav-item">
    <Link to={PATHS.organization()}>
      <div className={`sideNav-item__link`}>
        <div className="sideNav-item__logo">
          <dremio-icon name="corporate/dremio" alt=""></dremio-icon>
        </div>
      </div>
    </Link>
  </div>
);

export const OrganizationSideNav = (props: any) => {
  const { className, ...rest } = props;
  return (
    <SideNav
      className={clsx(className, "sideNav--organization")}
      headerAction={headerAction}
      actions={<></>}
      showOrganization={false}
      {...rest}
    />
  );
};
