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

import classes from "./OrgLandingHeader.less";
import { IconButton } from "dremio-ui-lib";
import LinkWithRef from "@app/components/LinkWithRef/LinkWithRef";

type OrgLandingHeaderProps = {
  orgName: string;
};

export const OrgLandingHeader = (props: OrgLandingHeaderProps): JSX.Element => {
  return (
    <div className={classes["org-landing-header"]}>
      <dremio-icon
        name="interface/enterprise"
        class={classes["org-landing-header__icon"]}
        alt=""
      ></dremio-icon>
      <h1 className={classes["org-landing-header__org-name"]}>
        {props.orgName}
      </h1>
      <IconButton
        as={LinkWithRef}
        to="/setting/organization"
        tooltip="Settings"
        className={classes["org-landing-header__settings-menu"]}
      >
        <dremio-icon
          name="interface/settings"
          class={classes["org-landing-header__settings-menu-icon"]}
          alt=""
        ></dremio-icon>
      </IconButton>
    </div>
  );
};
