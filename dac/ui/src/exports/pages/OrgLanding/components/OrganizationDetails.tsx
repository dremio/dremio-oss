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

import { type FunctionComponent } from "react";
import { type Organization } from "../../../types/Organization.type";
import classes from "./OrganizationDetails.less";
import clsx from "clsx";
import { intl } from "@app/utils/intl";

type OrganizationDetailsProps = {
  organization: Organization;
};

const statusTranslationKeys: Record<Organization["state"], string> = {
  ACTIVE: "Status.Normal",
};

const renderServiceStatus = (serviceStatus: Organization["state"]) => {
  return (
    <>
      <span
        className={clsx(
          classes["organization-details__status-bubble"],
          classes["organization-details__status-bubble--success"]
        )}
      />
      {intl.formatMessage({
        id: statusTranslationKeys[serviceStatus],
      })}
    </>
  );
};

export const OrganizationDetails: FunctionComponent<
  OrganizationDetailsProps
> = (props) => {
  const { organization } = props;
  return (
    <dl
      className={clsx(classes["organization-details"], "dremio-layout-stack")}
      style={{ maxWidth: "max-content" }}
      aria-label="Organization details"
    >
      <div>
        <dt>Organization ID</dt>
        <dd>
          <pre>{organization.id}</pre>
        </dd>
      </div>
      {/* <div>
        <dt>Service status</dt>
        <dd>{renderServiceStatus(organization.state)}</dd>
      </div> */}
    </dl>
  );
};
