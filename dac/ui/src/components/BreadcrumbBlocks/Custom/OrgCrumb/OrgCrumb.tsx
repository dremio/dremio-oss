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
import BreadcrumbLink from "../../Common/BreadcrumbLink/BreadcrumbLink";
import Breadcrumb from "../../Common/Breadcrumb/Breadcrumb";
import { connect } from "react-redux";
//@ts-ignore
import { getOrganizationInfo } from "@inject/selectors/account";
import * as PATHS from "@app/exports/paths";
import { FeatureSwitch } from "@app/exports/components/FeatureSwitch/FeatureSwitch";
import { ORGANIZATION_LANDING } from "@app/exports/flags/ORGANIZATION_LANDING";

const renderOrgLandingDisabled = (name: string) => {
  return <Breadcrumb iconName="interface/enterprise" text={name} />;
};

const OrgCrumb = (props: { orgInfo: Record<string, any> }) => {
  const { orgInfo } = props;

  return (
    orgInfo && (
      <FeatureSwitch
        flag={ORGANIZATION_LANDING}
        renderEnabled={() => (
          <BreadcrumbLink
            to={PATHS.organization()}
            text={orgInfo.name}
            iconName="interface/enterprise"
          />
        )}
        renderDisabled={() => renderOrgLandingDisabled(orgInfo.name)}
        renderPending={() => renderOrgLandingDisabled(orgInfo.name)}
      />
    )
  );
};

const mapStateToProps = (state: Record<string, any>) => {
  const orgInfo = getOrganizationInfo(state);
  return {
    orgInfo,
  };
};

export default connect(mapStateToProps, {})(OrgCrumb);
