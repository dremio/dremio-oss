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
import { type Organization } from "../../types/Organization.type";
import { Page, Section, SpinnerOverlay } from "dremio-ui-lib/dist-esm";
import { ServicesSection } from "./components/ServicesSection";
import { OrganizationDetails } from "./components/OrganizationDetails";
import { OrgLandingHeader } from "./components/OrgLandingHeader";
import { OrganizationSideNav } from "../../components/SideNav/OrganizationSideNav";

type ArcticPageProps = {
  organization: Organization;
};

export const OrgLanding: FunctionComponent<ArcticPageProps> = (props) => {
  const { organization } = props;
  return (
    <div className="page-content">
      <OrganizationSideNav />
      <SpinnerOverlay in={!organization} />
      {organization && (
        <Page header={<OrgLandingHeader orgName={organization.name} />}>
          <div
            className="dremio-layout-stack"
            style={{ "--space": "var(--dremio--spacing--2)" } as any}
          >
            <OrganizationDetails organization={organization} />

            <Section title={<h2 aria-hidden="true">Services</h2>}>
              <ServicesSection />
            </Section>
          </div>
        </Page>
      )}
    </div>
  );
};
