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

import { Meta } from "@storybook/react";

import { SectionMessage } from "../../../components";
import { MessageDetails } from "../../../components";

export default {
  title: "Containers/SectionMessage",
  component: SectionMessage,
} as Meta<typeof SectionMessage>;

export const Default = () => {
  return (
    <div className="dremio-layout-stack" style={{ "--space": "1em" }}>
      <SectionMessage appearance="information">
        This is an informational message
      </SectionMessage>
      <SectionMessage appearance="discovery">
        This is a discovery message
      </SectionMessage>
      <SectionMessage appearance="success">
        This is a success message
      </SectionMessage>
      <SectionMessage appearance="warning">
        This is a warning message
      </SectionMessage>
      <SectionMessage appearance="danger">
        This is a danger message
      </SectionMessage>
      <SectionMessage appearance="danger">
        <MessageDetails
          message="Failed to merge because there are no common commits between the branches you are trying to merge."
          details="Further message details or direct server error can be added here. This way a more detailed message can be shown in cases where this is relevant to the user."
          show
        />
      </SectionMessage>
      <SectionMessage
        appearance="danger"
        title="We encountered a problem while attempting to run this job"
      >
        Object &lsquo;NYC Taxi Trips&rsquo; not found within
        &lsquo;arctic1&rsquo;. Please check that it exists in the selected
        context.
      </SectionMessage>
      <SectionMessage appearance="warning">
        Compute resources &amp; the data access credential required for
        maintenance operations are missing.
      </SectionMessage>
      <SectionMessage
        appearance="discovery"
        title="Automate routine maintenance operations"
      >
        <>
          <p>
            You can automate routine maintenance operations like table
            optimization and cleanup by adding compute resources and data access
            credentials here.
          </p>
          <p>
            <a
              href="https://docs.dremio.com/cloud/arctic/automatic-table-optimization/"
              target="_blank"
              rel="noreferrer"
              style={{
                textDecoration: "none",
                fontWeight: "500",
                color: "lch(26 61.31 292.07)",
              }}
            >
              Learn More
            </a>
          </p>
        </>
      </SectionMessage>
      <SectionMessage appearance="success">
        The Arctic catalog &lsquo;arctic1&rsquo; was created successfully.
      </SectionMessage>
    </div>
  );
};

Default.storyName = "SectionMessage";
