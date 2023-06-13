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
    <div>
      <SectionMessage appearance="information">
        This is an informational message
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
    </div>
  );
};

Default.storyName = "SectionMessage";
