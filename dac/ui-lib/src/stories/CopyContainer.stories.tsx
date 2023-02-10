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
import React from "react";
import { ComponentStory, ComponentMeta } from "@storybook/react";

import { CopyContainer } from "../../components/CopyContainer";
import { Button } from "../../components/Button/Button";

export default {
  title: "Components/Copy Container",
  component: CopyContainer,
} as ComponentMeta<typeof CopyContainer>;

const Template: ComponentStory<typeof CopyContainer> = (args) => (
  <CopyContainer {...args}>
    <Button variant="primary">Copy Greeting</Button>
  </CopyContainer>
);

export const Primary = Template.bind({});
Primary.args = {
  contents: "Hello world",
};
