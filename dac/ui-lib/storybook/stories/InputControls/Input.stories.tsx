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

import { Meta, StoryFn } from "@storybook/react";

import { Input } from "../../../components";

export default {
  title: "Input Controls/Input",
  component: Input,
} as Meta<typeof Input>;

export const Default: StoryFn<typeof Input> = (args: any) => {
  return (
    <div className="dremio-prose">
      <div className="form-group">
        <label htmlFor="name">Name</label>
        <Input {...args} id="name" name="name" />
      </div>
    </div>
  );
};

Default.storyName = "Input";
