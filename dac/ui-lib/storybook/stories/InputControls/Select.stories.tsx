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

import { Select, useSelect } from "../../../components";

export default {
  title: "Input Controls/Select",
  component: Select,
} as Meta<typeof Select>;

const regions = [
  {
    id: "us-west-1",
    title: "US West (N. California)",
  },
  {
    id: "us-west-2",
    title: "US West (Oregon)",
  },
  {
    id: "us-east-1",
    title: "US East (N. Virginia)",
  },
  {
    id: "us-east-2",
    title: "US East (Ohio)",
  },
  {
    id: "ca-central-1",
    title: "Canada (Central)",
  },
] as const;

const options = [
  "us-west-1",
  "us-west-2",
  "us-east-1",
  "us-east-2",
  "ca-central-1",
] as const;

export const Default: StoryFn<typeof Select> = () => {
  const regionSelection = useSelect<typeof options>();
  const renderRegion = (id: typeof options[number]) => (
    <span>
      {regions.find((region) => region.id === id).title} <small>{id}</small>
    </span>
  );
  return (
    <div style={{ fontSize: "14px" }} className="dremio-prose">
      <div className="form-group">
        <label htmlFor="region">Region</label>
        <Select
          id="region"
          renderButtonLabel={(value) => {
            if (!value) {
              return "Select a region";
            }
            return renderRegion(value);
          }}
          renderOptionLabel={(value) => {
            return renderRegion(value);
          }}
          options={options}
          {...regionSelection}
        />
      </div>
    </div>
  );
};

Default.storyName = "Select";
