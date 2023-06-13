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

import { IconButton, Tag } from "../../../components";

export default {
  title: "Components/Tag",
  component: Tag,
} as Meta<typeof Tag>;

export const Default: StoryFn<typeof Tag> = () => {
  return (
    <div className="dremio-prose">
      <div className="flex gap-05">
        <Tag>default</Tag>
        <Tag className="bg-brand-subtle">brand-subtle</Tag>
        <Tag className="bg-success-subtle">success-subtle</Tag>
        <Tag className="bg-info-subtle">info-subtle</Tag>
        <Tag className="bg-danger-subtle">danger-subtle</Tag>
        <Tag className="bg-warning-subtle">warning-subtle</Tag>
        <Tag className="bg-warning-bold">warning-bold</Tag>
        <Tag className="bg-danger-bold">danger-bold</Tag>
        <Tag className="bg-info-bold">info-bold</Tag>{" "}
        <Tag className="bg-success-bold">success-bold</Tag>
        <Tag className="bg-brand-bold">brand-bold</Tag>
      </div>
      <div className="flex gap-05">
        <Tag>
          default{" "}
          <IconButton
            className="dremio-tag__hover-only"
            tooltip="Delete"
            style={{ margin: "-0.5em" }}
            onClick={() => alert("Deleted!")}
          >
            <dremio-icon name="interface/close-small"></dremio-icon>
          </IconButton>
        </Tag>
        <Tag>
          default{" "}
          <IconButton
            tooltip="Delete"
            style={{ margin: "-0.5em" }}
            onClick={() => alert("Deleted!")}
          >
            <dremio-icon name="interface/close-small"></dremio-icon>
          </IconButton>
        </Tag>
      </div>
    </div>
  );
};

Default.storyName = "Tag";
