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

import { Skeleton } from "../../../components";

export default {
  title: "Components/Skeleton",
  component: Skeleton,
} as Meta<typeof Skeleton>;

export const Default = () => {
  return (
    <div>
      <h1 style={{ fontSize: "24px" }}>
        <Skeleton width="12ch" />
      </h1>
      <p>
        <Skeleton width="10ch" />
      </p>
      <small>
        <Skeleton width="10ch" />
      </small>
    </div>
  );
};

Default.storyName = "Skeleton";
