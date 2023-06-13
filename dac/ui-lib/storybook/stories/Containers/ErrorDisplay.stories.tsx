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

import { Meta, StoryObj } from "@storybook/react";

import { ErrorDisplay } from "../../../components";

export default {
  title: "Containers/ErrorDisplay",
  component: ErrorDisplay,
} as Meta<typeof ErrorDisplay>;

export const Default: StoryObj = {
  args: {
    error: new Error("fetch: Internal Server Error"),
    title: "Something went wrong when we tried to fetch the list of jobs.",
    production: true,
    supportMessage:
      "If the problem persists after refreshing this page, please contact Dremio support.",
    renderSupportInfo: () => <div>Session ID: a89ef98g</div>,
  },
};

Default.storyName = "ErrorDisplay";
