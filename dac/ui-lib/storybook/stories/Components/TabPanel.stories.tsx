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

import {
  TabPanel,
  Tab,
  TabList,
  useTabList,
} from "../../../components/TabPanel/TabPanel";

export default {
  title: "Components/TabPanel",
  component: TabList,
} as Meta<typeof TabList>;

export const Default: StoryFn<typeof TabList> = () => {
  const { getTabProps, getTabPanelProps } = useTabList("tab1");
  return (
    <div className="dremio-prose">
      <div className="gap-05">
        <TabList aria-label="Sample Tabs">
          <Tab {...getTabProps({ id: "tab1", controls: "panel1" })}>
            <div className="dremio-icon-label">
              <dremio-icon name="common/SQLRunner" />
              First Tab
            </div>
          </Tab>
          <Tab {...getTabProps({ id: "tab2", controls: "panel2" })}>
            <div className="dremio-icon-label">
              <dremio-icon name="interface/job-overview" />
              Second Tab
            </div>
          </Tab>
          <Tab {...getTabProps({ id: "tab3", controls: "panel3" })}>
            <div className="dremio-icon-label">
              <dremio-icon name="common/job-profile" />
              Third Tab
            </div>
          </Tab>
        </TabList>
        <div>
          <TabPanel {...getTabPanelProps({ id: "panel1", labelledBy: "tab1" })}>
            Panel 1 content
          </TabPanel>
          <TabPanel {...getTabPanelProps({ id: "panel2", labelledBy: "tab2" })}>
            Panel 2 content
          </TabPanel>
          <TabPanel {...getTabPanelProps({ id: "panel3", labelledBy: "tab3" })}>
            Panel 3 content
          </TabPanel>
        </div>
      </div>
    </div>
  );
};

Default.storyName = "TabPanel";
