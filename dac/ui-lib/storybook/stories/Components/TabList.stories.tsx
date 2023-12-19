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

import { TabListWrapper, TabListTab } from "../../../components";

export default {
  title: "Components/TabList",
};

const tabsLarge = Array(15)
  .fill(null)
  .map((_, i) => `Query ${i + 1}`);

export const Large = () => {
  const selectedTab = tabsLarge[0];
  return (
    <div className="bg-neutral-25 p-1" style={{ height: "500px" }}>
      <div style={{ border: "1px solid var(--color--neutral--50)" }}>
        <TabListWrapper>
          {tabsLarge.map((tab) => (
            <TabListTab
              aria-controls={`panel-${tab.replace(" ", "")}`}
              aria-selected={tab === selectedTab}
              id={`tab-${tab.replace(" ", "")}`}
              key={tab}
              getMenuItems={() => [
                {
                  id: "rename",
                  label: "Rename",
                  handler: () => alert("rename"),
                },
                {
                  id: "saveScript",
                  label: "Save Script as…",
                  handler: () => alert("saveScript"),
                },
                {
                  id: "saveView",
                  label: "Save as View",
                  handler: () => alert("saveView"),
                },
                {
                  id: "close",
                  label: "Close",
                  handler: () => alert("close"),
                },
              ]}
            >
              {tab}
            </TabListTab>
          ))}
        </TabListWrapper>
      </div>
    </div>
  );
};

const tabsSmall = Array(3)
  .fill(null)
  .map((_, i) => `Query ${i + 1}`);

export const Small = () => {
  const selectedTab = tabsSmall[0];
  return (
    <div className="bg-neutral-25 p-1" style={{ height: "500px" }}>
      <div style={{ border: "1px solid var(--color--neutral--50)" }}>
        <TabListWrapper>
          {tabsSmall.map((tab) => (
            <TabListTab
              aria-controls={`panel-${tab.replace(" ", "")}`}
              aria-selected={tab === selectedTab}
              id={`tab-${tab.replace(" ", "")}`}
              key={tab}
            >
              {tab}
            </TabListTab>
          ))}
        </TabListWrapper>
      </div>
    </div>
  );
};
