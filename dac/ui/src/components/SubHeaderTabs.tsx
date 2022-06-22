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
import "./SubHeaderTabs.less";

type SubHeaderTabsProps = {
  onClickFunc: (tab: string) => void;
  tabArray: any[];
  selectedTab: string;
};

const SubHeaderTabs = ({
  onClickFunc,
  tabArray,
  selectedTab,
}: SubHeaderTabsProps) => {
  const selectedTabArray = selectedTab.split(" ");
  return (
    <div className="subHeader">
      {tabArray.map((tab: string, i: number) => {
        const tabNameForTesting = tab.split(" ");
        return (
          <div
            className={
              tab.startsWith(selectedTabArray[0])
                ? "subHeader__tabs --selected"
                : "subHeader__tabs"
            }
            onClick={() => {
              onClickFunc(tab);
            }}
            key={`${tab}-${i}`}
            data-qa={
              tab.startsWith(selectedTab)
                ? tabNameForTesting[0] + "subHeaderTab--selected"
                : tabNameForTesting[0] + "subHeaderTab"
            }
          >
            {tab}
          </div>
        );
      })}
    </div>
  );
};

export default SubHeaderTabs;
