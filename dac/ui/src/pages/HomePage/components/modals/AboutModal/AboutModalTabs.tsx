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

import { getIntlContext } from "dremio-ui-common/contexts/IntlContext.js";
import { TabsNavigationItem } from "dremio-ui-lib";

import { AboutModalState, ActionTypes } from "./aboutModalReducer";

import * as classes from "./AboutModal.module.less";

const { t } = getIntlContext();

type AboutModalTabsProps = {
  state: AboutModalState;
  dispatch: React.Dispatch<ActionTypes>;
};

function AboutModalTabs({ state, dispatch }: AboutModalTabsProps) {
  const tabs: { name: AboutModalState["activeTab"]; label: string }[] = [
    {
      name: "build",
      label: t("About.Tabs.Build"),
    },
    {
      name: "metrics",
      label: t("About.Tabs.Usage"),
    },
  ];

  return (
    <div className={classes["about-tabs"]}>
      {tabs.map((tab) => (
        <TabsNavigationItem
          name={tab.name}
          key={tab.name}
          activeTab={state.activeTab}
          onClick={() =>
            dispatch({ type: "SET_ACTIVE_TAB", activeTab: tab.name })
          }
        >
          {tab.label}
        </TabsNavigationItem>
      ))}
    </div>
  );
}

export default AboutModalTabs;
