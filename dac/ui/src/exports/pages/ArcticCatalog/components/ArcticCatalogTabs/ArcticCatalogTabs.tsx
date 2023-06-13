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

import { useIntl } from "react-intl";
import {
  constructArcticUrl,
  getArcticMessageId,
  getIconName,
  useArcticCatalogContext,
} from "../../arctic-catalog-utils";
import { arcticCatalogTabs, ArcticCatalogTabsType } from "../../ArcticCatalog";
// @ts-ignore
import { TabsNavigationItem } from "dremio-ui-lib";
import { withRouter, type WithRouterProps } from "react-router";
import { useNessieContext } from "@app/pages/NessieHomePage/utils/context";

import classes from "./ArcticCatalogTabs.module.less";

const nonCatalogTabs: ArcticCatalogTabsType[] = ["settings", "commit", "jobs"];
const nonSourceTabs: ArcticCatalogTabsType[] = [
  "settings",
  "commit",
  "jobs",
  "data",
];

const ArcticCatalogTabs = ({ router, children }: any & WithRouterProps) => {
  const intl = useIntl();
  const {
    state: { hash },
    baseUrl,
  } = useNessieContext();
  const arcticCtx = useArcticCatalogContext();

  const tabs = arcticCatalogTabs.filter((tab) =>
    arcticCtx?.isCatalog
      ? !nonCatalogTabs.includes(tab)
      : !nonSourceTabs.includes(tab)
  );

  const onTabClick = (tab: ArcticCatalogTabsType) => {
    if (tab === arcticCtx?.activeTab) return;

    const url = constructArcticUrl({
      type: arcticCtx?.isCatalog ? "catalog" : "source",
      baseUrl,
      tab,
      namespace: arcticCtx?.reservedNamespace ?? "",
      hash: hash ? `?hash=${hash}` : "",
    });
    router.push(url);
  };

  return (
    <>
      <div className={classes["arcticCatalog__tabs"]}>
        {tabs
          .filter((tab) => tab !== "settings" && tab !== "commit")
          .map((tab) => (
            <TabsNavigationItem
              activeTab={arcticCtx?.activeTab ?? tabs[0]}
              key={tab}
              name={tab}
              onClick={() => onTabClick(tab)}
              className={classes["arcticCatalog__tab"]}
            >
              <>
                <dremio-icon
                  class={
                    classes[
                      tab === "commits"
                        ? "arcticCatalog__tab__icon"
                        : "arcticCatalog__tab__icon-small"
                    ]
                  }
                  name={getIconName(tab)}
                />
                <span className={classes["arcticCatalog__tab__name"]}>
                  {intl.formatMessage({
                    id: getArcticMessageId(tab),
                  })}
                </span>
              </>
            </TabsNavigationItem>
          ))}
      </div>
      <div className={classes["arcticCatalog__tabContent"]}>{children}</div>
    </>
  );
};

export default withRouter(ArcticCatalogTabs);
