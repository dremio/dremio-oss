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
  constructVersionedEntityUrl,
  getIconName,
  useVersionedPageContext,
} from "../../versioned-page-utils";
import {
  versionedPageTabs,
  VersionedPageTabsType,
} from "#oss/exports/pages/VersionedHomePage/VersionedHomePage";
// @ts-ignore
import { TabsNavigationItem } from "dremio-ui-lib";
import { withRouter, type WithRouterProps } from "react-router";
import { useNessieContext } from "#oss/pages/NessieHomePage/utils/context";

import classes from "./VersionedPageTabs.module.less";

const nonCatalogTabs: VersionedPageTabsType[] = ["settings", "commit", "jobs"];
const nonSourceTabs: VersionedPageTabsType[] = [
  "settings",
  "commit",
  "jobs",
  "data",
];

const getVersionedMessageId = (tab: VersionedPageTabsType) => {
  const pre = "VersionedEntity";
  switch (tab) {
    case "data":
      return `${pre}.Data`;
    case "commits":
      return `${pre}.Commits`;
    case "tags":
      return `${pre}.Tags`;
    case "branches":
      return `${pre}.Branches`;
    default:
      return `${pre}.Data`;
  }
};

const VersionedHomePageTabs = ({ router, children }: any & WithRouterProps) => {
  const intl = useIntl();
  const {
    state: { hash },
    baseUrl,
  } = useNessieContext();
  const versionedCtx = useVersionedPageContext();

  const tabs = versionedPageTabs.filter((tab) =>
    versionedCtx?.isCatalog
      ? !nonCatalogTabs.includes(tab)
      : !nonSourceTabs.includes(tab),
  );

  const onTabClick = (tab: VersionedPageTabsType) => {
    if (tab === versionedCtx?.activeTab) return;

    const url = constructVersionedEntityUrl({
      type: versionedCtx?.isCatalog ? "catalog" : "source",
      baseUrl,
      tab,
      namespace: versionedCtx?.reservedNamespace ?? "",
      hash: hash ? `?hash=${hash}` : "",
    });
    router.push(url);
  };

  return (
    <>
      <div className={classes["versionedPage__tabs"]} role="tablist">
        {tabs
          .filter((tab) => tab !== "settings" && tab !== "commit")
          .map((tab) => (
            <TabsNavigationItem
              activeTab={versionedCtx?.activeTab ?? tabs[0]}
              key={tab}
              name={tab}
              onClick={() => onTabClick(tab)}
              className={classes["versionedPage__tab"]}
            >
              <>
                <dremio-icon
                  class={
                    classes[
                      tab === "commits"
                        ? "versionedPage__tab__icon"
                        : "versionedPage__tab__icon-small"
                    ]
                  }
                  name={getIconName(tab)}
                />
                {intl.formatMessage({
                  id: getVersionedMessageId(tab),
                })}
              </>
            </TabsNavigationItem>
          ))}
      </div>
      <div className={classes["versionedPage__tabContent"]}>{children}</div>
    </>
  );
};

export default withRouter(VersionedHomePageTabs);
