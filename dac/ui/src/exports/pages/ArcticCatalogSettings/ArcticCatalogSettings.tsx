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
import { useRef } from "react";
import Immutable from "immutable";
import { useIntl } from "react-intl";
import { Tooltip } from "dremio-ui-lib";
import { ArcticSideNav } from "../../components/SideNav/ArcticSideNav";
import NavCrumbs from "@inject/components/NavCrumbs/NavCrumbs";
import { browserHistory, RouteComponentProps } from "react-router";
import React, { useMemo } from "react";
import { ArcticCatalogProvider } from "@app/exports/providers/ArcticCatalogProvider";
import NavPanel from "@app/components/Nav/NavPanel";
import * as PATHS from "@app/exports/paths";
import { useFeatureFlag } from "@app/exports/providers/useFeatureFlag";
import { DATA_OPTIMIZATION } from "@app/exports/flags/DATA_OPTIMIZATION";
import { ARCTIC_CATALOG_LEVEL_ACCESS_CONTROL } from "@app/exports/flags/ARCTIC_CATALOG_LEVEL_ACCESS_CONTROL";
import clsx from "clsx";

import * as classes from "./ArcticCatalogSettings.module.less";

type Props = {
  children: JSX.Element;
} & RouteComponentProps<any, any>;

const TAB_GENERAL = "general";
const TAB_CONFIG = "configuration";
const TAB_PRIVILEGES = "privileges";

const getActiveTab = (pathname: string, arcticCatalogId: string) => {
  switch (pathname) {
    case PATHS.arcticCatalogSettingsConfiguration({ arcticCatalogId }):
    case PATHS.arcticCatalogSettingsConfigurationSummary({ arcticCatalogId }):
    case PATHS.arcticCatalogSettingsConfigurationNew({ arcticCatalogId }):
    case PATHS.arcticCatalogSettingsConfigurationEdit({ arcticCatalogId }): {
      return TAB_CONFIG;
    }
    case PATHS.arcticCatalogSettingsPrivileges({ arcticCatalogId }): {
      return TAB_PRIVILEGES;
    }
    case PATHS.arcticCatalogSettingsGeneral({ arcticCatalogId }):
    default: {
      return TAB_GENERAL;
    }
  }
};

const changeTab = (tabName: string, arcticCatalogId: string) => {
  switch (tabName) {
    case TAB_CONFIG: {
      browserHistory.push({
        pathname: PATHS.arcticCatalogSettingsConfiguration({ arcticCatalogId }),
      });
      break;
    }
    case TAB_PRIVILEGES: {
      browserHistory.push({
        pathname: PATHS.arcticCatalogSettingsPrivileges({ arcticCatalogId }),
      });
      break;
    }
    case TAB_GENERAL:
    default: {
      browserHistory.push({
        pathname: PATHS.arcticCatalogSettingsGeneral({ arcticCatalogId }),
      });
      break;
    }
  }
};

export const ArcticCatalogSettings = (props: Props) => {
  const {
    location: { pathname },
    params: { arcticCatalogId },
  } = props;
  const headerRef = useRef<any>();
  const { formatMessage } = useIntl();
  const [isDataOptimizationEnabled] = useFeatureFlag(DATA_OPTIMIZATION);
  const [isPrivilegesEnabled] = useFeatureFlag(
    ARCTIC_CATALOG_LEVEL_ACCESS_CONTROL
  );

  const tabs = useMemo(() => {
    const tabs = [
      [
        TAB_GENERAL,
        {
          icon: <dremio-icon name="interface/information" />,
          text: formatMessage({
            id: "Catalog.Settings.GeneralInformation",
          }),
        },
      ],
    ];

    if (isDataOptimizationEnabled) {
      tabs.push([
        TAB_CONFIG,
        {
          icon: <dremio-icon name="interface/data-optimization" />,
          text: formatMessage({
            id: "Catalog.Settings.Configuration",
          }),
        },
      ]);
    }

    // TODO: add logic that checks if they can view privileges
    if (isPrivilegesEnabled) {
      tabs.push([
        TAB_PRIVILEGES,
        {
          icon: <dremio-icon name="interface/privilege" />,
          text: formatMessage({
            id: "Common.Privileges",
          }),
        },
      ]);
    }

    return Immutable.OrderedMap(tabs);
  }, [formatMessage, isDataOptimizationEnabled, isPrivilegesEnabled]);

  const renderCatalogName = (contextProps: any) => {
    return headerRef?.current?.offsetWidth < headerRef?.current?.scrollWidth ? (
      <Tooltip title={contextProps?.catalog?.name}>
        <span>{contextProps?.catalog?.name}</span>
      </Tooltip>
    ) : (
      contextProps?.catalog?.name || ""
    );
  };

  return (
    <ArcticCatalogProvider {...props}>
      {(contextProps: any) => (
        <div
          className={clsx(
            "dremio-layout-container",
            classes["arctic-catalog-settings"]
          )}
        >
          <div className="sideNav-container --fixed">
            <ArcticSideNav />
          </div>
          <div className="dremio-layout-container --vertical">
            <NavCrumbs />
            <div className="dremio-layout-container">
              <div className="--fixed">
                <div
                  ref={headerRef}
                  className={clsx(
                    "dremio-typography-large",
                    "dremio-typography-bold",
                    classes["catalog-name"]
                  )}
                >
                  {renderCatalogName(contextProps)}
                </div>
                <NavPanel
                  showSingleTab
                  tabs={tabs}
                  changeTab={(tab: string) => changeTab(tab, arcticCatalogId)}
                  activeTab={getActiveTab(pathname, arcticCatalogId)}
                />
              </div>
              {props.children &&
                React.cloneElement(props.children, contextProps)}
            </div>
          </div>
        </div>
      )}
    </ArcticCatalogProvider>
  );
};
