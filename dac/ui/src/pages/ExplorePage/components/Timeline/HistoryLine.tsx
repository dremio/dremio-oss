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
import Immutable from "immutable";
import clsx from "clsx";
import { Tooltip } from "dremio-ui-lib";
import { PageTypes } from "../../pageTypes";
import TimeDot from "./TimeDot";
import * as classes from "./HistoryLine.module.less";
import { getSessionContext } from "dremio-ui-common/contexts/SessionContext.js";
import { useMultiTabIsEnabled } from "#oss/components/SQLScripts/useMultiTabIsEnabled";
import { isTabbableUrl } from "#oss/utils/explorePageTypeUtils";

type HistoryLineProps = {
  historyItems?: any;
  tipVersion: string;
  activeVersion: string;
  location: Record<string, any>;
  pageType:
    | PageTypes.default
    | PageTypes.graph
    | PageTypes.details
    | PageTypes.wiki
    | PageTypes.reflections
    | PageTypes.history;
};

const HistoryLine = (props: HistoryLineProps) => {
  const { activeVersion, historyItems, location, pageType, tipVersion } = props;
  const organizationLanding =
    typeof getSessionContext().getOrganizationId === "function";

  const tabsEnabled = useMultiTabIsEnabled();
  const isTabsRendered = tabsEnabled && isTabbableUrl(location);

  const adjustMaxHeight =
    !organizationLanding && !isTabsRendered
      ? { maxHeight: "calc(100vh - 55px)" }
      : {};

  const renderContent = () => {
    switch (pageType) {
      case PageTypes.graph:
      case PageTypes.details:
      case PageTypes.reflections:
      case PageTypes.wiki:
      case PageTypes.history:
        return <></>;
      case PageTypes.default:
        return (
          <div
            className={clsx(classes["historyLine"], {
              [classes["--withTabs"]]: isTabsRendered,
            })}
            style={{ ...adjustMaxHeight }}
          >
            <div className={classes["historyLine__clock-wrapper"]}>
              <Tooltip title="Dataset.History.Info" placement="right">
                <dremio-icon
                  name="sql-editor/date-time"
                  class={classes["historyLine__clock-wrapper__icon"]}
                />
              </Tooltip>
            </div>
            <hr className={classes["historyLine_hr"]} />
            <div className="timeDotContainer">
              {historyItems &&
                historyItems.map(
                  (
                    item: Immutable.Map<string, any>,
                    index: number,
                    arr: Immutable.List<any>,
                  ) => (
                    <TimeDot
                      location={location}
                      historyItem={item}
                      key={item.get("datasetVersion")}
                      isLast={index === arr.size - 1}
                      tipVersion={tipVersion}
                      activeVersion={activeVersion}
                    />
                  ),
                )}
            </div>
          </div>
        );
      default:
        throw new Error(`not supported page type; '${pageType}'`);
    }
  };
  return renderContent();
};

export default HistoryLine;
