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
import * as classes from "./SummaryStats.module.less";
import Immutable from "immutable";
import LinkWithHref from "#oss/components/LinkWithRef/LinkWithRef";
import { useDispatch } from "react-redux";
//@ts-ignore
import { Tooltip } from "dremio-ui-lib";
//@ts-ignore
import { formatDateTimestampShortNoTZ } from "dremio-ui-common/utilities/formatDate.js";
import { fetchStatusOfAnalyzeTools } from "#oss/utils/analyzeToolsUtils";
import { getIconPath } from "#oss/utils/getIconPath";
import { openTableau, openPowerBI } from "actions/explore/download";
import RenderStat from "./RenderStat";
import { VersionContextType } from "dremio-ui-common/components/VersionContext.js";
import { hideForNonDefaultBranch } from "dremio-ui-common/utilities/versionContext.js";
import { Button } from "dremio-ui-lib/components";

type SummaryStatsProps = {
  dataset: Immutable.Map<string, any>;
  jobsLink?: string;
  jobCount: number;
  descendantsCount: number;
  location: Record<string, any>;
  createdAt?: Date;
  ownerEmail?: string;
  lastModifyUserEmail?: string;
  lastModified?: Date;
  detailsView?: boolean;
  versionContext?: VersionContextType;
};

const SummaryStats = ({
  dataset,
  jobsLink,
  jobCount,
  descendantsCount,
  createdAt,
  ownerEmail,
  // lastModifyUserEmail,
  lastModified,
  location,
  detailsView,
  versionContext,
}: SummaryStatsProps) => {
  const dispatch = useDispatch();
  const currentRoute = location.pathname + location?.search;
  // reloads the page if the jobs link is the same as the current location
  const onJobClick = () => {
    if (currentRoute === jobsLink) {
      window.location.reload();
    }
  };

  const isBIToolsEnabled = hideForNonDefaultBranch(versionContext);

  const canAnalyzeWithBITools = dataset.getIn([
    "permissions",
    "canAnalyzeWithBITools",
  ]);

  const analyzeButtonsConfig = fetchStatusOfAnalyzeTools();

  // The analyze buttons have two requirements to render
  // 1) The user must have the "canAnalyzeWithBITools" permission for the dataset
  // 2) At least one of the analyze support keys must be enabled
  const shouldShowAnalyzeButtons =
    canAnalyzeWithBITools &&
    (analyzeButtonsConfig["client.tools.tableau"] ||
      analyzeButtonsConfig["client.tools.powerbi"]);

  return (
    <div className={classes["summary-stats-container"]}>
      {dataset.get("viewDialect") && (
        <RenderStat title="Common.Dialect" data={dataset.get("viewDialect")} />
      )}
      {!versionContext && (
        <RenderStat
          title="Jobs.Jobs.LastMonth"
          data={
            (jobCount || jobCount === 0) && (
              // @ts-ignore
              <LinkWithHref onClick={onJobClick} to={jobsLink}>
                {jobCount}
              </LinkWithHref>
            )
          }
        />
      )}
      {!versionContext && (
        <RenderStat
          title="Dataset.Descendants"
          data={
            (descendantsCount || descendantsCount === 0) && descendantsCount
          }
        />
      )}
      <RenderStat
        title="Common.Created"
        data={createdAt && formatDateTimestampShortNoTZ(new Date(createdAt))}
      />
      <RenderStat
        title="Common.Owner"
        showAvatar={detailsView}
        wrapContent
        data={ownerEmail && ownerEmail}
      />
      <RenderStat
        title="Common.Lastupdated"
        data={
          lastModified && formatDateTimestampShortNoTZ(new Date(lastModified))
        }
      />
      {/* <RenderStat
        title="Common.Lastupdated.By"
        wrapContent
        showAvatar={detailsView}
        data={lastModifyUserEmail && lastModifyUserEmail}
      /> */}
      {shouldShowAnalyzeButtons && isBIToolsEnabled && (
        <RenderStat
          title="Dataset.Summary.LaunchBI"
          data={
            // Ideally we'd use <Button>s instead of <div>s, but buttons cause issues when
            // rendered and clicked inside of MUI Tooltips
            <span className={classes["analyze-container"]}>
              {analyzeButtonsConfig["client.tools.tableau"] && (
                <Tooltip title="Dataset.Tableau" placement="top">
                  <Button
                    variant="secondary"
                    className={classes["analyze-button"]}
                    onClick={() => dispatch(openTableau(dataset))}
                    style={{ minWidth: 32 }}
                  >
                    <dremio-icon
                      name="corporate/tableau"
                      class={classes["analyze-button__icon"]}
                    />
                  </Button>
                </Tooltip>
              )}
              {analyzeButtonsConfig["client.tools.powerbi"] && (
                <Tooltip title="Dataset.PowerBI" placement="top">
                  <Button
                    className={classes["analyze-button"]}
                    onClick={() => dispatch(openPowerBI(dataset))}
                    variant="secondary"
                    style={{ minWidth: 32 }}
                  >
                    <img
                      src={getIconPath("corporate/power-bi")}
                      className={classes["analyze-button__icon"]}
                    />
                  </Button>
                </Tooltip>
              )}
            </span>
          }
        />
      )}
    </div>
  );
};

export default SummaryStats;
