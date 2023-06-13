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
import LinkWithHref from "@app/components/LinkWithRef/LinkWithRef";
import { useDispatch } from "react-redux";
//@ts-ignore
import { Tooltip } from "dremio-ui-lib";
//@ts-ignore
import { formatDateTimestampShortNoTZ } from "dremio-ui-common/utilities/formatDate.js";
import { fetchStatusOfAnalyzeTools } from "@app/utils/analyzeToolsUtils";
import { getIconPath } from "@app/utils/getIconPath";
import { openTableau, openPowerBI } from "actions/explore/download";
import RenderStat from "./RenderStat";
import { getVersionContextFromId } from "dremio-ui-common/utilities/datasetReference.js";
import { useIsBIToolsEnabled } from "@app/utils/arsUtils";

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
}: SummaryStatsProps) => {
  const dispatch = useDispatch();
  const currentRoute = location.pathname + location?.search;
  // reloads the page if the jobs link is the same as the current location
  const onJobClick = () => {
    if (currentRoute === jobsLink) {
      window.location.reload();
    }
  };

  const versionContext = getVersionContextFromId(dataset.get("entityId"));
  const isBIToolsEnabled = useIsBIToolsEnabled(versionContext);

  const canAnalyzeWithBITools = dataset.getIn([
    "permissions",
    "canAnalyzeWithBITools",
  ]);

  const analyzeButtonsConfig = fetchStatusOfAnalyzeTools();

  // The analyze buttons have three requirements to render
  // 1) Must not be in details view
  // 2) The user must have the "canAnalyzeWithBITools" permission for the dataset
  // 3) At least one of the analyze support keys must be enabled
  const shouldShowAnalyzeButtons =
    !detailsView &&
    canAnalyzeWithBITools &&
    (analyzeButtonsConfig["client.tools.tableau"] ||
      analyzeButtonsConfig["client.tools.powerbi"]);

  return (
    <div className={classes["summary-stats-container"]}>
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
      <RenderStat
        title="Dataset.Descendants"
        data={(descendantsCount || descendantsCount === 0) && descendantsCount}
      />
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
                  <div
                    className={classes["analyze-button"]}
                    // @ts-ignore
                    onClick={() => dispatch(openTableau(dataset))}
                  >
                    <dremio-icon
                      name="corporate/tableau"
                      class={classes["analyze-button__icon"]}
                    />
                  </div>
                </Tooltip>
              )}
              {analyzeButtonsConfig["client.tools.powerbi"] && (
                <Tooltip title="Dataset.PowerBI" placement="top">
                  <div
                    className={classes["analyze-button"]}
                    // @ts-ignore
                    onClick={() => dispatch(openPowerBI(dataset))}
                  >
                    <img
                      src={getIconPath("corporate/power-bi")}
                      className={classes["analyze-button__icon"]}
                    />
                  </div>
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
