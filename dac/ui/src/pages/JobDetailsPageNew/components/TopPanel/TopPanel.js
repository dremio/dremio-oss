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
import { useState } from "react";
import { Link, withRouter } from "react-router";
import { compose } from "redux";
import { injectIntl } from "react-intl";
import datasetPathUtils from "@app/utils/resourcePathUtils/dataset";
import jobsUtils, { JobState } from "@app/utils/jobsUtils";
import { getTabs, getIconName } from "dyn-load/utils/jobsUtils";
import * as ButtonTypes from "@app/components/Buttons/ButtonTypes";
import { Button } from "dremio-ui-lib";
import PropTypes from "prop-types";
import classNames from "classnames";
import JobStateIcon from "@app/pages/JobPage/components/JobStateIcon";
import TopPanelTab from "./TopPanelTab.js";
import CopyButton from "@app/components/Buttons/CopyButton";
import { UNSAVED_DATASET_PATH_URL } from "@app/constants/explorePage/paths";
import { PHYSICAL_DATASET_TYPES } from "@app/constants/datasetTypes";

import "./TopPanel.less";

const renderIcon = (iconName, className, selected) => {
  return (
    <dremio-icon
      name="interface/jobs-arrow-right"
      alt="Jobs"
      class={classNames("topPanel__icons", className, {
        "--lightBlue": selected,
      })}
    />
  );
};

export const TopPanel = (props) => {
  const {
    intl: { formatMessage },
    jobId,
    breadcrumbRouting,
    router,
    location,
    setComponent,
    jobStatus,
    showJobProfile,
    cancelJob,
    jobDetails,
  } = props;

  const renderOpenResults = () => {
    if (
      jobsUtils.getRunning(jobStatus) ||
      jobStatus === JobState.ENQUEUED ||
      jobStatus === JobState.PLANNING
    ) {
      return (
        <Button
          color={ButtonTypes.UI_LIB_SECONDARY}
          text={formatMessage({ id: "Common.Cancel" })}
          onClick={() => cancelJob(jobId)}
          disableMargin
        />
      );
    }

    const queryType = jobDetails.get("queryType");
    if (
      !jobDetails.get("resultsAvailable") ||
      jobStatus !== JobState.COMPLETED ||
      (queryType !== "UI_PREVIEW" && queryType !== "UI_RUN")
    ) {
      return null;
    }

    const datasetFullPath = jobDetails.get("datasetPaths");
    const fullPath =
      datasetFullPath && datasetFullPath.size > 0
        ? datasetPathUtils.toHrefV2(datasetFullPath)
        : UNSAVED_DATASET_PATH_URL;

    const nextLocation = {
      pathname: fullPath,
      query: {
        jobId,
        version: jobDetails.get("datasetVersion"),
        openResults: "true",
      },
    };

    const queriedDataset = jobDetails.get("queriedDatasets").get(0);
    const isPhysicalType = PHYSICAL_DATASET_TYPES.has(
      queriedDataset.get("datasetType")
    );
    const isUnsavedPath =
      UNSAVED_DATASET_PATH_URL === datasetFullPath.join("/");
    // Shouldn't have edit mode for physical datasets
    if (datasetFullPath && !(isUnsavedPath && isPhysicalType)) {
      nextLocation.query.mode = "edit";
    }

    return (
      <Link data-qa="open-results-link" to={nextLocation}>
        {formatMessage({ id: "Job.OpenResults" })} »
      </Link>
    );
  };

  const [selectedTab, setSelectedTab] = useState("Overview");

  const onTabClick = (tab, moreAttempts) => {
    if (tab === "Profile" && !moreAttempts) {
      setSelectedTab(tab);
      isSingleProfile
        ? showJobProfile(profileUrl)
        : onTabClick("Profile", true);
    } else {
      setComponent(tab);
      setSelectedTab(tab);
    }
  };

  const changePages = () => {
    const { state } = location;
    state && state.history
      ? breadcrumbRouting()
      : router.push({
          ...location,
          pathname: "/jobs",
        });
  };

  const attemptDetails = jobDetails && jobDetails.get("attemptDetails");
  const profileUrl = attemptDetails && attemptDetails.getIn([0, "profileUrl"]);
  const isSingleProfile = attemptDetails && attemptDetails.size === 1;
  const tabs = getTabs();
  return (
    <div className="topPanel">
      <div className="topPanel__navigationWrapper">
        <div className="topPanel__jobDetails">
          <div
            className="topPanel__jobs-logo"
            data-qa="jobs-logo"
            onClick={changePages}
          >
            {renderIcon("Jobs.svg", "topPanel__jobDetails__jobsIcon")}
          </div>
          <div className="gutter-top--half">
            <JobStateIcon state={jobStatus} />
          </div>
          <div data-qa="top-panel-jobId" className="topPanel__jobId">
            {jobId}
            <CopyButton
              text={jobId}
              title={formatMessage({ id: "Job.Id.Copy" })}
            />
          </div>
        </div>
        {tabs.map((tab, index) => {
          return (
            <TopPanelTab
              tabName={tab}
              onTabClick={onTabClick}
              selectedTab={selectedTab}
              iconName={getIconName(tab)}
              key={`${tab}-${index}`}
            />
          );
        })}
      </div>
      <span className="topPanel__openResults">{renderOpenResults()}</span>
    </div>
  );
};

TopPanel.propTypes = {
  intl: PropTypes.object.isRequired,
  breadcrumbRouting: PropTypes.func,
  jobId: PropTypes.string,
  setComponent: PropTypes.func,
  jobDetails: PropTypes.object,
  showJobProfile: PropTypes.func,
  jobStatus: PropTypes.string,
  cancelJob: PropTypes.func,
  router: PropTypes.object.isRequired,
  location: PropTypes.object.isRequired,
};

export default compose(withRouter, injectIntl)(TopPanel);
