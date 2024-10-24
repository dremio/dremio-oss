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
import { type ReactNode } from "react";
import { Link, WithRouterProps, withRouter } from "react-router";
import { useIntl } from "react-intl";
import datasetPathUtils from "#oss/utils/resourcePathUtils/dataset";
import jobsUtils, { JobState } from "#oss/utils/jobsUtils";
import { Button } from "dremio-ui-lib/components";
import classNames from "clsx";
import JobStateIcon from "#oss/pages/JobPage/components/JobStateIcon";
import CopyButton from "#oss/components/Buttons/CopyButton";
import { PHYSICAL_DATASET_TYPES } from "#oss/constants/datasetTypes";
import * as jobPaths from "dremio-ui-common/paths/jobs.js";
import * as sqlPaths from "dremio-ui-common/paths/sqlEditor.js";
import { getSonarContext } from "dremio-ui-common/contexts/SonarContext.js";
import { addProjectBase } from "dremio-ui-common/utilities/projectBase.js";

import "./TopPanel.less";

type TopPanelProps = {
  jobId?: string;
  jobDetails?: any;
  jobStatus?: string;
  cancelJob: (jobId?: string) => void;
  renderTabs: () => ReactNode;
};

export const TopPanel = (props: WithRouterProps & TopPanelProps) => {
  const {
    router,
    jobId,
    jobStatus,
    cancelJob,
    jobDetails,
    renderTabs,
    location,
  } = props;
  const reflectionId = location?.hash?.replace("#", "");
  const { formatMessage } = useIntl();

  const renderOpenResults = () => {
    const projectId = getSonarContext()?.getSelectedProjectId?.();
    if (
      jobsUtils.getRunning(jobStatus) ||
      jobStatus === JobState.ENGINE_START ||
      jobStatus === JobState.QUEUED ||
      jobStatus === JobState.ENQUEUED ||
      jobStatus === JobState.PLANNING
    ) {
      return (
        <Button variant="secondary" onClick={() => cancelJob(jobId)}>
          {formatMessage({ id: "Common.Cancel" })}
        </Button>
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
        ? addProjectBase(datasetPathUtils.toHrefV2(datasetFullPath))
        : sqlPaths.unsavedDatasetPathUrl.link({
            projectId,
          });

    const queriedDataset = jobDetails.get("queriedDatasets").get(0);
    const isPhysicalType = PHYSICAL_DATASET_TYPES.has(
      queriedDataset.get("datasetType"),
    );
    const isUnsavedPath =
      sqlPaths.unsavedDatasetPathUrl.link({
        projectId,
      }) === addProjectBase("/" + datasetFullPath.join("/"));

    // Shouldn't have edit mode for physical datasets
    const isEdit = datasetFullPath && !(isUnsavedPath && isPhysicalType);
    const nextLocation = {
      pathname: fullPath,
      query: {
        jobId,
        version: jobDetails.get("datasetVersion"),
        openResults: "true",
        ...(isEdit && { mode: "edit" }),
      },
    };

    return (
      <Link data-qa="open-results-link" to={nextLocation}>
        {formatMessage({ id: "Job.OpenResults" })} »
      </Link>
    );
  };

  const changePages = () => {
    const projectId = getSonarContext()?.getSelectedProjectId?.();
    router.push({
      pathname: reflectionId
        ? jobPaths.reflection.link({ projectId, reflectionId })
        : jobPaths.jobs.link({ projectId }),
      state: {
        jobFromDetails: jobId,
      },
    });
  };

  return (
    <div className="topPanel">
      <div className="topPanel__navigationWrapper">
        <div className="topPanel__jobDetails">
          <a
            className="topPanel__jobs-logo text-lg"
            data-qa="jobs-logo"
            onClick={changePages}
            tabIndex={0}
            onKeyDown={(e) => {
              if (e.code === "Enter" || e.code === "Space") {
                changePages(e);
              }
            }}
          >
            Jobs »
          </a>
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
        {renderTabs()}
      </div>
      <span className="topPanel__openResults">{renderOpenResults()}</span>
    </div>
  );
};

export default withRouter(TopPanel);
