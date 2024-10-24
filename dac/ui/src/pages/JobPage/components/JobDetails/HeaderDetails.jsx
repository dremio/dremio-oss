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
import { PureComponent } from "react";
import { connect } from "react-redux";
import Immutable from "immutable";
import PropTypes from "prop-types";
import { Link } from "react-router";
import { injectIntl } from "react-intl";

import * as ButtonTypes from "components/Buttons/ButtonTypes";
import { Button, Tooltip } from "dremio-ui-lib";
import { getIconPath } from "#oss/utils/getIconPath";
import datasetPathUtils from "utils/resourcePathUtils/dataset";
import {
  constructFullPathAndEncode,
  constructResourcePath,
} from "utils/pathUtils";
import { getViewState } from "selectors/resources";
import JobsUtils, { JobState } from "#oss/utils/jobsUtils";
import headerDetailsConfig from "@inject/pages/JobPage/components/JobDetails/headerDetailsConfig";

import JobStateIcon from "../JobStateIcon";

class HeaderDetails extends PureComponent {
  static propTypes = {
    cancelJob: PropTypes.func,
    style: PropTypes.object,
    jobId: PropTypes.string.isRequired,
    jobDetails: PropTypes.instanceOf(Immutable.Map).isRequired,
    downloadViewState: PropTypes.instanceOf(Immutable.Map).isRequired,
    downloadFile: PropTypes.func,
    intl: PropTypes.object.isRequired,
  };

  static contextTypes = {
    location: PropTypes.object,
  };

  constructor(props) {
    super(props);
    this.cancelJob = this.cancelJob.bind(this);
    this.openJobResults = this.openJobResults.bind(this);
  }

  getButton() {
    const { jobDetails, jobId, intl } = this.props;
    const currentJobState = jobDetails.get("state");

    // if the query is running or pending, expose the cancel button.
    if (
      JobsUtils.getRunning(currentJobState) ||
      currentJobState === JobState.ENGINE_START ||
      currentJobState === JobState.QUEUED ||
      currentJobState === JobState.ENQUEUED ||
      currentJobState === JobState.PLANNING
    ) {
      return (
        <Button
          color={ButtonTypes.UI_LIB_SECONDARY}
          text={intl.formatMessage({ id: "Common.Cancel" })}
          onClick={this.cancelJob}
          styles={styles.button}
          disableMargin
        />
      );
    }

    if (headerDetailsConfig.hideOpenResultsButton) {
      return null;
    }

    const queryType = jobDetails.get("queryType");
    // don't show the open results link if there are no results available or if not a completed UI query.
    // TODO: show a message if the results are not available (DX-7459)
    if (
      !jobDetails.get("resultsAvailable") ||
      currentJobState !== JobState.COMPLETED ||
      (queryType !== "UI_PREVIEW" && queryType !== "UI_RUN")
    ) {
      return null;
    }

    // determine the full path of the item. If we have a root datasetPathList, use that. If not, use the first parent's
    // datasetPathList.
    const datasetFullPath =
      jobDetails.get("datasetPathList") ||
      jobDetails.getIn(["parentsList", 0, "datasetPathList"]);
    let fullPath;
    if (datasetFullPath && datasetFullPath.size > 0) {
      // Todo: This is a temporary fix to avoid double quotes around Space. DX-27492
      fullPath = `${datasetFullPath.get(0)}.${constructFullPathAndEncode(
        datasetFullPath.slice(1),
      )}`;
    } else {
      fullPath = constructFullPathAndEncode(datasetFullPath);
    }
    const resourcePath = constructResourcePath(fullPath);
    const nextLocation = {
      // Setting pathname as temp/UNTITLED in case we dont have a datasetPath available
      pathname: datasetFullPath
        ? datasetPathUtils.toHref(resourcePath)
        : "tmp/UNTITLED",
      query: {
        jobId,
        version: jobDetails.get("datasetVersion"),
        openResults: "true",
      },
    };

    // make sure we get back into the right mode for "edit" queries
    if (jobDetails.get("datasetPathList")) {
      nextLocation.query.mode = "edit";
    }

    return (
      <span style={styles.openResults}>
        <dremio-icon
          name="entities/dataset-view"
          alt={intl.formatMessage({ id: "Dataset.VirtualDataset" })}
          style={styles.virtualDatasetIcon}
        />
        <Link data-qa="open-results-link" to={nextLocation}>
          {intl.formatMessage({ id: "Job.OpenResults" })} »
        </Link>
      </span>
    );
  }

  openJobResults() {}

  cancelJob() {
    this.props.cancelJob();
  }

  render() {
    const { style, jobDetails, intl } = this.props;

    if (!jobDetails.get("state")) {
      return null;
    }

    const flame = jobDetails.get("snowflakeAccelerated")
      ? "interface/flame-snowflake"
      : "interface/flame";
    const flameAlt = jobDetails.get("snowflakeAccelerated")
      ? "Job.AcceleratedHoverSnowFlake"
      : "Job.AcceleratedHover";

    return (
      <header
        className="details-header"
        style={{ ...styles.detailsHeader, ...(style || {}) }}
      >
        <div style={styles.stateHolder}>
          <JobStateIcon state={jobDetails.get("state")} />
          <div className="state">
            <span className="h4" style={styles.state}>
              {intl.formatMessage({
                id: "Job.State." + jobDetails.get("state"),
              })}
            </span>
          </div>
          {jobDetails.get("accelerated") && (
            <Tooltip title={flameAlt}>
              <img
                src={getIconPath(flame)}
                alt={intl.formatMessage({ id: flameAlt })}
                style={styles.flameIcon}
              />
            </Tooltip>
          )}
          {jobDetails.get("spilled") && (
            <Tooltip title={intl.formatMessage({ id: "Job.SpilledHover" })}>
              <dremio-icon
                name="interface/disk-spill"
                style={styles.flameIcon}
              />
            </Tooltip>
          )}
        </div>
        <div style={styles.rightPart}>{this.getButton()}</div>
      </header>
    );
  }
}
const styles = {
  button: {
    display: "inline-flex",
    float: "right",
    margin: "0 0 0 10px",
    width: 120,
    height: 28,
  },
  state: {
    marginBottom: "0",
  },
  openResults: {
    display: "flex",
    alignItems: "center",
    margin: "0 0 0 5px",
  },
  stateHolder: {
    display: "flex",
    alignItems: "center",
  },
  rightPart: {
    display: "flex",
    justifyContent: "flex-end",
    marginLeft: "auto",
  },
  detailsHeader: {
    height: 38,
    background: "rgba(0,0,0,.05)",
    display: "flex",
    alignItems: "center",
    padding: "0 10px 0 5px",
  },
  stateIcon: {
    width: 24,
    height: 24,
  },
  virtualDatasetIcon: {
    width: 24,
    height: 24,
    marginRight: "var(--dremio--spacing--05)",
  },
  flameIcon: {
    width: 20,
    height: 20,
    marginLeft: 5,
    marginTop: -2,
  },
};

function mapStateToProps(state, props) {
  return {
    downloadViewState: getViewState(
      state,
      `DOWNLOAD_JOB_RESULTS-${props.jobId}`,
    ),
  };
}

export default connect(mapStateToProps)(injectIntl(HeaderDetails));
