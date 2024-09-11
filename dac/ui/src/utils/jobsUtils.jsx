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
import moment from "@app/utils/dayjs";
import { useRef, useEffect } from "react";
import { Link } from "react-router";

import config from "@inject/utils/config";
import localStorageUtils from "@app/utils/storageUtils/localStorageUtils";
import OverView from "@app/pages/JobDetailsPageNew/components/OverView/OverView";
import SQLTab from "@app/pages/JobDetailsPageNew/components/SQLTab/SQLTab";
import JobStateIcon from "@app/pages/JobPage/components/JobStateIcon";
import Profile from "@app/pages/JobDetailsPageNew/components/Profile/Profile";
import timeUtils from "./timeUtils";
import * as jobPaths from "dremio-ui-common/paths/jobs.js";
import { getSonarContext } from "dremio-ui-common/contexts/SonarContext.js";
import { getPrivilegeContext } from "dremio-ui-common/contexts/PrivilegeContext.js";

// see AttemptEvent.State
export const JobState = {
  PENDING: "PENDING",
  METADATA_RETRIEVAL: "METADATA_RETRIEVAL",
  PLANNING: "PLANNING",
  QUEUED: "QUEUED",
  ENQUEUED: "ENQUEUED",
  ENGINE_START: "ENGINE_START",
  EXECUTION_PLANNING: "EXECUTION_PLANNING",
  STARTING: "STARTING",
  RUNNING: "RUNNING",
  COMPLETED: "COMPLETED",
  CANCELED: "CANCELED",
  FAILED: "FAILED",
};

const RECORD_STEP = 1000;
const RECORDS_IN_THOUSTHAND = RECORD_STEP;

export const jobDetailsTabs = ["Overview", "SQL", "Profile"];

export function getIconName(tab) {
  switch (tab) {
    case "Overview":
      return "interface/job-overview";
    case "SQL":
      return "interface/job-sql";
    case "Profile":
      return "interface/job-raw-profile";
    default:
      return "interface/job-overview";
  }
}

export function getJobIdsList(currentJobsMap = []) {
  const jobList = [];
  for (let i = 0; i < currentJobsMap.length; i++) {
    if (currentJobsMap[i].jobId) {
      jobList.push([currentJobsMap[i].jobId, i + 1]);
    }
  }
  return jobList;
}

export function getSqlList(currentJobsMap = []) {
  const sqlList = [];
  for (let i = 0; i < currentJobsMap.length; i++) {
    if (!currentJobsMap[i].jobId && !currentJobsMap[i].cancelled) {
      currentJobsMap[i].sqlStatement &&
        sqlList.push([currentJobsMap[i].sqlStatement, i + 1]);
    }
  }
  return sqlList;
}

export function getFilteredSqlList(jobs, filter) {
  return jobs.filter(
    (sql) =>
      sql[0] && sql[0].toLocaleLowerCase().includes(filter.toLocaleLowerCase()),
  );
}

export function getTagClassName(tab) {
  return tab === "Profile" ? "topPanel-rawProfile__rawProfileIcon" : null;
}

export function renderExecutionContent() {
  return <div></div>;
}

export function renderContent(contentPage, renderProps) {
  const {
    jobDetails,
    downloadJobFile,
    isContrast,
    setIsContrast,
    jobDetailsFromStore,
    showJobIdProfile,
    jobId,
    location,
  } = renderProps;
  switch (contentPage) {
    case "Overview":
      return (
        <OverView
          sql={jobDetails.get("queryText")}
          jobDetails={jobDetails}
          downloadJobFile={downloadJobFile}
          isContrast={isContrast}
          onClick={setIsContrast}
          status={
            jobDetailsFromStore
              ? jobDetailsFromStore.get("state")
              : jobDetails.get("jobStatus")
          }
          location={location}
        />
      );
    case "SQL":
      return (
        <SQLTab
          algebraicMatch={jobDetails.get("algebraicReflectionsDataset")}
          isContrast={isContrast}
          jobId={jobId}
          isComplete={jobDetails.get("isComplete")}
          onClick={setIsContrast}
          submittedSql={jobDetails.get("queryText")}
        />
      );
    case "Profile":
      return (
        <Profile
          jobDetails={jobDetails}
          showJobProfile={showJobIdProfile}
          location={location}
        />
      );
    default:
      return (
        <OverView
          sql={jobDetails.get("queryText")}
          status={
            jobDetailsFromStore
              ? jobDetailsFromStore.get("state")
              : jobDetails.get("jobStatus")
          }
          location={location}
        />
      );
  }
}

export const usePrevious = (value) => {
  const ref = useRef();
  useEffect(() => {
    ref.current = value;
  });
  return ref.current;
};

export const renderJobStatus = (jobState) => <JobStateIcon state={jobState} />;

export class JobsUtils {
  getNumberOfRunningJobs(jobs) {
    if (jobs) {
      return jobs.filter(
        (item) =>
          item.get("state") &&
          item.get("state").toLowerCase() === JobState.RUNNING,
      ).size;
    }
    return 0;
  }

  isJobRunning(jobState) {
    return ![JobState.FAILED, JobState.CANCELED, JobState.COMPLETED].includes(
      jobState,
    );
  }

  getFilterOptions(filters) {
    let filterString = "";
    filters.forEach((item) => {
      for (const key in item) {
        if (
          key !== "sort" &&
          key !== "order" &&
          key !== "st" &&
          key !== "contains"
        ) {
          item[key].forEach((filterItem, indexItem) => {
            const firstBracket = indexItem === 0 ? "(" : "";
            const lastBracket = indexItem === item[key].length - 1 ? ")" : "";
            const separator = indexItem < item[key].length - 1 ? "," : ";";
            filterString += `${firstBracket}${key}==${filterItem}${lastBracket}${separator}`;
          });
        }
      }
    });
    return filterString.slice(0, -1);
  }

  getSortOption(filters) {
    let filterString = "";
    filters.forEach((item) => {
      for (const key in item) {
        if (key === "sort" || key === "order") {
          filterString += `${key}=${item[key]}&`;
        }
      }
    });
    return filterString.slice(0, -1);
  }

  getTimeOption(filters) {
    let filterString = "";
    filters.forEach((item) => {
      for (const key in item) {
        if (key === "st") {
          filterString = `(st=ge=${item[key][0]};ft=le=${item[key][1]})`;
        }
      }
    });
    return filterString;
  }

  getContainsTextOption(filters) {
    const containsObg = filters.find((item) => {
      return item.contains;
    });
    return containsObg ? containsObg.contains : "";
  }

  msToHHMMSS(ms) {
    if (ms === undefined) {
      return "-";
    }
    return timeUtils.durationWithZero(moment.duration(ms));
  }

  getJobDuration(startTime, endTime, isNumberFormat) {
    const diff = moment(endTime).diff(moment(startTime));
    return this.formatJobDuration(diff, isNumberFormat);
  }

  formatJobDuration(duration, isNumberFormat) {
    return timeUtils.durationWithZero(
      moment.duration(duration, "ms"),
      isNumberFormat,
    );
  }

  getRunning(jobState) {
    return jobState === JobState.RUNNING;
  }

  getFinishTime(jobState, jobEndTime) {
    if (jobState === JobState.STARTING) {
      return "-";
    }
    if (this.getRunning(jobState)) {
      return "In progress";
    }
    return timeUtils.formatTime(jobEndTime, "In progress");
  }

  getFormattedRecords(records) {
    if (
      (!records && records !== 0) ||
      isNaN(records) ||
      isNaN(Number(records))
    ) {
      return "";
    }

    if (records < RECORDS_IN_THOUSTHAND) {
      return records;
    }

    return records.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
  }

  getFormattedNumber(number) {
    if (number === undefined || Number.isNaN(number)) {
      return "--";
    }
    const units = [
      { value: 1, symbol: "" },
      { value: 1e3, symbol: "K" },
      { value: 1e6, symbol: "M" },
      { value: 1e9, symbol: "B" },
      { value: 1e12, symbol: "T" },
      { value: 1e15, symbol: "P" },
      { value: 1e18, symbol: "E" },
    ];
    const rx = /\.0+$|(\.[0-9]*[1-9])0+$/;
    const filteredUnit = units
      .slice()
      .reverse()
      .find((unit) => number >= unit.value);
    return filteredUnit
      ? (number / filteredUnit.value).toFixed(1).replace(rx, "$1") +
          filteredUnit.symbol
      : "0";
  }

  isMetadataJob(requestType) {
    switch (requestType) {
      case "GET_CATALOGS":
      case "GET_COLUMNS":
      case "GET_SCHEMAS":
      case "GET_TABLES":
        return true;
      default:
        return false;
    }
  }

  navigationURLForJobId({ id, createFullUrl, windowLocationSearch }) {
    const projectId = getSonarContext().getSelectedProjectId?.();
    const url = `${jobPaths.job.link({
      jobId: id,
      projectId,
    })}${windowLocationSearch}`;
    return createFullUrl ? window.location.origin + url : url;
  }

  navigationURLForLayoutId(id, createFullUrl) {
    let url;
    const projectId = getSonarContext()?.getSelectedProjectId?.();
    if (!getPrivilegeContext().isAdmin()) {
      url = `${jobPaths.reflection.link({
        projectId,
        reflectionId: id,
      })}?filters=${encodeURIComponent(
        JSON.stringify({ sql: ["*" + id + "*"], qt: ["ACCELERATION"] }),
      )}`;
    } else {
      url = `${jobPaths.jobs.link({
        projectId,
      })}?filters=${encodeURIComponent(
        JSON.stringify({ sql: ["*" + id + "*"], qt: ["ACCELERATION"] }),
      )}`;
    }
    return createFullUrl ? window.location.origin + url : url;
  }

  getReflectionsByRelationship(jobDetails) {
    if (!jobDetails.get("acceleration")) {
      // failed planning, pre-1.3, etc guard
      return {};
    }

    const reflectionRelationships = jobDetails
      .get("acceleration")
      .get("reflectionRelationships")
      .toJS();
    const byRelationship = {};
    for (const reflectionRelationship of reflectionRelationships) {
      byRelationship[reflectionRelationship.relationship] =
        byRelationship[reflectionRelationship.relationship] || [];
      byRelationship[reflectionRelationship.relationship].push(
        reflectionRelationship,
      );
    }
    return byRelationship;
  }

  formatJobDurationWithMS(duration, isNumberFormat) {
    return timeUtils.durationWithMS(
      moment.duration(duration, "ms"),
      isNumberFormat,
    );
  }

  isNewJobsPage() {
    return config && config.showNewJobsPage;
  }

  getReflectionsLink(item, location) {
    const showLink =
      localStorageUtils.isUserAnAdmin() &&
      (item.get("reflectionType") === "RAW" ||
        item.get("reflectionType") === "AGGREGATE");
    return showLink ? (
      <Link
        to={{
          ...location,
          state: {
            modal: "AccelerationModal",
            datasetId: item.get("datasetId"),
            layoutId: item.get("reflectionID"),
          },
        }}
      >
        {item.get("reflectionName")}
      </Link>
    ) : (
      item.get("reflectionName")
    );
  }

  getSortLabel = (queryMeasure) => {
    switch (queryMeasure) {
      case "Runtime":
        return "Time";
      case "Total Memory":
        return "Memory";
      case "Bytes processed":
        return "Bytes";
      case "Parquet":
        return "Parquet";
      case "Records Processed":
        return "Records";
      case "Thread Skew":
        return "Thread Skew";
      default:
        return "Time";
    }
  };

  getSortKey = (queryMeasure) => {
    switch (queryMeasure) {
      case "Runtime":
        return "runTime";
      case "Total Memory":
        return "totalMemory";
      case "Bytes processed":
        return "bytesProcessed";
      case "Parquet":
        return "Parquet";
      case "Records Processed":
        return "recordsProcessed";
      case "Thread Skew":
        return "numThreads";
      default:
        return "processingTime";
    }
  };

  getSortedArray(arrayToSort, path, order) {
    const pathSplitted = path.split(".");
    return arrayToSort.sort((a, b) => {
      let x = a;
      let y = b;

      pathSplitted &&
        pathSplitted.forEach((key) => {
          x = x[key];
          y = y[key];
        });

      if (order === "DESC") {
        return y - x;
      } else {
        return x - y;
      }
    });
  }

  bytesToSize(bytes) {
    const sizes = ["Bytes", "KB", "MB", "GB", "TB"];
    if (bytes === -1 || bytes === undefined || Number.isNaN(bytes)) {
      return "--";
    }
    if (bytes === 0) {
      return "0 Bytes";
    }
    const radix = 10;
    const decimals = 2;
    const i = parseInt(Math.floor(Math.log(bytes) / Math.log(1024)), radix);
    return Math.round(bytes / Math.pow(1024, i), decimals) + " " + sizes[i];
  }

  getMetrixValue = (node, sortValue) => {
    switch (sortValue) {
      case "runtime":
        return timeUtils.nanoSecondsUpToHours(node[sortValue]);
      case "totalMemory":
        return this.bytesToSize(node[sortValue]);
      case "totalBufferForIncomingMemory":
        return this.bytesToSize(node[sortValue]);
      case "recordsProcessed":
        return this.getFormattedNumber(node[sortValue]);
      default:
        return timeUtils.nanoSecondsUpToHours(node[sortValue]);
    }
  };

  getTotalOperatorMemoryValue = (node) => {
    let opMemory = 0;
    if (node.operatorDataList?.length > 0) {
      node.operatorDataList.forEach((operator) => {
        opMemory += operator.baseMetrics?.totalMemory;
      });
    }
    return this.bytesToSize(opMemory);
  };
}

export default new JobsUtils();
