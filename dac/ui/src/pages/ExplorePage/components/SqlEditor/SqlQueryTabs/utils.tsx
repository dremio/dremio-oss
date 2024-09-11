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

import { Tab } from "@mui/material";
import { renderJobStatus } from "@app/utils/jobsUtils";
import { intl } from "@app/utils/intl";
// @ts-ignore
import { Tooltip } from "dremio-ui-lib";

export type QueryStatusType = {
  sqlStatement: string;
  cancelled: boolean;
  error?: any;
  jobId?: string;
  version?: string;
  statusList: any;
  isCancelDisabled?: boolean;
  lazyLoad?: boolean;
};

const allyProps = (index: number) => {
  return {
    id: `sql-query-tab-${index}`,
    "aria-controls": `sql-query-tab-${index}`,
  };
};

export const handleOnTabRouting = (
  tabValue: number,
  jobId: string | undefined,
  version: string | undefined,
  location: any = {},
  router: any,
  isMultiQueryRunning: boolean,
) => {
  if (isMultiQueryRunning) {
    return;
  }

  if (jobId && version) {
    router.replace({
      ...location,
      query: {
        ...location.query,
        jobId,
        tipVersion: version,
        version,
      },
      state: {
        tabValue,
      },
    });
  } else {
    router.replace({
      ...location,
      state: {
        tabValue,
      },
    });
  }
};

export const renderTabs = (
  queryStatuses: QueryStatusType[],
  location: any = {},
  router: any,
  statusList: [],
  isMultiQueryRunning: boolean,
) => {
  const tabs: JSX.Element[] = [];

  tabs.push(
    <Tab
      key={"sql-tab-0"}
      onClick={() =>
        handleOnTabRouting(
          0,
          undefined,
          undefined,
          location,
          router,
          isMultiQueryRunning,
        )
      }
      label={
        <div className="button-label">
          <dremio-icon
            name="navigation-bar/jobs"
            class="button-label-icon"
          ></dremio-icon>
        </div>
      }
      {...allyProps(0)}
    />,
  );

  for (let i = 1; i <= queryStatuses.length; i++) {
    const jobId = queryStatuses[i - 1].jobId;
    const version = queryStatuses[i - 1].version;
    tabs.push(
      <Tab
        key={`sql-tab-${i}`}
        onClick={() =>
          handleOnTabRouting(
            i,
            jobId,
            version,
            location,
            router,
            isMultiQueryRunning,
          )
        }
        label={
          <div className="button-label">
            {statusList[i - 1] === "REMOVED" ? (
              <RemovedIcon />
            ) : statusList[i - 1] ? (
              renderJobStatus(statusList[i - 1])
            ) : null}
            <span className="button-label-text">{`Query${i}`}</span>
          </div>
        }
        {...allyProps(i)}
      />,
    );
  }

  return tabs;
};

// custom icon for Removed state, since its a UI state (not backend state)
export const RemovedIcon = () => (
  <Tooltip title={intl.formatMessage({ id: "NewQuery.Removed" })}>
    <dremio-icon
      name="job-state/cancel"
      style={{ height: 24, width: 24 }}
      alt={intl.formatMessage({ id: "NewQuery.Removed" })}
    />
  </Tooltip>
);
