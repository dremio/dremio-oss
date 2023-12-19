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

import { useCallback, useEffect, useMemo } from "react";
import { useDispatch } from "react-redux";
import { getIntlContext } from "dremio-ui-common/contexts/IntlContext.js";
import { Spinner } from "dremio-ui-lib/components";

import { AboutModalState, ActionTypes } from "./aboutModalReducer";
import {
  useDailyJobStats,
  useJobsAndUsers,
  useUserStats,
} from "@app/exports/providers/AboutModalProviders";
import { isSmartFetchLoading } from "@app/utils/isSmartFetchLoading";
import { USE_NEW_STATS_API } from "@app/exports/endpoints/SupportFlags/supportFlagConstants";
import { useSupportFlag } from "@app/exports/endpoints/SupportFlags/getSupportFlag";
import { addNotification } from "@app/actions/notification";
import { MSG_CLEAR_DELAY_SEC } from "@app/constants/Constants";
import moment from "@app/utils/dayjs";

import * as classes from "./AboutModal.module.less";

type ClusterUsageDataProps = {
  state: AboutModalState;
  dispatch: React.Dispatch<ActionTypes>;
};

const { t } = getIntlContext();

function ClusterUsageData({ state, dispatch }: ClusterUsageDataProps) {
  const [usingNewStatsAPIs, isSupportFlagLoading] = useSupportFlag(
    USE_NEW_STATS_API
  );
  const { value: dailyJobStats, status: dailyJobStatsStatus } =
    useDailyJobStats(!state.alreadyFetchedMetrics);
  const {
    value: jobsAndUsers,
    status: jobsAndUsersStatus,
    error: jobsAndUsersErrors,
  } = useJobsAndUsers(!state.alreadyFetchedMetrics);
  const { value: userStats, status: userStatsStatus } = useUserStats(
    !state.alreadyFetchedMetrics
  );

  const reduxDispatch = useDispatch();

  const getDates = useCallback((format = "YYYY-MM-DD") => {
    const dates = [];

    for (let i = 0; i < 7; i++) {
      dates.push(moment().subtract(i, "days").format(format));
    }

    return dates;
  }, []);

  useEffect(() => {
    if (!isSupportFlagLoading) {
      dispatch({
        type: "SET_ALREADY_FETCHED_METRICS",
        alreadyFetchedMetrics: true,
      });
    }
  }, [isSupportFlagLoading, dispatch]);

  // takes the responses from the associated APIs and returns an object with dates as keys
  const formattedJobsAndUsers:
    | Record<string, { jobCount: number; uniqueUsersCount: number }>
    | undefined = useMemo(() => {
    if (isSupportFlagLoading) {
      return;
    }

    // using the deprecated APIs requires going through two different arrays
    // note that the APIs do not return info for dates where the users / jobs were 0
    // this can lead to the arrays having different sizes, or dates that don't exist in both
    if (!usingNewStatsAPIs) {
      // important to initialize the users count to 0 in case the date doesn't exist in the users array
      const initialStats = dailyJobStats?.jobStats.reduce(
        (obj, item) =>
          Object.assign(obj, {
            [item.date]: { jobCount: item.total, uniqueUsersCount: 0 },
          }),
        {} as Record<string, { jobCount: number; uniqueUsersCount: number }>
      );

      // no reason to update the object if user stats don't exist
      if (userStats?.userStatsByDate) {
        return userStats.userStatsByDate.reduce(
          (obj, item) =>
            Object.assign(obj, {
              [item.date]: {
                // in case the date isn't already present in the array
                jobCount: obj[item.date]?.jobCount ?? 0,
                uniqueUsersCount: item.total,
              },
            }),
          { ...initialStats }
        );
      } else {
        return initialStats;
      }
    } else {
      return jobsAndUsers?.stats.reduce(
        (obj, item) =>
          Object.assign(obj, {
            [item.date]: {
              jobCount: item.jobCount,
              uniqueUsersCount: item.uniqueUsersCount,
            },
          }),
        {}
      );
    }
  }, [
    dailyJobStats,
    isSupportFlagLoading,
    jobsAndUsers,
    userStats,
    usingNewStatsAPIs,
  ]);

  useEffect(() => {
    if (jobsAndUsersErrors?.responseBody) {
      reduxDispatch(
        addNotification(
          jobsAndUsersErrors.responseBody.errorMessage,
          "error",
          MSG_CLEAR_DELAY_SEC
        )
      );
    }
  }, [jobsAndUsersErrors?.responseBody, reduxDispatch]);

  useEffect(() => {
    const dates = getDates();
    const users: number[] = [];
    const jobs: number[] = [];
    const sum = { users: 0, jobs: 0 };

    if (formattedJobsAndUsers) {
      dispatch({ type: "SET_METRICS_IN_PROGRESS", metricsInProgress: true });

      for (const date of dates) {
        const dayStats = formattedJobsAndUsers[date];

        if (dayStats) {
          users.push(dayStats.uniqueUsersCount);
          jobs.push(dayStats.jobCount);

          sum.users += dayStats.uniqueUsersCount;
          sum.jobs += dayStats.jobCount;
        } else {
          users.push(0);
          jobs.push(0);
        }
      }

      dispatch({
        type: "SET_METRICS_CALCULATION_COMPLETE",
        users,
        jobs,
        average: {
          users: Math.round(sum.users / 7),
          jobs: Math.round(sum.jobs / 7),
        },
      });
    }
  }, [dispatch, formattedJobsAndUsers, getDates]);

  // when determining laoding state we need to check if:
  // 1) the correct APIs have returned
  // 2) the metrics data is done being processed
  const areMetricsLoading = () => {
    let metricsLoading = false;

    if (state.metricsInProgress) {
      metricsLoading = true;
    }

    if (!usingNewStatsAPIs) {
      metricsLoading =
        isSmartFetchLoading(dailyJobStatsStatus) ||
        isSmartFetchLoading(userStatsStatus);
    } else {
      metricsLoading = isSmartFetchLoading(jobsAndUsersStatus);
    }

    return metricsLoading;
  };

  return areMetricsLoading() ? (
    <Spinner className={classes["about-spinner"]} />
  ) : (
    <table className={classes["cluster-data"]}>
      <thead>
        <tr>
          <td>{t("Common.Date")}</td>
          <td>{t("App.UniqueUsers")}</td>
          <td>{t("App.JobsExecuted")}</td>
        </tr>
      </thead>
      <tbody>
        {getDates("MM/DD/YYYY").map((item, i) => (
          <tr key={item}>
            <td>{item}</td>
            <td>{state.users[i] ?? 0}</td>
            <td>{state.jobs[i] ?? 0}</td>
          </tr>
        ))}
      </tbody>
      <tfoot>
        <tr>
          <td>{t("Common.Average")}</td>
          <td>{state.average.users}</td>
          <td>{state.average.jobs}</td>
        </tr>
      </tfoot>
    </table>
  );
}

export default ClusterUsageData;
