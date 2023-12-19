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

import { useEffect } from "react";
import { useResourceSnapshot } from "smart-resource1/react";

import { BuildInfoResource } from "@app/exports/resources/BuildInfoResource";
import { DailyJobStatsResource } from "@app/exports/resources/DailyJobStatsResource";
import { JobsAndUsersResource } from "@app/exports/resources/JobsAndUsersResource";
import { UserStatsResource } from "@app/exports/resources/UserStatsResource";
import { USE_NEW_STATS_API } from "@app/exports/endpoints/SupportFlags/supportFlagConstants";
import { useSupportFlag } from "@app/exports/endpoints/SupportFlags/getSupportFlag";

import moment from "@app/utils/dayjs";

const NUM_DAYS = 7;
const START = moment().subtract(NUM_DAYS, "days").unix() * 1000;
const END = moment().unix() * 1000;

export const useBuildInfo = (shouldFetch = true) => {
  useEffect(() => {
    if (shouldFetch) {
      BuildInfoResource.fetch();
    }
  }, [shouldFetch]);

  return useResourceSnapshot(BuildInfoResource);
};

export const useDailyJobStats = (shouldFetch = true) => {
  const [value, loading] = useSupportFlag(USE_NEW_STATS_API);

  useEffect(() => {
    if (shouldFetch && !loading && !value) {
      DailyJobStatsResource.fetch({
        end: END,
        start: START,
      });
    }
  }, [loading, shouldFetch, value]);

  return useResourceSnapshot(DailyJobStatsResource);
};

export const useJobsAndUsers = (shouldFetch = true) => {
  const [value, loading] = useSupportFlag(USE_NEW_STATS_API);

  useEffect(() => {
    if (shouldFetch && !loading && value) {
      JobsAndUsersResource.fetch();
    }
  }, [loading, shouldFetch, value]);

  return useResourceSnapshot(JobsAndUsersResource);
};

export const useUserStats = (shouldFetch = true) => {
  const [value, loading] = useSupportFlag(USE_NEW_STATS_API);

  useEffect(() => {
    if (shouldFetch && !loading && !value) {
      UserStatsResource.fetch({
        end: END,
        start: START,
      });
    }
  }, [loading, shouldFetch, value]);

  return useResourceSnapshot(UserStatsResource);
};
