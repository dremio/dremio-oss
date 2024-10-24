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

import { JobsQueryParams } from "../types/Jobs.types";

export const parseQueryState = (query: any): JobsQueryParams => {
  try {
    return {
      filters: query.filters ? JSON.parse(query.filters) : {},
      sort: query.sort || "st",
      order: query.order || "DESCENDING",
    };
  } catch {
    return {
      filters: {},
      sort: "st",
      order: "DESCENDING",
    };
  }
};

export const formatQueryState = (query: JobsQueryParams) => {
  const { filters } = query;
  return {
    // add sort/order into URL when ready to be added
    filters: JSON.stringify(filters),
  };
};

export const getUpdatedStartTimeForQuery = (
  query: any,
  selectedStartTime?: any,
) => {
  try {
    if (
      !selectedStartTime ||
      selectedStartTime.type === "CUSTOM_INTERVAL" ||
      selectedStartTime?.type === "ALL_TIME_INTERVAL"
    ) {
      return query;
    }

    const parsedQuery = parseQueryState(query);
    const fromDate = selectedStartTime.time[0];
    const toDate = selectedStartTime.time[1];
    const fromDateTimestamp = fromDate.toDate().getTime();
    const toDateTimestamp = toDate.toDate().getTime();
    return formatQueryState({
      ...parsedQuery,
      filters: {
        ...parsedQuery.filters,
        st: [fromDateTimestamp, toDateTimestamp],
      },
    });
  } catch (e) {
    return query;
  }
};

export const isStartTimeOutdated = (query: any, selectedStartTime?: any) => {
  const parsedQuery = parseQueryState(query);
  if (
    selectedStartTime &&
    selectedStartTime?.type !== "CUSTOM_INTERVAL" &&
    selectedStartTime?.type !== "ALL_TIME_INTERVAL" &&
    parsedQuery.filters.st
  ) {
    const prevToDateTime = parsedQuery.filters.st[1];
    const currentDateTime = Date.now();
    return Math.abs(currentDateTime - prevToDateTime) > 60000;
  } else return false;
};
