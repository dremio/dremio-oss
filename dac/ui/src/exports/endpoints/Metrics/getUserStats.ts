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

import { getApiContext } from "dremio-ui-common/contexts/ApiContext.js";
import { APIV3Call } from "#oss/core/APICall";

type GetUserStatsParams = {
  end: number;
  start: number;
};

const userStatsUrl = (params: GetUserStatsParams) =>
  new APIV3Call()
    .paths("stats/user")
    .params(params)
    .params({ onlyUniqueUsersByDate: true })
    .toString();

type UserStats = {
  edition: string;
  userStatsByDate: {
    date: string;
    total: number;
  }[];
  userStatsByMonth: [];
  userStatsByWeek: [];
};

export const getUserStats = async (
  params: GetUserStatsParams,
): Promise<UserStats> => {
  return getApiContext()
    .fetch(userStatsUrl(params))
    .then((res) => res.json());
};
