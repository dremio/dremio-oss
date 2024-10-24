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

import { getUserDetails } from "./getUserDetails";

import { type UserDetails } from "./UserDetails.type";

export type GetUsersDetailsParams = {
  ids: string[];
};

export const getUsersDetails = (
  params: GetUsersDetailsParams,
): Promise<Map<string, UserDetails>> => {
  return Promise.allSettled(
    params.ids.map((id) => getUserDetails({ id })),
  ).then((results) => {
    return results.reduce((userMap, result) => {
      if (result.status === "fulfilled") {
        userMap.set(result.value.id, result.value);
      }
      return userMap;
    }, new Map());
  });
};
