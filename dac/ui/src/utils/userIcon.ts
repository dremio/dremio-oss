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
// @ts-ignore
import Immutable from "immutable";

export default function getUserIconInitials(user: Immutable.Map<any, any>) {
  const userName = user.get("userName");
  const userNameFirst2 =
    user.get("firstName") && user.get("lastName")
      ? user.get("firstName").substring(0, 1).toUpperCase() +
        user.get("lastName").substring(0, 1).toUpperCase()
      : userName && userName.substring(0, 2).toUpperCase();
  return userNameFirst2;
}

export function getUserIconInitialsForAllUsers(createdByObj: {
  email: string;
  firstName: string;
  lastName: string;
  name: string;
}) {
  const userNameOrEmail = createdByObj.name
    ? createdByObj.name
    : createdByObj.email;
  const userNameFirst2 =
    createdByObj.firstName && createdByObj.lastName
      ? createdByObj.firstName.substring(0, 1).toUpperCase() +
        createdByObj.lastName.substring(0, 1).toUpperCase()
      : userNameOrEmail && userNameOrEmail.substring(0, 2).toUpperCase();
  return userNameFirst2;
}
