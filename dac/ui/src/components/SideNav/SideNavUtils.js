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

import { UNSAVED_DATASET_PATH } from "@app/constants/explorePage/paths";

export const isActive = ({
  name,
  dataset = false,
  loc,
  admin = false,
  jobs = false,
  sql = false,
  isDDPOnly = false,
}) => {
  const active = "--active";
  if (loc === name) {
    return active;
  }

  if (
    isDDPOnly &&
    (loc.startsWith("/commit") ||
      loc.startsWith("/namespace") ||
      loc.startsWith("/branches") ||
      loc.startsWith("/table"))
  ) {
    return active;
  }

  if (jobs && (loc.startsWith("/jobs") || loc.startsWith("/job"))) {
    return active;
  }

  if (
    dataset &&
    (loc.startsWith("/space") ||
      loc.startsWith("/home") ||
      loc.startsWith("/source") ||
      loc === name)
  ) {
    return active;
  }

  if (admin && (loc.startsWith("/admin") || loc.startsWith("/setting"))) {
    return active;
  }

  if (sql && loc.startsWith(UNSAVED_DATASET_PATH)) {
    return active;
  }

  return "";
};
