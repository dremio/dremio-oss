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

import { FeatureFlagResponse } from "../FeatureFlagResponse.type";
import { SONAR_URLABILITY_UI } from "@app/exports/flags/SONAR_URLABILITY_UI";
import { SQL_JOB_STATUS } from "@app/exports/flags/SQL_JOB_STATUS";

export const flags: Record<string, FeatureFlagResponse["entitlement"]> = {
  arctic_catalog_creation: "ENABLED",
  arctic_catalog_creation_ui: "ENABLED",
  arctic_catalog_ui: "ENABLED",
  organization_landing_ui: "ENABLED",
  data_plane_project_creation_ui: "ENABLED",
  disable_data_plane_project_creation_ui: "DISABLED",
  [SONAR_URLABILITY_UI as string]: "ENABLED",
  [SQL_JOB_STATUS as string]: "ENABLED",
};
