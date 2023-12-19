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

import { SONAR_URLABILITY_UI } from "@inject/featureFlags/flags/SONAR_URLABILITY_UI";
import {
  FeaturesFlagsResource,
  loadFeatureFlags,
} from "../resources/FeaturesFlagsResource";

/**
 * Initialize the localStorage value to the default value
 */
const DEFAULT_VALUE = "true";
if (!localStorage.getItem(SONAR_URLABILITY_UI as string)) {
  localStorage.setItem(SONAR_URLABILITY_UI as string, DEFAULT_VALUE);
}

export const initializeSonarUrlability = async () => {
  await loadFeatureFlags(SONAR_URLABILITY_UI as string); //Fetch feature flag
  const featFlag =
    !!FeaturesFlagsResource.value?.get(SONAR_URLABILITY_UI as string) + "";

  const localStorageFlag = localStorage.getItem(SONAR_URLABILITY_UI as string);

  if (featFlag !== localStorageFlag) {
    localStorage.setItem(SONAR_URLABILITY_UI as string, featFlag + "");
    window.location.assign("/"); //Redirect to base path if flag is toggled
  }
};

export const isSonarUrlabilityEnabled = () => {
  const flag =
    localStorage.getItem(SONAR_URLABILITY_UI as string) || DEFAULT_VALUE;
  return flag === "true";
};
