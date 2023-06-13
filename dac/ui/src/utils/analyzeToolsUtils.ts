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
import { HANDLE_THROUGH_API } from "@inject/pages/HomePage/components/HeaderButtonConstants";
import config from "dyn-load/utils/config";

export const fetchStatusOfAnalyzeTools = () => {
  const analyzeButtonsConfig = {
    "client.tools.tableau": false,
    "client.tools.powerbi": false,
  };

  if (HANDLE_THROUGH_API) {
    const supportFlags = localStorage.getItem("supportFlags")
      ? JSON.parse(localStorage.getItem("supportFlags") as string)
      : null;

    analyzeButtonsConfig["client.tools.tableau"] =
      supportFlags?.["client.tools.tableau"] ?? false;

    analyzeButtonsConfig["client.tools.powerbi"] =
      supportFlags?.["client.tools.powerbi"] ?? false;
  } else {
    const { tableau = { enabled: false }, powerbi = { enabled: false } } =
      config?.analyzeTools ?? {};

    analyzeButtonsConfig["client.tools.tableau"] = tableau.enabled;
    analyzeButtonsConfig["client.tools.powerbi"] = powerbi.enabled;
  }

  return analyzeButtonsConfig;
};
