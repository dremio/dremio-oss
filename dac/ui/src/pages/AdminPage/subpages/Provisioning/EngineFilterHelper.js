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
import {
  DEFAULT_ENGINE_FILTER_SELECTIONS,
  ENGINE_FILTER_NAME,
} from "dyn-load/constants/provisioningPage/provisioningConstants";
import { ENGINE_SIZE } from "#oss/constants/provisioningPage/provisioningConstants";

export function getFilteredEngines(engines, filters) {
  let filteredEngines = engines;
  if (filters && engines && engines.size) {
    const filterValues = {
      [ENGINE_FILTER_NAME.status]:
        filters[ENGINE_FILTER_NAME.status] ||
        DEFAULT_ENGINE_FILTER_SELECTIONS[ENGINE_FILTER_NAME.status],
      [ENGINE_FILTER_NAME.size]:
        filters[ENGINE_FILTER_NAME.size] ||
        DEFAULT_ENGINE_FILTER_SELECTIONS[ENGINE_FILTER_NAME.size],
    };

    if (filterValues[ENGINE_FILTER_NAME.status].length) {
      filteredEngines = filteredEngines.filter((engine) =>
        filterValues[ENGINE_FILTER_NAME.status].includes(
          engine.get("currentState"),
        ),
      );
    }
    if (filterValues[ENGINE_FILTER_NAME.size].length) {
      filteredEngines = filteredEngines.filter((engine) => {
        const nodeCount = engine.getIn(["dynamicConfig", "containerCount"]);
        const engineSizeOption =
          ENGINE_SIZE.find((size) => size.value === nodeCount) ||
          ENGINE_SIZE[ENGINE_SIZE.length - 1];
        return filterValues[ENGINE_FILTER_NAME.size].includes(
          engineSizeOption.id,
        );
      });
    }
  }
  return filteredEngines;
}
