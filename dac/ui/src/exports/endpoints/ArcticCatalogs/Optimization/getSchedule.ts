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
//@ts-ignore
import { getApiContext } from "dremio-ui-common/contexts/ApiContext.js";
import { getOptimizationSchedulesUrl } from "./utils";
import { ScheduleInfoPage } from "./Schedule.types";

type getScheduleParams = {
  catalogId: string;
  reference: string;
  tableId: string;
};

export const getSchedule = (
  params: getScheduleParams
): Promise<ScheduleInfoPage> =>
  getApiContext()
    .fetch(
      getOptimizationSchedulesUrl({
        catalogId: params.catalogId,
        filter: `reference == '${params.reference}' && tableId == '${params.tableId}'`,
      })
    )
    .then((res) => res.json())
    .catch((error) => error.json());
