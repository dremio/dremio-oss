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

import { ArcticCatalogJobsQueryParams } from "./ArcticCatalogJobs";
// @ts-ignore
import { EngineSize } from "@app/exports/endpoints/ArcticCatalogJobs/ArcticCatalogJobs.type";

export const ENGINE_SIZE: { [key in EngineSize]: string } = {
  [EngineSize.XXSMALLV1]: "2XSmall",
  [EngineSize.XSMALLV1]: "XSmall",
  [EngineSize.SMALLV1]: "Small",
  [EngineSize.MEDIUMV1]: "Medium",
  [EngineSize.LARGEV1]: "Large",
  [EngineSize.XLARGEV1]: "XLarge",
  [EngineSize.XXLARGEV1]: "2XLarge",
  [EngineSize.XXXLARGEV1]: "3XLarge",
};

export const parseQueryState = (query: any): ArcticCatalogJobsQueryParams => ({
  filters: query.filters ? JSON.parse(query.filters) : {},
  sort: query.sort || "st",
  order: query.order || "DESCENDING",
});

export const formatQueryState = (query: ArcticCatalogJobsQueryParams) => {
  const { filters } = query;
  return {
    // add sort/order into URL when ready to be added
    filters: JSON.stringify(filters),
  };
};

export const formatArcticJobsCELQuery = (
  query: ArcticCatalogJobsQueryParams
) => {
  const filters = query.filters;
  const filterStrings = Object.entries(filters)
    .map(([key, values]: any) => {
      if (!values) {
        return null;
      }
      if (key === "startTime" && values instanceof Array) {
        //start time
        return "";
      } else if (key === "contains" && values.length) {
        return `quickfind == "${values?.[0]}"`;
      }

      if (values.length) {
        const curKey = key === "usr" ? "user" : key;
        let curFilter = `${curKey} in (list(`;
        values.forEach(
          (val: any, i: number) =>
            (curFilter = curFilter + `${i !== 0 ? ", " : ""}"${val}"`)
        );
        return curFilter + "))";
      }
    })
    .filter((x) => x);

  // add sort/order into URL when ready to be added
  return filterStrings.length ? "" + filterStrings.join(" && ") + "" : "";
};
