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

import { SmartResource } from "smart-resource1";
import moize from "moize";
import { getReflectionSummary } from "../endpoints/ReflectionSummary/getReflectionSummary";

export const reflectionSummaryCache = moize.promise(getReflectionSummary, {
  maxAge: 30000,
  maxSize: 100,
  isDeepEqual: true,
});

const paginatedReflectionsFetcher = async (pageCount: number, params: any) => {
  let result;
  let nextPageToken = null;
  for (let pageNum = 0; pageNum < pageCount; pageNum++) {
    if (pageNum > 0 && !nextPageToken) {
      break;
    }
    const val: any = await reflectionSummaryCache({
      ...params,
      pageToken: nextPageToken,
    });
    nextPageToken = val.nextPageToken;
    if (pageNum === 0) {
      result = { ...val };
    } else {
      result.nextPageToken = nextPageToken;
      result.data = [...result.data, ...val.data];
    }
  }
  return result;
};

export const PaginatedReflectionsResource = new SmartResource(
  paginatedReflectionsFetcher,
  {
    mode: "takeEvery",
  }
);
