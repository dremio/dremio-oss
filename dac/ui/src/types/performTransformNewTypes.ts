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

import Immutable from "immutable";

export type NewPerformTransformSingleProps = {
  dataset: Immutable.Map<string, any>;
  currentSql: string;
  queryContext: Immutable.List<any>;
  viewId: string;
  isRun: boolean;
  runningSql?: string;
  isSaveViewAs?: boolean;
  sessionId: string;
  sqlStatement: string;
  nextTable: Immutable.Map<string, any>;
  finalTransformData: Record<string, any>;
  references: any;
};

export type NewGetFetchDatasetMetaActionProps = {
  dataset: Immutable.Map<string, any>;
  currentSql: string;
  queryContext: Immutable.List<any>;
  viewId: string;
  isRun: boolean;
  sessionId: string;
  nextTable: Immutable.Map<string, any>;
  finalTransformData: Record<any, string>;
  references: any;
};

export type HandlePostNewQueryJobSuccessProps = {
  response: any;
  newVersion: string;
  queryStatuses: any[];
  curIndex: number;
  callback: any;
  tabId: string;
};
