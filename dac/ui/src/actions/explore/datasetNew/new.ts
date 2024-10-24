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

import { RSAA } from "redux-api-middleware";
import { APIV2Call } from "#oss/core/APICall";
// @ts-ignore
import { updateBody } from "@inject/actions/explore/dataset/updateLocation";
import {
  NEW_UNTITLED_SQL_FAILURE,
  NEW_UNTITLED_SQL_START,
  NEW_UNTITLED_SQL_SUCCESS,
} from "#oss/actions/explore/dataset/new";
import Immutable from "immutable";
import exploreUtils from "#oss/utils/explore/exploreUtils";
import readResponseAsJSON from "#oss/utils/apiUtils/responseUtils";

// common helper for running and previewing new queries
const postNewUntitledSql = (
  href: string,
  sql: string,
  queryContext: any[] | Immutable.List<any>,
  viewId: string,
  references: any,
) => {
  const meta = { viewId };

  const body = {
    context: queryContext,
    sql,
    references,
  };

  updateBody(body);

  const apiCall = new APIV2Call().fullpath(href);

  return {
    [RSAA]: {
      types: [
        { type: NEW_UNTITLED_SQL_START, meta },
        readResponseAsJSON(NEW_UNTITLED_SQL_SUCCESS, meta),
        { type: NEW_UNTITLED_SQL_FAILURE, meta: { ...meta, noUpdate: true } },
      ],
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
      endpoint: apiCall,
    },
  };
};

// returns a RSAA to create a new temporary dataset using new_tmp_untitled_sql
export const newTmpUntitledSql = (
  sql: string,
  queryContext: any[],
  viewId: string,
  references: any,
  sessionId: string,
  newVersion: string,
) => {
  return (dispatch: any) => {
    const href = exploreUtils.getTmpUntitledSqlHref({ newVersion, sessionId });

    return dispatch(
      postNewUntitledSql(href, sql, queryContext, viewId, references),
    );
  };
};

// returns a RSAA to create a new temporary dataset using new_tmp_untitled_sql_and_run
export const newTmpUntitledSqlAndRun = (
  sql: string,
  queryContext: Immutable.List<any>,
  viewId: string,
  references: any,
  sessionId: string,
  newVersion: string,
) => {
  return (dispatch: any) => {
    const href = exploreUtils.getTmpUntitledSqlAndRunHref({
      newVersion,
      sessionId,
    });

    return dispatch(
      postNewUntitledSql(href, sql, queryContext, viewId, references),
    );
  };
};
