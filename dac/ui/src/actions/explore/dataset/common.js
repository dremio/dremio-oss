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
import { push, replace } from "react-router-redux";
import urlParse from "url-parse";
import { collapseExploreSql } from "@app/actions/explore/ui";
import { PageTypes } from "@app/pages/ExplorePage/pageTypes";
import {
  changePageTypeInUrl,
  getPathPart,
} from "@app/pages/ExplorePage/pageTypeUtils";

import { APIV2Call } from "@app/core/APICall";
import schemaUtils from "utils/apiUtils/schemaUtils";
import apiUtils from "@app/utils/apiUtils/apiUtils";
import exploreUtils from "@app/utils/explore/exploreUtils";

export const RUN_TABLE_TRANSFORM_START = "RUN_TABLE_TRANSFORM_START";
export const RUN_TABLE_TRANSFORM_SUCCESS = "RUN_TABLE_TRANSFORM_SUCCESS";
export const RUN_TABLE_TRANSFORM_FAILURE = "RUN_TABLE_TRANSFORM_FAILURE";
import { addProjectBase as wrapBackendLink } from "dremio-ui-common/utilities/projectBase.js";
import * as sqlPaths from "dremio-ui-common/paths/sqlEditor.js";
import { getSonarContext } from "dremio-ui-common/contexts/SonarContext.js";

/**
 * common helper for different table operations
 */
export function postDatasetOperation({
  href,
  schema,
  viewId,
  dataset,
  uiPropsForEntity,
  invalidateViewIds,
  body,
  notificationMessage,
  metas = [],
  nextTable,
  sessionId,
}) {
  const meta = {
    viewId,
    invalidateViewIds,
    dataset,
    entity: dataset,
    nextTable,
    href,
  };
  const successMeta = notificationMessage
    ? {
        ...meta,
        notification: {
          message: notificationMessage,
          level: "success",
        },
      }
    : meta;

  const apiCall = new APIV2Call().fullpath(href);

  return {
    [RSAA]: {
      types: [
        { type: RUN_TABLE_TRANSFORM_START, meta: { ...meta, ...metas[0] } },
        schemaUtils.getSuccessActionTypeWithSchema(
          RUN_TABLE_TRANSFORM_SUCCESS, // this action doesn't do anything, but leaving here as a placeholder
          schema,
          { ...successMeta, ...metas[1] },
          uiPropsForEntity
        ),
        { type: RUN_TABLE_TRANSFORM_FAILURE, meta: { ...meta, ...metas[2] } },
      ],
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...apiUtils.getJobDataNumbersAsStringsHeader(),
      },
      body: body && JSON.stringify(sessionId ? { ...body, sessionId } : body),
      endpoint: apiCall,
    },
  };
}

export function _getNextJobId(fullDataset) {
  const newJobId = fullDataset.getIn(["jobId", "id"]);
  // only add the jobId to the query for run results (not approximate)
  return fullDataset.get("approximate") ? undefined : newJobId;
}

export function navigateToNextDataset(
  response,
  {
    replaceNav,
    linkName,
    isSaveAs,
    preserveTip,
    // we need to change a pathname only in the following cases
    // 1) Save as
    // 2) Edit original sql // handled by navigateAfterReapply
    // 3) When we write a new query and click Preview/Run to navigate to newUntitled page
    changePathname,
    renderScriptTab,
    isTransform,
    newJobId,
  } = {}
) {
  return (dispatch, getStore) => {
    changePathname = isSaveAs || changePathname;
    const location = getStore().routing.locationBeforeTransitions;
    const projectId = getSonarContext()?.getSelectedProjectId?.();
    const historyItem =
      getStore().resources.entities.get("historyItem") || new Immutable.Map({});
    const history =
      getStore().resources.entities.get("history") || new Immutable.Map({});
    const datasetUI =
      getStore().resources.entities.get("datasetUI") || new Immutable.Map({});

    const { tipVersion, openResults } = location.query || {};
    let targetPageType = PageTypes.default;
    let keepQuery = false;
    let collapseSqlEditor = false;

    //for graph, reflections, and wiki we have to keep query parameters and redirect to corresponding page
    for (const pageType of [
      PageTypes.graph,
      PageTypes.wiki,
      PageTypes.reflections,
      PageTypes.history,
    ]) {
      const urlPart = getPathPart(pageType);
      //check if url ends with page type
      if (location.pathname.endsWith(urlPart) && !openResults) {
        targetPageType = pageType;
        keepQuery = true;
        collapseSqlEditor = true; // collapse an editor for wiki and graph pages
        break;
      }
    }

    if (collapseSqlEditor) {
      dispatch(collapseExploreSql());
    }

    const payload = response.payload || Immutable.Map();
    const resultId = payload.get("result");
    const nextDataset = payload.getIn(["entities", "datasetUI", resultId]);
    const fullDataset = payload.getIn(["entities", "fullDataset", resultId]);
    if (!nextDataset || !fullDataset) {
      throw new Error("transform did not return next dataset");
    }

    // Shouldn't push if in virtual history, should push for physical history
    const previousTipVerison = location.query && location.query.tipVersion;
    const [isInVirtualHistory, isInPhysicalHistory] =
      exploreUtils.getIfInEntityHistory(
        history,
        historyItem,
        datasetUI,
        previousTipVerison
      );

    const isUnsavedWithDataset = !nextDataset.getIn([
      "apiLinks",
      "namespaceEntity",
    ]);
    const goToSqlRunner =
      renderScriptTab ||
      (isUnsavedWithDataset && (!isInVirtualHistory || isInPhysicalHistory));

    const nextVersion = nextDataset.get("datasetVersion");

    let link =
      (nextDataset &&
        wrapBackendLink(nextDataset.getIn(["links", linkName || "self"]))) ||
      "";
    if (goToSqlRunner)
      link = `${sqlPaths.unsavedDatasetPath.link({
        projectId,
      })}?version=${nextVersion}`;

    const parsedLink = urlParse(link, true);

    const nextPath = goToSqlRunner
      ? sqlPaths.unsavedDatasetPath.link({
          projectId,
        })
      : location.pathname;
    let pathname = changePageTypeInUrl(
      changePathname ? parsedLink.pathname : nextPath,
      targetPageType
    );

    // if coming from Open Results, root space needs to be updated if dataset exists in /source or /home
    if (
      openResults &&
      pathname.startsWith("/space") &&
      (nextDataset.getIn(["links", "self"]).startsWith("/source") ||
        nextDataset.getIn(["links", "self"]).startsWith("/home"))
    ) {
      const editLink = nextDataset.getIn(["links", "edit"]);
      pathname = `${editLink.substring(0, editLink.indexOf("?"))}`;
    }

    const create = location.query?.create;
    const jobId = _getNextJobId(fullDataset);
    const mode = isSaveAs ? "edit" : location.query?.mode;
    const query = {
      ...(keepQuery ? location.query : {}), // Initial dataset request will navigate. Need to not clobber graph query params.
      ...parsedLink.query,
      ...(jobId ? { jobId } : newJobId ? { jobId: newJobId } : {}),
      ...(mode ? { mode } : {}),
      ...(isUnsavedWithDataset && create ? { create } : {}),
      version: nextVersion,
      tipVersion: preserveTip ? tipVersion || nextVersion : nextVersion,
      ...(openResults ? { openResults: "true" } : {}),
    };
    const action = replaceNav ? replace : push;

    const nextState = {
      ...(isTransform ? { isTransform } : {}),
      ...(goToSqlRunner && renderScriptTab ? { renderScriptTab } : {}),
    };

    const state = isSaveAs ? { afterDatasetSave: true } : nextState;

    return dispatch(action({ pathname, query, state }));
  };
}
