/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
import { all, put, call, takeEvery, spawn, select, take, fork} from 'redux-saga/effects';
import invariant from 'invariant';
import { newUntitledSql, newUntitledSqlAndRun } from 'actions/explore/dataset/new';
import { PERFORM_TRANSFORM, runTableTransform } from 'actions/explore/dataset/transform';
import {
  PERFORM_TRANSFORM_AND_RUN,
  runDataset,
  transformAndRunDataset,
  RUN_DATASET_SQL
} from 'actions/explore/dataset/run';
import { expandExploreSql } from 'actions/explore/ui';

import { loadTableData, cancelDataLoad, loadDataset, focusSqlEditorSaga } from '@app/sagas/performLoadDataset';
import { transformHistoryCheck } from 'sagas/transformHistoryCheck';
import { getExploreState, getExplorePageDataset } from 'selectors/explore';
import { getExploreViewState } from 'selectors/resources';

import apiUtils from 'utils/apiUtils/apiUtils';
import { needsTransform } from 'sagas/utils';

import {
  transformThenNavigate, TransformFailedError, TransformCanceledError, TransformCanceledByLocationChangeError
} from './transformWatcher';

export default function* watchPerformTransform() {
  yield all([
    takeEvery(PERFORM_TRANSFORM, handlePerformTransform),
    takeEvery(PERFORM_TRANSFORM_AND_RUN, handlePerformTransformAndRun),
    fork(processRunDatasetSql)
  ]);
}

function* processRunDatasetSql() {
  while (true) { // eslint-disable-line no-constant-condition
    const action = yield take(RUN_DATASET_SQL);
    yield call(handleRunDatasetSql, action);
  }
}

export function* handlePerformTransformAndRun({ payload }) {
  yield* performTransform({...payload, isRun: true});
}

export function* handlePerformTransform({ payload }) {
  yield* performTransform(payload);
}

// callback = function(didTransform, dataset)
export function* performTransform({
  dataset, currentSql, queryContext, viewId, nextTable, isRun, transformData,
  callback, // function(didTransform, dataset)
  forceDataLoad // a boolean flag that forces a preview reload, when nothing is changed
}) {
  try {
    const { apiAction, navigateOptions } = yield call(getFetchDatasetMetaAction, {
      dataset, currentSql, queryContext, viewId, nextTable, isRun, transformData,
      forceDataLoad
    });

    let resultDataset = dataset;
    const didTransform = !!apiAction;
    if (apiAction) {
      yield call(cancelDataLoad);
      // response will be not empty. See transformThenNavigate
      const response = yield call(transformThenNavigate, apiAction, viewId, navigateOptions);
      if (!response || response.error) {
        throw new Error('transformThenNavigate must return not empty response without error');
      }
      resultDataset = apiUtils.getEntityFromResponse('datasetUI', response);

      // we successfully loaded a dataset metadata. We need load table data for it
      // 'spawn' means async data loading. 'call' - sync data loading
      yield spawn(loadTableData, resultDataset.get('datasetVersion'), isRun);
      // at this moment top part of an editor should be already enabled so we could focus the editor
      yield call(focusSqlEditorSaga); // DX-9819 focus sql editor when transform is done
    }

    if (callback) {
      yield call(callback, didTransform, resultDataset);
    }

    if (dataset.get('isNewQuery')) {
      // Expand the sql box state after executing New Query
      yield put(expandExploreSql());
    }
  } catch (e) {
    // do nothing for api errors. view state handles them.
    if (
      !apiUtils.isApiError(e) &&
      !(e instanceof TransformFailedError) &&
      !(e instanceof TransformCanceledError) &&
      !(e instanceof TransformCanceledByLocationChangeError)
    ) {
      throw e;
    }
  }
}

export function* handleRunDatasetSql({ isPreview }) {
  const dataset = yield select(getExplorePageDataset);
  const exploreViewState = yield select(getExploreViewState);
  const exploreState = yield select(getExploreState);
  const viewId = exploreViewState.get('viewId');
  const currentSql = exploreState.view.currentSql;
  const queryContext = exploreState.view.queryContext;

  if (yield call(proceedWithDataLoad, dataset, queryContext, currentSql)) {
    const performTransformParam = {
      dataset,
      currentSql,
      queryContext,
      viewId
    };
    if (isPreview) {
      performTransformParam.forceDataLoad = true;
    } else {
      performTransformParam.isRun = true;
    }

    yield call(performTransform, performTransformParam);
  }
}


/*
 * Helpers
 */

// do we need so many different endpoints? We should refactor our api to reduce the number
// Returns an action that should be triggered for case of Run/Preview
// export for tests
export function* getFetchDatasetMetaAction({
  dataset,
  currentSql,
  queryContext,
  viewId,
  nextTable,
  isRun,
  transformData,
  forceDataLoad
}) {
  const sql = currentSql || dataset.get('sql');
  invariant(!queryContext || queryContext instanceof Immutable.List, 'queryContext must be Immutable.List');
  const finalTransformData = yield call(getTransformData, dataset, sql, queryContext, transformData);
  let apiAction;
  let navigateOptions;

  if (isRun) {
    if (!dataset.get('datasetVersion')) {
      // dataset is not created. Create with sql and run.
      apiAction = yield call(newUntitledSqlAndRun, sql, queryContext, viewId);
      navigateOptions = { changePathname: true }; //changePathname to navigate to newUntitled
    } else if (finalTransformData) {
      // transform is requested. Transform and run.
      apiAction = yield call(transformAndRunDataset, dataset, finalTransformData, viewId);
    } else {
      // just run
      apiAction = yield call(runDataset, dataset, viewId);
      navigateOptions = { replaceNav: true, preserveTip: true };
    }
  } else {
    // VBesschetnov original code has the following flow:
    // ----------------------------------------------------------------------------
    // if (!dataset.get('datasetVersion')) { ... }
    // if (finalTransformData) { ... }
    // ----------------------------------------------------------------------------
    // I could not imagine a case when dataset without version could be transformed. It looks more
    // like creation of a new VDS. So I would refactor the code with an assumption that we will
    // never face got a situation when !dataset.get('datasetVersion') && !!finalTransformData
    // The one possible case that come in my mind is when we navigate to existent VDS by direct link
    // and a user will try to alter a query, until we receive a current version of VDS. But it is
    // unlikely as we block the UI until VDS info would be loaded.
    // I will throw an exception in case if the assumption would be invalid
    if (!dataset.get('datasetVersion') && finalTransformData) {
      throw new Error('this case is not supported. Code is built in assumption, that this case will never happen. We need investigate this case');
    }
    // ----------------------------------------------------------------------------


    if (!dataset.get('datasetVersion')) {
      apiAction = yield call(newUntitledSql, sql, queryContext && queryContext.toJS(), viewId);
      navigateOptions = { changePathname: true }; //changePathname to navigate to newUntitled
    } else if (finalTransformData) {
      apiAction = yield call(runTableTransform, dataset, finalTransformData, viewId, nextTable);
    } else {
      // just preview existent dataset
      if (forceDataLoad) {
        apiAction = yield call(loadDataset, dataset, viewId);
      }
      navigateOptions = { replaceNav: true, preserveTip: true };
    }
  }


  // api action could be empty only for this case, when there is an existent dataset without changes
  // and data reload was not enforced
  if (!apiAction && !(dataset.get('datasetVersion') && !finalTransformData && !forceDataLoad)) {
    throw new Error('we should not appear here');
  }
  return {
    apiAction,
    navigateOptions
  };
}

/**
 * Returns updateSQL transforms if there isn't already transformData, and dataset is not new.
 */
export const getTransformData = (dataset, sql, queryContext, transformData) => {
  if (dataset.get('isNewQuery') || transformData) {
    return transformData;
  }

  const savedSql = (dataset && dataset.get('sql')) || '';
  const savedContext = dataset && dataset.get('context') || Immutable.List();
  if ((sql !== null && savedSql !== sql) || !savedContext.equals(queryContext || Immutable.List())) {
    return { type: 'updateSQL', sql, sqlContextList: queryContext && queryContext.toJS()};
  }
};

/**
 * Checks if data load should be done despite the possible data loses
 *
 *  - When a user changes a sql or query that will result with a new dataset version.
 * But if a user changed some previous version, he could loose some future history versions.
 * In that case we show a confirmation to the user and he could cancel the transformation.
 * We return {@see false} form this saga in that case, otherwise {@see true}.
 *
 *  - If user does change sql or context, he may want to just load data for a current
 * version of a dataset. In that case the method returns {@see true}.
 *
 * @exports only for tests
 * @param {Immutable.Map} dataset
 * @param {Immutable.List} queryContext
 * @param {string} currentSql
 * @returns {boolean} true if we should proceed with transformation
 */
export function* proceedWithDataLoad(dataset, queryContext, currentSql) {
  // check that we have a transform case, i.e. sql or context is changed
  const sqlOrContextChanged = needsTransform(dataset, queryContext, currentSql);

  if (sqlOrContextChanged) {
    return (yield call(transformHistoryCheck, dataset));
  }
  // nothing is changed. We should allow to load data
  return true;
}
