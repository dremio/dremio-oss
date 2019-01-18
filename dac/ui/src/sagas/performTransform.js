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
import { put, call, takeEvery } from 'redux-saga/effects';
import invariant from 'invariant';

import { PERFORM_TRANSFORM } from 'actions/explore/dataset/transform';
import { PERFORM_TRANSFORM_AND_RUN } from 'actions/explore/dataset/run';

import { navigateToNextDataset } from 'actions/explore/dataset/common';
import { newUntitledSql, newUntitledSqlAndRun } from 'actions/explore/dataset/new';
import { runTableTransform } from 'actions/explore/dataset/transform';
import { runDataset, transformAndRunDataset } from 'actions/explore/dataset/run';
import { expandExploreSql } from 'actions/explore/ui';
import { loadTableData } from '@app/sagas/performLoadDataset';

import apiUtils from 'utils/apiUtils/apiUtils';

import { loadDataset } from './performLoadDataset';

import {
  performWatchedTransform, TransformCanceledError, TransformCanceledByLocationChangeError
} from './transformWatcher';

export class TransformFailedError {
  constructor(response) {
    this.name = 'TransformFailedError';
    this.response = response;
  }
}

export default function* watchPerformTransform() {
  yield [
    takeEvery(PERFORM_TRANSFORM, handlePerformTransform),
    takeEvery(PERFORM_TRANSFORM_AND_RUN, handlePerformTransformAndRun)
  ];
}

export function* handlePerformTransformAndRun({ payload }) {
  yield* performTransform({...payload, isRun: true});
}

export function* handlePerformTransform({ payload }) {
  yield* performTransform(payload);
}

// callback = function(didTransform, dataset)
export function* performTransform({
  dataset, currentSql, queryContext, viewId, nextTable, isRun, transformData, callback
}) {
  const sql = currentSql || dataset.get('sql');
  invariant(!queryContext || queryContext instanceof Immutable.List, 'queryContext must be Immutable.List');
  const finalTransformData = yield call(getTransformData, dataset, sql, queryContext, transformData);

  try {
    let nextDataset = dataset;
    let response;
    if (isRun) {
      response = yield call(doRun, nextDataset, sql, queryContext, finalTransformData, viewId);
    } else {
      if (!dataset.get('datasetVersion')) {
        const discardResults = Boolean(finalTransformData);
        nextDataset = yield call(createDataset, dataset, sql, queryContext, viewId, discardResults);
      }
      if (finalTransformData) {
        response = yield call(
          transformThenNavigate, runTableTransform(nextDataset, finalTransformData, viewId, nextTable), viewId
        );
      }
      // response can be null if performTransform was called to createNew.
    }

    if (callback) {
      if (!response) {
        yield call(callback, false, nextDataset);
      } else if (!response.error) {
        nextDataset = response ? apiUtils.getEntityFromResponse('datasetUI', response) : nextDataset;
        yield call(callback, true, nextDataset);
      }
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


/*
 * Helpers
 */

/**
 * Returns updateSQL transforms if there isn't already transformData, and dataset is not new.
 */
export const getTransformData = (dataset, sql, queryContext, transformData) => {
  if (dataset.get('isNewQuery') || transformData) {
    return transformData;
  }

  const savedSql = dataset && dataset.get('sql');
  const savedContext = dataset && dataset.get('context') || Immutable.List();
  if ((sql !== undefined && savedSql !== sql) || !savedContext.equals(queryContext)) {
    return { type: 'updateSQL', sql, sqlContextList: queryContext && queryContext.toJS()};
  }
};

export function* createDataset(dataset, sql, queryContext, viewId, discardResults) {
  let response;

  // This is used when we are doing 2 requests back to back, and we don't want to update the ui with the results
  if (discardResults) {
    response = yield call(
      performWatchedTransform, newUntitledSql(sql, queryContext && queryContext.toJS(), viewId, true), viewId);
  } else if (dataset.get('needsLoad')) {
    // TODO Consider removing this after DX-4937: initial request will always returns dataset even when query fails.
    // It's possible for the dataset to not be loaded if the initial load failed. In this case, try to load it again.
    const promise = yield call(loadDataset, dataset, viewId);
    response = yield promise;

    yield put(navigateToNextDataset(response));
  } else {
    response = yield call(
      transformThenNavigate, newUntitledSql(sql, queryContext && queryContext.toJS(), viewId, false), viewId);
  }
  if (response.error) {
    throw new TransformFailedError('createDataset failed');
  }
  return apiUtils.getEntityFromResponse('datasetUI', response);
}

export function* doRun(dataset, sql, queryContext, transformData, viewId) {
  let response;
  if (!dataset.get('datasetVersion')) {
    // dataset is not created. Create with sql and run.
    response = yield call(transformThenNavigate, newUntitledSqlAndRun(sql, queryContext, viewId), viewId);
  } else if (transformData) {
    // transform is requested. Transfrom and run.
    response = yield call(transformThenNavigate, transformAndRunDataset(dataset, transformData, viewId), viewId);
  } else {
    // just run
    response  = yield call(
      transformThenNavigate, runDataset(dataset, viewId), viewId, {replaceNav: true, preserveTip: true}
    );
  }
  if (response && !response.error) {
    const newDatasetVersion = response.payload.get('result');
    yield call(loadTableData, newDatasetVersion, true); // true means we should force data reload
  }
  return response;
}

export function* transformThenNavigate(action, viewId, navigateOptions) {
  const response = yield call(
    performWatchedTransform,
    action,
    viewId
  );
  if (response && !response.error) {
    yield put(navigateToNextDataset(response, navigateOptions));
    return response;
  }
  throw new TransformFailedError(response);
}
