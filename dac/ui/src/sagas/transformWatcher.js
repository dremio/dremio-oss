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
import { take, race, call, put } from 'redux-saga/effects';
import invariant from 'invariant';
import { delay } from 'redux-saga';
import { startDatasetMetadataLoad, completeDatasetMetadataLoad } from '@app/actions/explore/view';
import { explorePageChanged } from '@app/sagas/runDataset';
import { navigateToNextDataset } from '@app/actions/explore/dataset/common';
import { startExplorePageListener, stopExplorePageListener } from '@app/actions/explore/dataset/data';

import { showConfirmationDialog, hideConfirmationDialog} from 'actions/confirmation';

import RealTimeTimer from 'components/RealTimeTimer';

import { RESET_NEW_QUERY } from 'actions/explore/view';
import { cancelTransform } from 'actions/explore/dataset/transform';
import timeUtils from 'utils/timeUtils';

import {
  getApiActionEntity
} from './utils';

export const MAX_TIME_PER_OPERATION = 10000;

export class TransformCanceledError {
  constructor(entity) {
    this.message = 'transform canceled.';
    this.name = 'TransformCanceledError';
    this.entity = entity;
  }
}

export class TransformCanceledByLocationChangeError {
  constructor(entity) {
    this.message = 'transform canceled by location change.';
    this.name = 'TransformCanceledByLocationChangeError';
    this.entity = entity;
  }
}

export class TransformFailedError {
  constructor(response) {
    this.name = 'TransformFailedError';
    this.response = response;
  }
}

// is used to load metadata for a dataset
export function* transformThenNavigate(action, viewId, navigateOptions) {
  try {
    yield put(startDatasetMetadataLoad());
    const response = yield call(
      performWatchedTransform,
      action,
      viewId
    );
    if (response && !response.error) {
      yield put(stopExplorePageListener());
      yield put(navigateToNextDataset(response, navigateOptions));
      return response;
    }
    throw new TransformFailedError(response);
  } finally {
    yield put(startExplorePageListener(false));
    yield put(completeDatasetMetadataLoad());
  }
}

//export for tests
export function* performWatchedTransform(apiAction, viewId) {
  invariant(viewId, 'viewId param is required for performWatchedTransform');
  const apiPromise = yield put(apiAction);
  // "apiPromise instanceof Promise" always return "false" in IE/Edge. So check for thenable
  invariant(apiPromise && apiPromise.then, 'action must return a Promise');

  const raceResults = yield race({
    tableTransform: apiPromise,
    cancel: call(cancelTransformWithModal, viewId),
    resetNewQuery: take(RESET_NEW_QUERY),
    locationChange: call(explorePageChanged)
  });

  if (!raceResults.cancel) {
    // TODO: this is a tad dangerous because it manipulates the global state under the assumption
    // the it has the long transform modal up. This is probably safe enough for now though (Chris).
    yield put(hideConfirmationDialog());
  }

  if (raceResults.cancel || raceResults.resetNewQuery) {
    throw new TransformCanceledError(getApiActionEntity(apiAction));
  }

  if (raceResults.locationChange) {
    throw new TransformCanceledByLocationChangeError(getApiActionEntity(apiAction));
  }
  return raceResults.tableTransform;
}

export function* cancelTransformWithModal(viewId) {
  yield call(delay, MAX_TIME_PER_OPERATION);
  let action;
  const confirmPromise = new Promise((resolve) => {
    action = showConfirmationDialog({
      title: la('Preparing Results…'),
      showOnlyConfirm: true,
      confirmText: la('Cancel'),
      text: [
        <span>
          {la('Elapsed time')}: <RealTimeTimer
            startTime={Date.now() - MAX_TIME_PER_OPERATION}
            formatter={(diff) => timeUtils.formatTimeDiff(diff)}
          />
        </span>
      ],
      confirm: resolve
    });
  });

  yield put(action);
  yield confirmPromise;
  yield put(cancelTransform(viewId));
  return true;
}
