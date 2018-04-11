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
import { select, put, take, race, call, takeEvery } from 'redux-saga/effects';

import { TRANSFORM_HISTORY_CHECK } from 'actions/explore/dataset/transform';
import { showConfirmationDialog} from 'actions/confirmation';
import { getHistoryItems } from 'selectors/explore';
import { getLocation } from 'selectors/routing';

import { getLocationChangePredicate } from './utils';

/**
 * Saga that confirms with user that they want to abandon history.
 * TODO Consider changings this to promises instead of a saga, so it can work better with
 * apiUtils.attachFormSubmitHandlers. It's not so complex as to need a saga.
 */
export default function* watchTransformHistoryCheck() {
  yield takeEvery(TRANSFORM_HISTORY_CHECK, handleTransformHistoryCheck);
}

export function* handleTransformHistoryCheck(action) {
  const {meta: {dataset, continueCallback, cancelCallback}} = action;
  const confirmed = yield call(transformHistoryCheck, dataset);
  if (confirmed && continueCallback) {
    yield call(continueCallback);
  } else if (!confirmed && cancelCallback) {
    yield call(cancelCallback);
  }
}

export function *transformHistoryCheck(dataset) {
  const historyItems = yield select(getHistoryItems, dataset.get('tipVersion'));

  const showModal = yield call(shouldShowWarningModal, historyItems, dataset);
  if (showModal) {
    return yield call(confirmTransform);
  }
  return true;
}

/*
 * Helpers
 */

export const shouldShowWarningModal = (historyItems, dataset) => {
  const index = historyItems.findIndex(item => item.get('datasetVersion') === dataset.get('datasetVersion'));
  return index !== 0 && index !== -1;
};

export function* confirmTransform() {
  let action;
  const confirmPromise = new Promise((resolve, reject) => {
    action = showConfirmationDialog({
      title: la('History Warning'),
      text: [
        la('Performing this action will cause you to lose history.'),
        la('Are you sure you want to continue?')
      ],
      confirmText: la('Continue'),
      cancelText: la('Cancel'),
      confirm: () => resolve(true), // resolves to true so that confirm below has a truthy value
      cancel: reject
    });
  });
  yield put(action);

  const location = yield select(getLocation);
  try {
    const {confirm} = yield race({
      confirm: confirmPromise,
      locationChange: take(getLocationChangePredicate(location))
    });

    if (confirm) {
      return true;
    }
    return false;
  } catch (e) {
    // modal was canceled and promise rejected
    return false;
  }
}
