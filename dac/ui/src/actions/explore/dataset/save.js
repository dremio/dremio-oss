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
import { push } from 'react-router-redux';
import flatten from 'lodash/flatten';
import invariant from 'invariant';

import exploreUtils from 'utils/explore/exploreUtils';

import { datasetWithoutData } from 'schemas/v2/fullDataset';
import { constructFullPathAndEncode, getRouteParamsFromLocation } from 'utils/pathUtils';
import apiUtils from 'utils/apiUtils/apiUtils';
import { performNextAction } from 'actions/explore/nextAction';

import { navigateToNextDataset, postDatasetOperation } from './common';
import { navigateAfterReapply } from './reapply';

export function saveDataset(dataset, viewId, nextAction) {
  return (dispatch) => {
    if (dataset.getIn(['fullPath', 0]) === 'tmp') {
      return dispatch(saveAsDataset(nextAction));
    }
    return dispatch(submitSaveDataset(dataset, viewId)).then((response) => {
      if (!response.error) {
        return dispatch(afterSaveDataset(response, nextAction));
      }
      return response;
    });
  };
}

export function saveAsDataset(nextAction, message) {
  return (dispatch, getStore) => {
    const location = getStore().routing.locationBeforeTransitions;
    return dispatch(push({
      ...location,
      state: {modal: 'SaveAsDatasetModal', nextAction, message}
    }));
  };
}

export function submitSaveDataset(dataset, viewId) {
  return (dispatch) => {
    const savedTag = dataset.get('version');
    const link = dataset.getIn(['apiLinks', 'self']);
    const href = `${link}/save?savedTag=${savedTag}`;
    return dispatch(postDatasetOperation({
      href,
      viewId,
      schema: datasetWithoutData,
      metas: [{}, {mergeEntities: true}], // Save returns dataset and history only, so need to merge fullDataset
      notificationMessage: la('Successfully saved.')
    }));
  };
}

export function submitSaveAsDataset(name, fullPath, location, reapply) {
  return (dispatch) => {
    const routeParams = getRouteParamsFromLocation(location);
    const link = exploreUtils.getHrefForTransform(routeParams, location);
    const href = `${link}/${reapply ?
      'reapplyAndSave' : 'save'}?as=${constructFullPathAndEncode(fullPath.concat(name))}`;
    return dispatch(postDatasetOperation({
      href,
      schema: datasetWithoutData,
      notificationMessage: la('Successfully saved.'),
      metas: [{}, {mergeEntities: true}]
    }));
  };
}

export function submitReapplyAndSaveAsDataset(name, fullPath, location) {
  return (dispatch) => {
    const routeParams = getRouteParamsFromLocation(location);

    const link = exploreUtils.getHrefForTransform(routeParams, location);
    const href = `${link}/reapplyAndSave?as=${constructFullPathAndEncode(fullPath.concat(name))}`;
    return dispatch(postDatasetOperation({
      href,
      schema: datasetWithoutData,
      notificationMessage: la('Successfully saved.'),
      metas: [{}, {mergeEntities: true}]
    })).then((response) => {
      if (!response.error) {
        dispatch(navigateAfterReapply(response, true));
      }
      return response;
    });
  };
}

// response must be successful save
export function afterSaveDataset(response, nextAction) {
  invariant(!response.error, 'response cannot be an error');
  return (dispatch) => {
    const nextDataset = apiUtils.getEntityFromResponse('datasetUI', response);
    const historyItems = response.payload.getIn(['entities', 'historyItem']).toList();
    dispatch(navigateToNextDataset(response, {replaceNav: true, isSaveAs: true}));

    // old versions need to be reloaded for occ version and possible change in display name
    dispatch(deleteOldDatasetVersions(nextDataset.get('datasetVersion'), historyItems));
    dispatch(performNextAction(nextDataset, nextAction));
    return response;
  };
}
export const DELETE_OLD_DATASET_VERSIONS = 'DELETE_OLD_DATASET_VERSIONS';

export function deleteOldDatasetVersions(currentVersion, historyItemsList) {
  const datasetVersions = historyItemsList
    .map((item) => item.get('datasetVersion'))
    .filter((datasetVersion) => datasetVersion !== currentVersion).toJS();

  return {
    type: DELETE_OLD_DATASET_VERSIONS,
    meta: {
      entityRemovePaths: flatten(datasetVersions
        .map((id) => ['datasetUI', 'tableData', 'fullDataset', 'history']
          .map( entityType => [entityType, id])))
    }
  };
}
