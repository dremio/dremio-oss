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
import { RSAA } from 'redux-api-middleware';
import { push } from 'react-router-redux';

import provisionSchema from 'schemas/provision';
import schemaUtils from 'utils/apiUtils/schemaUtils';
import { APIV2Call } from '@app/core/APICall';

export const LOAD_AWS_DEFAULTS_START = 'LOAD_AWS_DEFAULTS_START';
export const LOAD_AWS_DEFAULTS_SUCCESS = 'LOAD_AWS_DEFAULTS_SUCCESS';
export const LOAD_AWS_DEFAULTS_FAILURE = 'LOAD_AWS_DEFAULTS_FAILURE';

export const loadAwsDefaults = () => {
  const apiCall = new APIV2Call().paths('provision/aws/defaults');

  return {
    [RSAA]: {
      types: [
        LOAD_AWS_DEFAULTS_START,
        LOAD_AWS_DEFAULTS_SUCCESS,
        LOAD_AWS_DEFAULTS_FAILURE
      ],
      method: 'GET',
      endpoint: apiCall
    }
  };
};

export const LOAD_PROVISIONING_START = 'LOAD_PROVISIONING_START';
export const LOAD_PROVISIONING_SUCCESS = 'LOAD_PROVISIONING_SUCCESS';
export const LOAD_PROVISIONING_FAILURE = 'LOAD_PROVISIONING_FAILURE';

function fetchLoadProvisioning(provisionType, viewId) {
  const meta = {provisionType, viewId};
  const typeQuery = provisionType ? `?type=${provisionType}` : '';

  const apiCall = new APIV2Call().paths(`provision/clusters${typeQuery}`);

  return {
    [RSAA]: {
      types: [
        {type: LOAD_PROVISIONING_START, meta},
        {type: LOAD_PROVISIONING_SUCCESS, meta: {...meta, entityClears: ['provision']}},
        {type: LOAD_PROVISIONING_FAILURE, meta}
      ],
      method: 'GET',
      endpoint: apiCall
    }
  };
}

export function loadProvision(provisionType, viewId)  {
  return (dispatch) => {
    return dispatch(fetchLoadProvisioning(provisionType, viewId));
  };
}

export const UPDATE_WORKERS_SIZE_START = 'UPDATE_WORKERS_SIZE_START';
export const UPDATE_WORKERS_SIZE_SUCCESS = 'UPDATE_WORKERS_SIZE_SUCCESS';
export const UPDATE_WORKERS_SIZE_FAILURE = 'UPDATE_WORKERS_SIZE_FAILURE';

function fetchUpdateWorkersSize(form, provisionId, viewId) {
  const meta = {viewId};

  const apiCall = new APIV2Call().paths(`provision/cluster/${provisionId}/dynamicConfig`);

  return {
    [RSAA]: {
      types: [
        {type: UPDATE_WORKERS_SIZE_START, meta},
        schemaUtils.getSuccessActionTypeWithSchema(UPDATE_WORKERS_SIZE_SUCCESS, provisionSchema, meta),
        {type: UPDATE_WORKERS_SIZE_FAILURE, meta}
      ],
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(form),
      endpoint: apiCall
    }
  };
}

export function changeWorkersSize(form, provisionId, viewId) { // todo: rename to ~'putDynamicConfig'
  return (dispatch) => {
    return dispatch(fetchUpdateWorkersSize(form, provisionId, viewId));
  };
}

export const REMOVE_PROVISION_START = 'REMOVE_PROVISION_START';
export const REMOVE_PROVISION_SUCCESS = 'REMOVE_PROVISION_SUCCESS';
export const REMOVE_PROVISION_FAILURE = 'REMOVE_PROVISION_FAILURE';

function fetchRemoveProvision(provisionId, viewId) {
  const meta = {provisionId, viewId};

  const apiCall = new APIV2Call().paths(`provision/cluster/${provisionId}`);

  return {
    [RSAA]: {
      types: [
        {type: REMOVE_PROVISION_START, meta},
        {type: REMOVE_PROVISION_SUCCESS, meta},
        {type: REMOVE_PROVISION_FAILURE, meta}
      ],
      method: 'DELETE',
      endpoint: apiCall
    }
  };
}

export function removeProvision(provisionId, viewId) {
  return (dispatch) => {
    return dispatch(fetchRemoveProvision(provisionId, viewId));
  };
}

export const CREATE_PROVISION_START = 'CREATE_PROVISION_START';
export const CREATE_PROVISION_SUCCESS = 'CREATE_PROVISION_SUCCESS';
export const CREATE_PROVISION_FAILURE = 'CREATE_PROVISION_FAILURE';


function fetchCreateProvision(form, viewId) {
  const meta = {viewId};

  const apiCall = new APIV2Call().paths('provision/cluster');

  return {
    [RSAA]: {
      types: [
        {type: CREATE_PROVISION_START, meta},
        schemaUtils.getSuccessActionTypeWithSchema(CREATE_PROVISION_SUCCESS, provisionSchema, meta),
        {type: CREATE_PROVISION_FAILURE, meta}
      ],
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(form),
      endpoint: apiCall
    }
  };
}

export function createProvision(form, viewId) {
  return (dispatch) => {
    return dispatch(fetchCreateProvision(form, viewId));
  };
}

export const EDIT_PROVISION_START = 'EDIT_PROVISION_START';
export const EDIT_PROVISION_SUCCESS = 'EDIT_PROVISION_SUCCESS';
export const EDIT_PROVISION_FAILURE = 'EDIT_PROVISION_FAILURE';

function fetchEditProvision(data, viewId) {
  const meta = {viewId, entityId: data.id};

  const apiCall = new APIV2Call()
    .paths('provision/cluster')
    .path(data.id);

  return {
    [RSAA]: {
      types: [
        {type: EDIT_PROVISION_START, meta},
        schemaUtils.getSuccessActionTypeWithSchema(EDIT_PROVISION_SUCCESS, provisionSchema, meta),
        {type: EDIT_PROVISION_FAILURE, meta}
      ],
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(data),
      endpoint: apiCall
    }
  };
}

export function editProvision() {
  return (dispatch) => {
    return dispatch(fetchEditProvision(...arguments));
  };
}

export function openAddProvisionModal(clusterType) {
  return (dispatch, getStore) => {
    const location = getStore().routing.locationBeforeTransitions;
    return dispatch(push({
      ...location,
      state: {modal: 'AddProvisionModal', clusterType}
    }));
  };
}

export function openEditProvisionModal(provisionId, clusterType) {
  return (dispatch, getStore) => {
    const location = getStore().routing.locationBeforeTransitions;
    return dispatch(push({
      ...location,
      state: {modal: 'AddProvisionModal', provisionId, clusterType}
    }));
  };
}

export function openMoreInfoProvisionModal(entityId) {
  return (dispatch, getStore) => {
    const location = getStore().routing.locationBeforeTransitions;
    return dispatch(push({
      ...location,
      state: {modal: 'MoreInfoProvisionModal', entityId}
    }));
  };
}

export function openAdjustWorkersModal(entityId) {
  return (dispatch, getStore) => {
    const location = getStore().routing.locationBeforeTransitions;
    return dispatch(push({
      ...location,
      state: {modal: 'AdjustWorkersModal', entityId}
    }));
  };
}
