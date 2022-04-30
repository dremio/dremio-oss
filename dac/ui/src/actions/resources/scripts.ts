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
// @ts-ignore
import { RSAA } from 'redux-api-middleware';
import { APIV3Call } from '@app/core/APICall';

export const FETCH_SCRIPTS_START = 'FETCH_SCRIPTS_START';
export const FETCH_SCRIPTS_SUCCESS = 'FETCH_SCRIPTS_SUCCESS';
export const FETCH_SCRIPTS_FAILURE = 'FETCH_SCRIPTS_FAILURE';

export function fetchScripts(): any {
  const meta = { viewId: 'AllScripts' };
  const apiCall = new APIV3Call()
    .path('scripts')
    .params({ orderBy: '-modifiedAt' })
    .params({ maxResults: 100 });

  return {
    [RSAA]: {
      types: [
        { type: FETCH_SCRIPTS_START, meta },
        { type: FETCH_SCRIPTS_SUCCESS, meta },
        { type: FETCH_SCRIPTS_FAILURE, meta: { ...meta, notification: true } }
      ],
      method: 'GET',
      endpoint: apiCall
    }
  };
}


export const CREATE_SCRIPT_START = 'CREATE_SCRIPT_START';
export const CREATE_SCRIPT_SUCCESS = 'CREATE_SCRIPT_SUCCESS';
export const CREATE_SCRIPT_FAILURE = 'CREATE_SCRIPT_FAILURE';

export function createScript(payload: any): any {
  const meta = { viewId: 'AllScripts' };
  const apiCall = new APIV3Call().path('scripts');

  return {
    [RSAA]: {
      types: [
        { type: CREATE_SCRIPT_START, meta },
        { type: CREATE_SCRIPT_SUCCESS, meta },
        { type: CREATE_SCRIPT_FAILURE, meta }
      ],
      method: 'POST',
      endpoint: apiCall,
      body: JSON.stringify(payload)
    }
  };
}

export const UPDATE_SCRIPT_START = 'UPDATE_SCRIPT_START';
export const UPDATE_SCRIPT_SUCCESS = 'UPDATE_SCRIPT_SUCCESS';
export const UPDATE_SCRIPT_FAILURE = 'UPDATE_SCRIPT_FAILURE';

export function updateScript(payload: any, scriptId: string, hideFail: boolean): any {
  const meta = { viewId: 'AllScripts' };
  const apiCall = new APIV3Call()
    .path('scripts')
    .path(scriptId);

  return {
    [RSAA]: {
      types: [
        { type: UPDATE_SCRIPT_START, meta },
        { type: UPDATE_SCRIPT_SUCCESS, meta },
        { type: UPDATE_SCRIPT_FAILURE, meta: { ...meta, notification: !hideFail } }
      ],
      method: 'PUT',
      endpoint: apiCall,
      body: JSON.stringify(payload)
    }
  };
}

export const DELETE_SCRIPT_START = 'DELETE_SCRIPT_START';
export const DELETE_SCRIPT_SUCCESS = 'DELETE_SCRIPT_SUCCESS';
export const DELETE_SCRIPT_FAILURE = 'DELETE_SCRIPT_FAILURE';

export function deleteScript(scriptId: string): any {
  const meta = { viewId: 'AllScripts' };
  const apiCall = new APIV3Call()
    .path('scripts')
    .path(scriptId);

  return {
    [RSAA]: {
      types: [
        { type: DELETE_SCRIPT_START, meta },
        { type: DELETE_SCRIPT_SUCCESS, meta },
        { type: DELETE_SCRIPT_FAILURE, meta: { ...meta, notification: true } }
      ],
      method: 'DELETE',
      endpoint: apiCall
    }
  };
}

export const SELECT_ACTIVE_SCRIPT = 'SELECT_ACTIVE_SCRIPT';

export const setActiveScript = (script: any) => ({
  type: SELECT_ACTIVE_SCRIPT,
  script
});

export const CLEAR_SCRIPT_STATE = 'CLEAR_SCRIPT_STATE';

export const clearActiveScript = () => ({
  type: CLEAR_SCRIPT_STATE
});
