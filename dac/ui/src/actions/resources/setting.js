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
import { normalize } from 'normalizr';
import Immutable from 'immutable';
import ApiUtils from '@app/utils/apiUtils/apiUtils';
import crudFactory from './crudFactory';

const resourceName = 'setting';

const actions = crudFactory(resourceName);

/**
 *
 * @param {string[]} requiredSettings - a list of setting keys that must be returned from server
 * @param {bool} includeSetSettings - set to true, if you neeed to receive a settings that were *
 * changed by any user
 * @param {string} viewId
 * @returns setting values for all {@see requiredSettings} + settings that were altered by the users
 */
export const getDefinedSettings = (requiredSettings, includeSetSettings, viewId) => async dispatch => {
  if (!requiredSettings) {
    throw new Error('requiredSettings must be provided');
  }
  const prefix = 'GET_DEFINED_SETTINGS';
  const meta = { viewId };
  dispatch({type: `${prefix}_START`, meta});

  try {
    const response = await ApiUtils.fetch('settings/', {
      method: 'POST', body: JSON.stringify({
        requiredSettings,
        includeSetSettings
      })
    }, 2);
    const json = await response.json();
    dispatch({
      type: `${prefix}_SUCCESS`,
      meta: { ...meta, entityClears: [resourceName]},
      payload: Immutable.fromJS(normalize(json, actions.listSchema))
    });
  } catch (e) {
    dispatch({type: `${prefix}_FAILURE`, meta});
  }
};
export default actions;
