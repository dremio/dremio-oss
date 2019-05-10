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
import { CALL_API } from 'redux-api-middleware';
import { CALL_MOCK_API } from 'mockApi';
import { API_URL_V2 } from 'constants/Api';
import { Schema, arrayOf } from 'normalizr';
import schemaUtils from 'utils/apiUtils/schemaUtils';
import Immutable from 'immutable';

const COMMON = {headers: {'Content-Type': 'application/json'}};

const METHODS_WITH_REQUEST_BODY = new Set(['PUT', 'POST']);

export default (schemaOrName, {useLegacyPluralization = false} = {}) => {
  const schema = typeof schemaOrName === 'string' ? new Schema(schemaOrName) : schemaOrName;
  const entityName = schema.getKey();
  const listSchema = { [entityName + 's']: arrayOf(schema) };
  const upper = entityName.toUpperCase();
  const path = entityName.toLowerCase() + (useLegacyPluralization ? '' : 's');
  // const title = entityName.charAt(0).toUpperCase() + entityName.slice(1);
  const idAttribute = schema.getIdAttribute();

  const apiCallFactory = (method) => {
    return function call(idOrObject, meta) {
      let id = idOrObject;
      if (typeof idOrObject === 'object') {
        id = get(idOrObject, idAttribute);
      }

      let url = `${API_URL_V2}/${path}/${id || ''}`;

      let successMeta = meta;

      if (method === 'DELETE') {
        successMeta = {
          ...successMeta,
          success: true, // view reducer duck-type happiness
          entityRemovePaths: [[entityName, id]] // if we succeed, it should be gone
        };
        const version = get(idOrObject, 'version');
        if (version !== undefined) {
          url += '?version=' + version;
        }
      }

      const req = {
        [call.mock ? CALL_MOCK_API : CALL_API]: {
          ...COMMON,
          types: [
            {type: `${upper}_${method}_START`, meta},
            schemaUtils.getSuccessActionTypeWithSchema(`${upper}_${method}_SUCCESS`, schema, successMeta),
            {type: `${upper}_${method}_FAILURE`, meta} // todo: failure not called? start called instead?!
          ],
          method,
          body: METHODS_WITH_REQUEST_BODY.has(method) ? JSON.stringify(idOrObject) : undefined,
          endpoint: url,
          ...call.mock
        }
      };

      return req;
    };
  };

  const calls = {
    // add schemas to output to make them re-usable.
    schema,
    listSchema
  };
  for (const call of ['GET', 'POST', 'PUT', 'DELETE']) {
    calls[call.toLowerCase()] = apiCallFactory(call);
  }

  calls.getAll = function call(meta) { // todo: more DRY
    const method = 'GET_ALL';
    const successMeta = {...meta, entityClears: [entityName]}; // trigger a clear, since records may now be gone;
    const req = {
      [call.mock ? CALL_MOCK_API : CALL_API]: {
        ...COMMON,
        types: [
          {type: `${upper}_${method}_START`, meta},
          schemaUtils.getSuccessActionTypeWithSchema(
            `${upper}_${method}_SUCCESS`,
            listSchema, // todo: simplify and normalize responses
            successMeta
          ),
          {type: `${upper}_${method}_FAILURE`, meta}
        ],
        method: 'GET',
        endpoint: `${API_URL_V2}/${path}${useLegacyPluralization ? 's' : ''}/`,
        ...call.mock
      }
    };
    return req;
  };

  for (const call of Object.values(calls)) {
    call.dispatch = function() {
      return dispatch => dispatch(call(...arguments));
    };
  }

  return calls;
};


function get(obj, key) { // todo: kill this
  return Immutable.Iterable.isIterable(obj) ? obj.get(key) : obj[key];
}
