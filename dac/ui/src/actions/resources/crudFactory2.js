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
import { API_URL_V3 } from 'constants/Api';
import Immutable from 'immutable';

const COMMON = {headers: {'Content-Type': 'application/json'}};

const METHODS_WITH_REQUEST_BODY = new Set(['PUT', 'POST']);

function extractEntities(data) {
  const response = {
    entities: {}
  };

  if ('type' in data) {
    // assume a single object
    response.entities[data.entityType] = {[data.id]: data};
    return Immutable.fromJS(response);
  }

  for (const key of Object.keys(data)) {
    const entity = data[key];
    if (Array.isArray(entity)) {
      for (const item of entity) {
        if ('entityType' in item) {
          const type = item.entityType;
          if (!response.entities[type]) {
            response.entities[type] = {};
          }

          response.entities[type][item.id] = item;
        }
      }
    }
  }

  return Immutable.fromJS(response);
}

export default (entityName, extras) => {
  const upper = entityName.toUpperCase();
  const path = entityName.toLowerCase();
  const idAttribute = 'id';

  const apiCallFactory = (method, overrides = {}) => {
    return function call(...args) {
      let idOrObject, meta, opts;
      if (method === 'GET_LIST') {
        [meta, opts] = args;
      } else {
        [idOrObject, meta, opts] = args;
      }

      let id = idOrObject;
      if (typeof idOrObject === 'object') {
        id = get(idOrObject, idAttribute);
      }

      // overrides
      const callPath = opts && opts.path || overrides.path || path;

      const apiEndpoint = API_URL_V3;
      let url = `${apiEndpoint}/${callPath}/${id || ''}`;
      const params = new URLSearchParams();

      const entityNameToUse = overrides.entityName || entityName;

      let successMeta = meta;
      if (method === 'DELETE') {
        successMeta = {
          ...successMeta,
          success: true, // view reducer duck-type happiness
          entityRemovePaths: [[entityNameToUse, id]] // if we succeed, it should be gone
        };
        const version = get(idOrObject, 'version');
        if (version !== undefined) {
          params.append('version', version);
        }
      } else if (method === 'GET_LIST') {
        successMeta = {...meta, entityClears: [entityNameToUse]}; // trigger a clear, since records may now be gone;
      }

      if (opts && opts.query) {
        for (const param of Object.keys(opts.query)) {
          params.append(param, opts.query[param]);
        }
      }

      const paramStr = params.toString();
      if (paramStr) {
        url += `?${paramStr}`;
      }

      const req = {
        [call.mock ? CALL_MOCK_API : CALL_API]: {
          ...COMMON,
          types: [
            {type: `${upper}_${method}_START`, meta},
            {
              type: `${upper}_${method}_SUCCESS`,
              meta: successMeta,
              payload: (action, state, res) => {
                const contentType = res.headers.get('Content-Type');
                if (contentType && contentType.includes('json')) {
                  return res.json().then((pureJSON) => {
                    return extractEntities(pureJSON);
                  });
                }
              }
            },
            {type: `${upper}_${method}_FAILURE`, meta} // todo: failure not called? start called instead?!
          ],
          method: method === 'GET_LIST' ? 'GET' : method,
          body: METHODS_WITH_REQUEST_BODY.has(method) ? JSON.stringify(idOrObject) : undefined,
          endpoint: url,
          ...call.mock
        }
      };

      return req;
    };
  };

  const calls = {};
  for (const call of ['GET', 'POST', 'PUT', 'DELETE']) {
    calls[call.toLowerCase()] = apiCallFactory(call);
  }
  calls.getList = apiCallFactory('GET_LIST');

  if (extras) {
    for (const name of Object.keys(extras)) {
      const extra = extras[name];
      calls[name] = apiCallFactory(extra.method, extra);
    }
  }

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
