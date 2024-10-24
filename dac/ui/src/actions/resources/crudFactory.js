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
import { RSAA } from "redux-api-middleware";
import { CALL_MOCK_API } from "mockApi";
import { arrayOf, Schema } from "normalizr";
import schemaUtils from "utils/apiUtils/schemaUtils";
import Immutable from "immutable";
import { APIV2Call } from "#oss/core/APICall";
import { ApiError } from "redux-api-middleware";
import { isNotSoftware } from "dyn-load/utils/versionUtils";

const COMMON = { headers: { "Content-Type": "application/json" } };

const METHODS_WITH_REQUEST_BODY = new Set(["PUT", "POST"]);

export default (schemaOrName, { useLegacyPluralization = false } = {}) => {
  const schema =
    typeof schemaOrName === "string" ? new Schema(schemaOrName) : schemaOrName;
  const entityName = schema.getKey();
  const listSchema = { [entityName + "s"]: arrayOf(schema) };
  const upper = entityName.toUpperCase();
  const path = entityName.toLowerCase() + (useLegacyPluralization ? "" : "s");
  // const title = entityName.charAt(0).toUpperCase() + entityName.slice(1);
  const idAttribute = schema.getIdAttribute();

  const apiCallFactory = (method) => {
    return function call(idOrObject, meta) {
      let id = idOrObject;
      if (typeof idOrObject === "object") {
        id = get(idOrObject, idAttribute);
      }

      const apiCall = new APIV2Call().paths(`${path}/${id || ""}`);

      let successMeta = meta;

      if (method === "DELETE") {
        successMeta = {
          ...successMeta,
          success: true, // view reducer duck-type happiness
          entityRemovePaths: [[entityName, id]], // if we succeed, it should be gone
        };
        const version = get(idOrObject, "version");
        if (version !== undefined) {
          apiCall.params({ version });
        }
      }

      const failurePayload = isNotSoftware()
        ? {
            payload: (action, state, res) => {
              return res
                .json()
                .then((res) => {
                  return res;
                })
                .catch((e) => {
                  return new ApiError(res.status, e, undefined);
                });
            },
          }
        : {};

      const req = {
        [call.mock ? CALL_MOCK_API : RSAA]: {
          ...COMMON,
          types: [
            { type: `${upper}_${method}_START`, meta },
            schemaUtils.getSuccessActionTypeWithSchema(
              `${upper}_${method}_SUCCESS`,
              schema,
              successMeta,
            ),
            {
              type: `${upper}_${method}_FAILURE`,
              meta,
              ...failurePayload,
            }, // todo: failure not called? start called instead?!
          ],
          method,
          body: METHODS_WITH_REQUEST_BODY.has(method)
            ? JSON.stringify(idOrObject)
            : undefined,
          endpoint: apiCall,
          ...call.mock,
        },
      };

      return req;
    };
  };

  const calls = {
    // add schemas to output to make them re-usable.
    schema,
    listSchema,
  };
  for (const call of ["GET", "POST", "PUT", "DELETE"]) {
    calls[call.toLowerCase()] = apiCallFactory(call);
  }

  calls.getAll = function call(meta) {
    // todo: more DRY
    const method = "GET_ALL";
    const successMeta = { ...meta, entityClears: [entityName] }; // trigger a clear, since records may now be gone;

    const apiCall = new APIV2Call().paths(
      `${path}${useLegacyPluralization ? "s" : ""}`,
    );

    const req = {
      [call.mock ? CALL_MOCK_API : RSAA]: {
        ...COMMON,
        types: [
          { type: `${upper}_${method}_START`, meta },
          schemaUtils.getSuccessActionTypeWithSchema(
            `${upper}_${method}_SUCCESS`,
            listSchema, // todo: simplify and normalize responses
            successMeta,
          ),
          { type: `${upper}_${method}_FAILURE`, meta },
        ],
        method: "GET",
        endpoint: apiCall,
        ...call.mock,
      },
    };
    return req;
  };

  for (const call of Object.values(calls)) {
    call.dispatch = function () {
      return (dispatch) => dispatch(call(...arguments));
    };
  }

  return calls;
};

function get(obj, key) {
  // todo: kill this
  return Immutable.Iterable.isIterable(obj) ? obj.get(key) : obj[key];
}
