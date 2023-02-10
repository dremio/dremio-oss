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
import { normalize } from "normalizr";
import Immutable from "immutable";

import { mapDataset } from "apiMappers/datasetMapper";
import { applyDecorators } from "utils/decorators";

class SchemaUtils {
  getSuccessActionTypeWithSchema(
    type,
    schema,
    meta,
    key,
    value,
    transform,
    schemaKey
  ) {
    return {
      type,
      meta,
      payload: (action, state, res) => {
        const contentType = res.headers.get("Content-Type");
        if (contentType && contentType.indexOf("json") !== -1) {
          return res.json().then((pureJson) => {
            const finalJson =
              !!transform && typeof transform === "function"
                ? transform(pureJson)
                : pureJson;
            if (typeof key === "object") {
              key.forEach((item) => {
                finalJson[item.key] = item.value;
              });
            } else if (key && value) {
              finalJson[key] = value; //when no id from api, we generate it
            }
            const hash = {
              fullDataset: mapDataset,
            };
            const payload = Immutable.fromJS(
              normalize(
                (hash[schema._key] && hash[schema._key](finalJson, key)) ||
                  finalJson,
                schema
              )
            );
            let newPayLoad = payload.set(
              "entities",
              payload.get("entities").size === 0 && schemaKey
                ? Immutable.fromJS({ [schemaKey]: {} })
                : applyDecorators(payload.get("entities"))
            );

            if (
              payload.getIn([
                "entities",
                "accessControlList",
                finalJson.id,
                "grants",
              ]) !== undefined &&
              type === "ACCESS_CONTROL_GET_SUCCESS"
            ) {
              newPayLoad = newPayLoad.setIn(
                ["entities", "accessControlList", finalJson.id],
                Immutable.fromJS(finalJson)
              );
            }
            return newPayLoad;
          });
        }
      },
    };
  }

  // TODO: this can be trimmed down in a later patch since it's only used for the new query flow
  newGetSuccessActionTypeWithSchema(
    type,
    schema,
    meta,
    datasetVersion,
    jobId,
    paginationUrl
  ) {
    return {
      type,
      meta,
      payload: (action, state, res) => {
        const contentType = res.headers.get("Content-Type");
        if (contentType && contentType.indexOf("json") !== -1) {
          return res.json().then((pureJson) => {
            const finalJson = pureJson;

            const hash = {
              fullDataset: mapDataset,
            };

            const payload = Immutable.fromJS(
              normalize(
                (hash[schema._key] &&
                  hash[schema._key](finalJson, undefined)) ||
                  finalJson,
                schema
              )
            );
            let newPayLoad = payload.set(
              "entities",
              applyDecorators(payload.get("entities"))
            );

            if (
              payload.getIn([
                "entities",
                "accessControlList",
                finalJson.id,
                "grants",
              ]) !== undefined &&
              type === "ACCESS_CONTROL_GET_SUCCESS"
            ) {
              newPayLoad = newPayLoad.setIn(
                ["entities", "accessControlList", finalJson.id],
                Immutable.fromJS(finalJson)
              );
            }
            return newPayLoad
              .setIn(
                ["entities", "fullDataset", datasetVersion, "jobId", "id"],
                jobId
              )
              .setIn(
                ["entities", "fullDataset", datasetVersion, "paginationUrl"],
                paginationUrl
              );
          });
        }
      },
    };
  }
}

const schemaUtils = new SchemaUtils();

export default schemaUtils;
