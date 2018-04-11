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

import { mapDataset } from 'apiMappers/datasetMapper';
import { applyDecorators } from 'utils/decorators';

class SchemaUtils {
  getSuccessActionTypeWithSchema(type, schema, meta, key, value) {
    return {
      type,
      meta,
      payload: (action, state, res) => {
        const contentType = res.headers.get('Content-Type');
        if (contentType && contentType.indexOf('json') !== -1) {
          return res.json().then((pureJson) => {
            if (typeof key === 'object') {
              key.forEach(item => {
                pureJson[item.key] = item.value;
              });
            } else if (key && value) {
              pureJson[key] = value; //when no id from api, we generate it
            }
            const hash = {
              fullDataset: mapDataset
            };
            return applyDecorators(
              Immutable.fromJS(normalize(hash[schema._key] && hash[schema._key](pureJson, key) || pureJson, schema))
            );
          });
        }
      }
    };
  }
}

const schemaUtils = new SchemaUtils();

export default schemaUtils;
