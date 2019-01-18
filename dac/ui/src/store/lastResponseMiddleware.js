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
function lastResponseMiddleware() {
  return () => next => action => {
    if (localStorage.getItem('isE2E')) {
      let data;

      // table data load action
      if (action.type === 'LOAD_NEXT_ROWS_SUCCESS' && action.payload.returnedRowCount > 0) {
        data = {
          isTableData: true,
          data: action.payload
        };
      } else if (action.type && action.type !== 'GET_DATASET_ACCELERATION_SUCCESS' && action.type.endsWith('_SUCCESS') &&
          action.payload && action.payload.toJS) {
          // dataset acceleration requests can break some e2e tests so skip storing them here
        data = action.payload.toJS();
      }

      if (data) {
        global.LAST_RESPONSE = data;
      }
    }

    return next(action);
  };
}
export default lastResponseMiddleware();
