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

function readNewTmpUntitledResponse(type: any, meta: any) {
  return {
    type,
    meta,
    payload: (_action: any, _state: any, res: any) => {
      const contentType = res.headers.get("Content-Type");

      if (~contentType?.indexOf("json")) {
        return res
          .json()
          .then((json: any) => {
            return json;
          })
          .catch((e: any) => {
            console.error("error: ", e);
          });
      }
    },
  };
}

export default readNewTmpUntitledResponse;
