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
class ObjectUtils {
  contains(arr, elem) {
    return arr.indexOf(elem) !== -1;
  }

  getUniqArray(arr, property) {
    const uniqArr = [];
    arr.forEach((data) => {
      if (property) {
        if (uniqArr.indexOf(data[property]) === -1) {
          uniqArr.push(data[property]);
        }
      } else if (uniqArr.indexOf(data) === -1) {
        uniqArr.push(data);
      }
    });

    return uniqArr;
  }

  merge() {
    const res = {};
    for (let i = 0; i < arguments.length; i++) {
      for (const key in arguments[i]) {
        res[key] = arguments[i][key];
      }
    }
    return res;
  }
}

const objectUtils = new ObjectUtils();

export default objectUtils;
