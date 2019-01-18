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

export const MEMORY_UNITS = new Map([ // todo: loc
// ['B', 1024 ** 0],
  ['KB', 1024 ** 1],
  ['MB', 1024 ** 2],
  ['GB', 1024 ** 3],
  ['TB', 1024 ** 4]
]);


export default class NumberFormatUtils {

  static roundNumberField(value, precision = 2) {
    return Number(parseFloat(value).toFixed(precision)).toString();
  }

  /**
   * Memory units policy standards are a mess. See https://wiki.ubuntu.com/UnitsPolicy
   * This solution follows discussion at
   *   https://stackoverflow.com/questions/10420352/converting-file-size-in-bytes-to-human-readable-string
   * The result shows with up to 2 decimals after point using binary notation (IEC).
   * @param bytes
   * @return {string}
   */
  static makeMemoryValueString = function(bytes) {
    const i = bytes === 0 ? 0 : Math.floor( Math.log(bytes) / Math.log(1024) );
    return (bytes / Math.pow(1024, i)).toFixed(2).replace(/\.?0+$/, '') + ' ' + ['B', 'kB', 'MB', 'GB', 'TB'][i];
  };

}
