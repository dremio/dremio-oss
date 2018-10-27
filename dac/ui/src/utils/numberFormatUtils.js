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

  static makeMemoryValueString = (value) => {
    let unit = 'MB';
    let num = 0;
    // the logic of selecting unit and value is preserved to match one in ByteField / MultiplierField component
    // it uses lower units in cases where there would be a long decimal
    for (const [key, multiplier] of MEMORY_UNITS) {
      if (value < multiplier) break;
      if (NumberFormatUtils.stringifyWithoutExponent(value / multiplier).match(/\.[0-9]{3}/)) break;
      unit = key;
      num = +(value / multiplier).toFixed(2);
    }
    return `${num} ${unit}`;
  };

  static stringifyWithoutExponent = (number) => {
    return number.toFixed(20).replace(/\.?0+$/, '');
  };

}
