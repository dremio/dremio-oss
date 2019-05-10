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

/**
 * Generates a enum from list of strings. Property names are equal to keys as well as values.
 *
 * So input ['enum_value1', 'enum_value2'] produces following enum
 * {
 *    enum_value1: 'enum_value1',
 *    enum_value2: 'enum_value2'
 * }
 * @param {string[]} list - a list of enum values
 * @returns enum
 */
export const generateEnumFromList = list => list.reduce((e, v) => {
  e[v] = v;
  return e;
}, {});
