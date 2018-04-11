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
// TODO: loc
export default {
  DATE: {
    values: ['YYYY-MM-DD', 'MM.DD.YY', 'DD/MM/YY', 'MON DD, YYYY'],
    examples: [
      {format: 'YYYY', description: 'Four digits of year'},
      {format: 'YY', description: 'Last two digits of year'},
      {format: 'MM', description: 'Month (1-12)'},
      {format: 'MON', description: 'Abbreviated month name (Mar, Oct)'},
      {format: 'MONTH', description: 'Full month name (March, October)'},
      {format: 'DD', description: 'Day of month (1-31)'}
    ]
  },

  TIME: {
    values: ['HH:MI', 'HH24:MI', 'HH24:MI:SS', 'HH24:MI:SS.FFF'],
    examples: [
      {format: 'HH', description: 'Hour of day (1-12)'},
      {format: 'HH24', description: 'Hour of day (0-23)'},
      {format: 'MI', description: 'Minutes (0-59)'},
      {format: 'SS', description: 'Seconds (0-59)'},
      {format: 'FFF', description: 'Milliseconds (0-999)'}
    ]
  },

  DATETIME: {
    values: ['YYYY-MM-DD HH24:MI:SS', 'YYYY-MM-DD HH24:MI:SS.FFF', 'YYYY-MM-DD"T"HH24:MI:SS.FFFTZO'],
    examples: [
      {format: 'YYYY', description: 'Four digits of year'},
      {format: 'MM', description: 'Month (1-12)'},
      {format: 'DD', description: 'Day of month (1-31)'},
      {format: 'HH24', description: 'Hour of day (0-23)'},
      {format: 'MI', description: 'Minutes (0-59)'},
      {format: 'SS', description: 'Seconds (0-59)'}
    ]
  }
};
