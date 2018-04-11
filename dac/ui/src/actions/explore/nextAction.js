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
import { toggleExploreSql } from 'actions/explore/ui';
import { openTableau, openQlikSense, openPowerBI } from 'actions/explore/download';

export const NEXT_ACTIONS = {
  toggleSql: 'TOGGLE_SQL',
  openTableau: 'OPEN_TABLEAU',
  openQlik: 'OPEN_QLIK_AFTER',
  openPowerBI: 'OPEN_POWER_BI'
};

export const performNextAction = (dataset, nextAction) => {
  switch (nextAction) {
  case NEXT_ACTIONS.toggleSql:
    return toggleExploreSql();
  case NEXT_ACTIONS.openTableau:
    return openTableau(dataset);
  case NEXT_ACTIONS.openQlik:
    return openQlikSense(dataset);
  case NEXT_ACTIONS.openPowerBI:
    return openPowerBI();
  default:
    console.error('Unknown next action', nextAction);
    return {type: 'NOP'};
  }
};
