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
import Immutable from 'immutable';
import { NODE_COLUMNS_CONFIG } from '@app/constants/provisioningPage/provisioningConstants';
import { makeContainerPropertyRow } from '@app/pages/AdminPage/subpages/Provisioning/provisioningUtils';
import { EngineStatusBar } from '@inject/pages/AdminPage/subpages/Provisioning/components/EngineStatusBar';

export default function(input) {
  Object.assign(input.prototype, { // eslint-disable-line no-restricted-properties
    getColumns() {
      return NODE_COLUMNS_CONFIG;
    },

    getTableData(engine) {
      if (!engine) return new Immutable.List();
      const getRows = (listKey, status) => {
        const list = engine.getIn(['containers', listKey]);
        if (!list || !list.size) return new Immutable.List();

        return list.map(item => {
          const itemObj = makeContainerPropertyRow(item);
          itemObj.status = {node: () => status};
          return {data: itemObj};
        });
      };
      // status is spelled out in the table column
      const runningRows = getRows('runningList', 'Running');
      const disconnectedRows = getRows('disconnectedList', 'Provisioning or Disconnected');
      return disconnectedRows.concat(runningRows);
    },

    renderEngineStatusBar(engine) {
      return <EngineStatusBar engine={engine}/>;
    }
  });
}
