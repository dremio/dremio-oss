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
import { ENGINE_COLUMNS_CONFIG } from '@app/constants/provisioningPage/provisioningConstants';
import {isYarn } from '@app/pages/AdminPage/subpages/Provisioning/provisioningUtils';
import { EngineActionCell } from '@app/pages/AdminPage/subpages/Provisioning/components/EngineActionCell';

export default function(input) {
  Object.assign(input.prototype, { // eslint-disable-line no-restricted-properties
    getTableColumns() {
      return ENGINE_COLUMNS_CONFIG;
    },

    getAction(entity)  {
      const {editProvision, removeProvision} = this.props;
      const usingWorkers = isYarn(entity);
      return <EngineActionCell
        engine={entity}
        usingWorkers={usingWorkers}
        editProvision={editProvision}
        removeProvision={removeProvision}
        handleAddRemove={this.handleAddRemove}
        handleStartStop={this.handleStartStop}
      />;
    }
  });
}
