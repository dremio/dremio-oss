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
import { isYarn } from "@app/pages/AdminPage/subpages/Provisioning/provisioningUtils";
import { EngineActionCell } from "@app/pages/AdminPage/subpages/Provisioning/components/EngineActionCell";
import EngineStatus from "@app/pages/AdminPage/subpages/Provisioning/components/EngineStatus";
import { ClickableCell } from "dremio-ui-common/components/TableCells/ClickableCell.js";
import { SortableHeaderCell } from "dremio-ui-common/components/TableCells/SortableHeaderCell.js";
import { intl } from "@app/utils/intl";

export default function (input) {

  const originalGetEngineSize = input.prototype.getEngineSize;
  const originalGetRunningNodes = input.prototype.getRunningNodes;
  const originalGetEngineName = input.prototype.getEngineName;
  const originalCPUCores = input.prototype.getClusterCPUCores;
  const originalClusterRAM = input.prototype.getClusterRAM;
  const originalClusterIP = input.prototype.getClusterIp;

  Object.assign(input.prototype, {
    // eslint-disable-line no-restricted-properties

    getClusterCPUCores(engine) {
      return originalCPUCores.call(this, engine)
    },

    getClusterRAM(engine) {
      return originalClusterRAM.call(this, engine)
    },

    getClusterIp(engine) {
      return originalClusterIP.call(this, engine)
    },

    getEngineColumnConfig(statusViewState, onRowClick, sort) {
      return [
        {
          id: "status",
          class: "leantable-sticky-column leantable-sticky-column--left",
          renderHeaderCell: () => "",
          renderCell: (row) => {
            return <EngineStatus engine={row.data} viewState={statusViewState} />;
          },
        },
        {
          id: "engine",
          renderHeaderCell: () => <SortableHeaderCell columnId="engines">{intl.formatMessage({ id: "Engine.EngineName" })}</SortableHeaderCell>,
          renderCell: (row) => {
            return (
              <ClickableCell style={{ marginTop: '20px' }} onClick={() => onRowClick(row.id)}>
                {originalGetEngineName.call(this, row.data)}
              </ClickableCell>
            )
          },
          sortable: true
        },
        {
          id: "size",
          renderHeaderCell: () =>  <SortableHeaderCell columnId="size">{intl.formatMessage({ id: "Engine.Size" })}</SortableHeaderCell>,
          renderCell: (row) => {
            return originalGetEngineSize.call(this, row.data);
          },
          sortable: true
        },
        {
          id: "cores",
          renderHeaderCell: () => <SortableHeaderCell columnId="cores">{intl.formatMessage({ id: "Engine.Cores" })}</SortableHeaderCell>,
          renderCell: (row) => {
            return this.getClusterCPUCores(row.data);
          },
          sortable: true
        },
        {
          id: "memory",
          renderHeaderCell: () => <SortableHeaderCell columnId="memory">{intl.formatMessage({ id: "Engine.Memory" })}</SortableHeaderCell>,
          renderCell: (row) => {
            return this.getClusterRAM(row.data);
          },
          sortable: true
        },
        {
          id: "ip",
          renderHeaderCell: () => <SortableHeaderCell columnId="ip">{intl.formatMessage({ id: "Engine.IP" })}</SortableHeaderCell>,
          renderCell: (row) => {
            return this.getClusterIp(row.data);
          },
          sortable: true
        },
        {
          id: "nodes",
          renderHeaderCell: () => <SortableHeaderCell columnId="nodes">{intl.formatMessage({ id: "Engine.OnlineNodes" })}</SortableHeaderCell>,
          renderCell: (row) => {
            return originalGetRunningNodes.call(this, row.data);
          },
          sortable: true
        },
        {
          id: "action",
          class:
            "leantable-row-hover-visibility leantable-sticky-column leantable-sticky-column--right",
          renderHeaderCell: () => "",
          renderCell: (row) => {
            return this.getAction(row.data);
          },
        },
      ]
    },

    getTableColumns({statusViewState: statusViewState, onRowClick: onRowClick, sort}) {
      return this.getEngineColumnConfig(statusViewState, onRowClick, sort);
    },

    getAction(entity) {
      const { editProvision, removeProvision } = this.props;
      const usingWorkers = isYarn(entity);
      return (
        <EngineActionCell
          engine={entity}
          usingWorkers={usingWorkers}
          editProvision={editProvision}
          removeProvision={removeProvision}
          handleAddRemove={this.handleAddRemove}
          handleStartStop={this.handleStartStop}
          className="provisioning__actions__float"
        />
      );
    },
  });
}
