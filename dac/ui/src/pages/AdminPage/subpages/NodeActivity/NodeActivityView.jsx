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
import { PureComponent } from "react";
import PropTypes from "prop-types";
import { connect } from "react-redux";
import Immutable from "immutable";

import StatefulTableViewer from "#oss/components/StatefulTableViewer";
import NumberFormatUtils from "#oss/utils/numberFormatUtils";
import { getViewState } from "#oss/selectors/resources";
import SettingHeader from "#oss/components/SettingHeader";
import NodeTableCell from "#oss/pages/AdminPage/subpages/NodeActivity/NodeTableCell";
import NodeTableCellStatus from "#oss/pages/AdminPage/subpages/NodeActivity/NodeTableCellStatus";
import { NodeTableCellColors } from "#oss/pages/AdminPage/subpages/NodeActivity/NodeTableCell";
import NodeActivityViewMixin from "dyn-load/pages/AdminPage/subpages/NodeActivity/NodeActivityViewMixin";
import "./NodeActivity.less";
import { page, pageContent } from "uiTheme/radium/general";
import EllipsedText from "#oss/components/EllipsedText";
import CopyButton from "#oss/components/Buttons/CopyButton";
import { Tooltip } from "dremio-ui-lib";
import { intl } from "#oss/utils/intl";

export const VIEW_ID = "NodeActivityView";
export const COLUMNS_CONFIG = [
  //TODO intl
  {
    label: "",
    name: "Node Status",
    width: 40,
    minWidth: 40,
    style: { paddingLeft: "8px", paddingTop: "7px" },
    headerStyle: { paddingLeft: "8px" },
  },
  {
    label: "Node",
    width: 160,
    minWidth: 160,
  },
  {
    label: "Node Type",
    width: 160,
    minWidth: 160,
    style: { paddingLeft: "8px" },
    headerStyle: { paddingLeft: "8px" },
  },
  {
    label: "Host",
    width: 180,
    minWidth: 180,
    style: { paddingLeft: "8px" },
    headerStyle: { paddingLeft: "8px" },
    onHoverClass: "node-activity-copy",
  },
  {
    label: "Port",
    width: 100,
    minWidth: 100,
    style: { paddingLeft: "8px" },
    headerStyle: { paddingLeft: "8px" },
  },
  {
    label: "CPU",
    width: 100,
    minWidth: 100,
    style: { paddingLeft: "8px" },
    headerStyle: { paddingLeft: "8px" },
  },
  {
    label: "Memory",
    width: 140,
    minWidth: 140,
    style: { paddingLeft: "8px" },
    headerStyle: { paddingLeft: "8px" },
  },
  {
    label: "Version",
    width: 260,
    minWidth: 260,
    style: { paddingLeft: "8px" },
    headerStyle: { paddingLeft: "8px" },
  },
];
@NodeActivityViewMixin
class NodeActivityView extends PureComponent {
  static propTypes = {
    sourceNodesList: PropTypes.instanceOf(Immutable.Map),
    viewState: PropTypes.instanceOf(Immutable.Map),
  };

  state = {};

  getTableColumnsConfig() {
    return COLUMNS_CONFIG;
  }
  getTableColumns() {
    return this.getTableColumnsConfig().map((column) => ({
      key: column.label,
      ...column,
    }));
  }
  getNodeCellStatus(node) {
    if (!node.get("isCompatible")) {
      return NodeTableCellColors.RED;
    }
    return node.get("status");
  }
  getStatusTooltip(status) {
    switch (status) {
      case "green":
        return intl.formatMessage({ id: "Admin.Engines.Status.Enabled" });
      case "red":
        return intl.formatMessage({ id: "Admin.Engines.Status.Incompatible" });
      default:
        return "";
    }
  }
  getEngineStatus(node) {
    const status = this.getNodeCellStatus(node);
    let icon = "";
    switch (status) {
      case "red":
        icon = "engine-state/stopped";
        break;
      case "green":
      default:
        icon = "engine-state/running";
        break;
    }
    return <NodeTableCellStatus icon={icon} />;
  }
  getToolTipForIncompatibleNode() {
    return laDeprecated(
      "Please ensure that the version of dremio is the same on all coordinators and executors.",
    );
  }
  getNodeCell(node) {
    return <NodeTableCell name={node.get("name")} />;
  }
  getNodeTypeCell(node) {
    let type = "";
    const isCoordinator = node && node.get("isCoordinator");
    const isMaster = node && node.get("isMaster");
    const isExecutor = node && node.get("isExecutor");
    if (isCoordinator) {
      type = isMaster ? "master coordinator" : "coordinator";
    }
    if (isExecutor) {
      if (isCoordinator) {
        type += ", ";
      }
      type += "executor";
    }
    return <div className={"nodeType"}>{type}</div>;
  }

  getNodeData(columnNames, node) {
    const [nodeStatus, name, nodeType, ip, port, cpu, memory, version] =
      columnNames;

    const status = this.getNodeCellStatus(node);
    return {
      data: {
        [nodeStatus]: {
          node: () => (
            <Tooltip title={this.getStatusTooltip(status)}>
              <span>{this.getEngineStatus(node)}</span>
            </Tooltip>
          ),
        },
        [name]: {
          node: () => this.getNodeCell(node),
        },
        [nodeType]: {
          node: () => this.getNodeTypeCell(node),
        },
        [ip]: {
          node: () => {
            return (
              <div className="node-activity-ip-container">
                <EllipsedText text={node.get("ip")} style={{ flexGrow: 0 }} />
                <div className="node-activity-copy-button">
                  <CopyButton
                    title={"Copy Host"}
                    text={node.get("ip")}
                    buttonStyle={{ height: "28px", width: "28px" }}
                  />
                </div>
              </div>
            );
          },
          value: node.get("ip"),
        },
        [port]: {
          node: () => (node.get("port") !== -1 ? node.get("port") : "N/A"),
        },
        [cpu]: {
          node: () =>
            node.get("cpu") !== 0
              ? `${NumberFormatUtils.roundNumberField(node.get("cpu"))}%`
              : "N/A",
        },
        [memory]: {
          node: () =>
            node.get("memory") !== 0
              ? `${NumberFormatUtils.roundNumberField(node.get("memory"))}%`
              : "N/A", // todo: check comps for digits. and fix so no need for parseFloat
        },
        [version]: {
          node: () => node.get("version") || "-",
        },
      },
    };
  }
  getNodes() {
    return this.props.sourceNodesList.get("nodes");
  }
  getTableData() {
    // todo: styling: col alignment and spacing (esp. numbers)
    const columnNames = COLUMNS_CONFIG.map((column) => column.label);
    const nodes = this.getNodes();
    return nodes.map((node) => this.getNodeData(columnNames, node, nodes));
  }
  render() {
    const tableData = this.getTableData();
    const columns = this.getTableColumns();
    const endChildren = this.getHeaderEndChildren();
    const header = endChildren ? (
      <SettingHeader
        icon="settings/node-activity"
        title={laDeprecated("Node Activity")}
        endChildren={endChildren}
      />
    ) : (
      <SettingHeader
        icon="settings/node-activity"
        title={laDeprecated("Node Activity")}
      />
    );
    return (
      <div id="admin-nodeActivity" style={page}>
        {header}
        <div className="node-table-container">
          {this.getSubHeader()}
          <div style={pageContent}>
            <StatefulTableViewer
              tableData={tableData}
              columns={columns}
              viewState={this.props.viewState}
              rowHeight={40}
              virtualized
              scrollableTable
              fixedColumnCount={2}
              defaultSortDirection="ASC"
            />
          </div>
        </div>
      </div>
    );
  }
}
function mapStateToProps(state) {
  return {
    viewState: getViewState(state, VIEW_ID),
  };
}
export default connect(mapStateToProps)(NodeActivityView);
