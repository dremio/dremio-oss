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
import PropTypes from "prop-types";
import Art from "@app/components/Art";
import { CLIENT_TOOL_ID } from "@app/constants/Constants";
import Immutable from "immutable";

export const RESERVED = [
  CLIENT_TOOL_ID.powerbi,
  CLIENT_TOOL_ID.tableau,
  CLIENT_TOOL_ID.qlik,
];

const AnalyzeTools = (props) => {
  const renderTool = (id, name, icon) => {
    return (
      <div style={styles.toolWrap}>
        <div style={styles.iconCell}>
          <Art src={icon} alt={name} style={styles.icon} />
        </div>
        <div style={styles.toolName}>{name}</div>
        <div style={styles.actionCell}>
          {props.renderSettings(id, { showLabel: false })}
        </div>
      </div>
    );
  };

  const qlikEnabled = props.settings.get(CLIENT_TOOL_ID.qlikEnabled);

  return (
    <div
      style={{
        padding: "10px 0 20px",
        borderBottom: "1px solid hsla(0, 0%, 0%, 0.1)",
      }}
    >
      <h3>{la("Client Tools")}</h3>
      <div style={props.descriptionStyle}>
        {la("Note: Users will see changes when they next reload.")}
      </div>
      <div style={styles.toolsTable}>
        <div style={styles.tableRightHeader}>{la("Enabled")}</div>
        {renderTool(CLIENT_TOOL_ID.tableau, "Tableau:", "Tableau.svg")}
        {renderTool(CLIENT_TOOL_ID.powerbi, "Power BI:", "PowerBi.svg")}
        {qlikEnabled &&
          qlikEnabled.get("value") &&
          renderTool(CLIENT_TOOL_ID.qlik, "Qlik:", "Qlik.svg")}
      </div>
    </div>
  );
};

AnalyzeTools.propTypes = {
  descriptionStyle: PropTypes.object,
  renderSettings: PropTypes.func,
  settings: PropTypes.instanceOf(Immutable.Map).isRequired,
};

export default AnalyzeTools;

const styles = {
  toolWrap: {
    display: "flex",
  },
  toolsTable: {
    width: 400,
  },
  tableRightHeader: {
    fontSize: "12px",
    fontWeight: "500",
    textAlign: "right",
    paddingRight: 80,
  },
  iconCell: {
    padding: "5px 10px",
  },
  icon: {
    width: 24,
    height: 24,
  },
  toolName: {
    fontSize: "12px",
    fontWeight: "500",
    padding: "10px 0",
  },
  actionCell: {
    marginLeft: "auto",
  },
};
