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
import Radium from "radium";
import { injectIntl } from "react-intl";

import PropTypes from "prop-types";

import { Divider } from "@material-ui/core";
import MenuItem from "./MenuItem";
import Menu from "./Menu";
import { getSaveMenuItems } from "./SaveMenuUtil";
import { MAX_MINE_SCRIPTS_ALLOWANCE } from "@app/components/SQLScripts/sqlScriptsUtils";

import "./SaveMenu.less";

export const DOWNLOAD_TYPES = {
  json: "JSON",
  csv: "CSV",
  parquet: "PARQUET",
};

class SaveMenu extends PureComponent {
  static propTypes = {
    action: PropTypes.func,
    closeMenu: PropTypes.func,
    mustSaveDatasetAs: PropTypes.bool,
    mustSaveViewAs: PropTypes.bool,
    scriptPermissions: PropTypes.array,
    disableBoth: PropTypes.bool,
    isSqlEditorTab: PropTypes.bool,
    isUntitledScript: PropTypes.bool,
    intl: PropTypes.object,
    numberOfMineScripts: PropTypes.number,
  };

  saveAction = (saveType) => {
    this.props.closeMenu();
    this.props.action(saveType);
  };

  renderMenuItems = () => {
    const {
      mustSaveDatasetAs,
      isUntitledScript,
      disableBoth,
      scriptPermissions,
      isSqlEditorTab,
      intl,
      numberOfMineScripts,
    } = this.props;

    const saveMenuItems = [
      {
        label: "NewQuery.SaveScriptAs",
        handleSave: () => this.saveAction("saveScriptAs"),
        id: "saveScriptAs",
        disabled: numberOfMineScripts === MAX_MINE_SCRIPTS_ALLOWANCE || false,
        class: "save-as-menu-item",
      },
      {
        label: "NewQuery.SaveScript",
        handleSave: () => this.saveAction("saveScript"),
        disabled:
          (numberOfMineScripts === MAX_MINE_SCRIPTS_ALLOWANCE &&
            isUntitledScript) ||
          false,
        id: "saveScript",
        class: "save-menu-item",
      },
      {
        label: "NewQuery.SaveView",
        handleSave: () => this.saveAction("saveView"),
        disabled: mustSaveDatasetAs || disableBoth,
        id: "saveView",
        class: "save-menu-item",
      },
      {
        label: "NewQuery.SaveViewAs",
        handleSave: () => this.saveAction("saveViewAs"),
        id: "saveViewAs",
        disabled: disableBoth,
        class: "save-as-menu-item",
      },
    ];

    const list = getSaveMenuItems({
      saveMenuItems,
      permissionsFromScript: scriptPermissions,
      isUntitledScript,
      isSqlEditorTab,
    });

    return list.map((item, i) => {
      const showTooltip =
        (item &&
          item.id === "saveScript" &&
          numberOfMineScripts === MAX_MINE_SCRIPTS_ALLOWANCE &&
          isUntitledScript) ||
        (item &&
          item.id === "saveScriptAs" &&
          numberOfMineScripts === MAX_MINE_SCRIPTS_ALLOWANCE);

      return item && item.label ? (
        <MenuItem
          key={item.label}
          className={item.class}
          disabled={item.disabled}
          onClick={item.handleSave}
          showTooltip={showTooltip}
          title={
            showTooltip
              ? intl.formatMessage({ id: "Scripts.Maximum.Reached" })
              : null
          }
        >
          {intl.formatMessage({ id: item.label })}
        </MenuItem>
      ) : (
        <Divider key={`${i}`} className="custom-menu-divider" />
      );
    });
  };

  render() {
    return <Menu>{this.renderMenuItems()}</Menu>;
  }
}
export default injectIntl(Radium(SaveMenu));
