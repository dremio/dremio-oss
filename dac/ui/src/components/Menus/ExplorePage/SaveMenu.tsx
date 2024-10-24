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

import { memo } from "react";
import { useIntl } from "react-intl";
import { Divider } from "@mui/material";

import MenuItem from "./MenuItem";
import Menu from "./Menu";
import {
  getSaveMenuItems,
  getTabSaveMenuItms,
  SaveScriptAsMenuItem,
  SaveScriptMenuItem,
  SaveViewAsMenuItem,
  SaveViewMenuItem,
} from "./SaveMenuUtil";
import { MAX_MINE_SCRIPTS_ALLOWANCE } from "#oss/components/SQLScripts/sqlScriptsUtils";

import "./SaveMenu.less";
import { useMultiTabIsEnabled } from "#oss/components/SQLScripts/useMultiTabIsEnabled";

export const DOWNLOAD_TYPES = {
  json: "JSON",
  csv: "CSV",
  parquet: "PARQUET",
};

type SaveMenuProps = {
  mustSaveDatasetAs: boolean;
  isUntitledScript: boolean;
  disableBoth: boolean;
  scriptPermissions: string[];
  isSqlEditorTab: boolean;
  numberOfMineScripts: number;
  closeMenu: () => void;
  action: (arg: string) => void;
};

function SaveMenu({
  mustSaveDatasetAs,
  isUntitledScript,
  disableBoth,
  scriptPermissions,
  isSqlEditorTab,
  numberOfMineScripts,
  closeMenu,
  action,
}: SaveMenuProps) {
  const intl = useIntl();

  const isMaxScripts = numberOfMineScripts === MAX_MINE_SCRIPTS_ALLOWANCE;

  const saveAction = (saveType: string) => {
    closeMenu();
    action(saveType);
  };

  const tabsEnabled = useMultiTabIsEnabled();

  const list = tabsEnabled
    ? getTabSaveMenuItms()
    : getSaveMenuItems({
        scriptPermissions,
        isUntitledScript,
        isSqlEditorTab,
      });

  const isDisabled = (id: string) => {
    switch (id) {
      case SaveScriptAsMenuItem.id:
        return isMaxScripts;
      case SaveScriptMenuItem.id:
        return isMaxScripts && isUntitledScript;
      case SaveViewMenuItem.id:
        return mustSaveDatasetAs || disableBoth;
      case SaveViewAsMenuItem.id:
        return disableBoth;
      default:
        return false;
    }
  };

  return (
    <Menu>
      {list.map((item, i) => {
        if (item === null) {
          return <Divider key={`${i}`} className="custom-menu-divider" />;
        }

        const showTooltip =
          (item.id === "saveScript" && isMaxScripts && isUntitledScript) ||
          (item.id === "saveScriptAs" && isMaxScripts);

        return (
          <MenuItem
            key={item.label}
            className={item.class}
            disabled={isDisabled(item.id)}
            onClick={() => saveAction(item.id)}
            showTooltip={showTooltip}
            title={
              showTooltip
                ? intl.formatMessage({ id: "Scripts.Maximum.Reached" })
                : null
            }
          >
            {intl.formatMessage({ id: item.label })}
          </MenuItem>
        );
      })}
    </Menu>
  );
}

export default memo(SaveMenu);
