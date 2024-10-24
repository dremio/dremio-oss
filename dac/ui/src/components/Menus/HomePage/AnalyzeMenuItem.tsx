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
import { useCallback } from "react";

import Immutable from "immutable";
import { FormattedMessage } from "react-intl";

import config from "dyn-load/utils/config";
import { getAnalyzeToolsConfig } from "#oss/utils/config";
import { fetchStatusOfAnalyzeTools } from "#oss/utils/analyzeToolsUtils";
import MenuItem from "components/Menus/MenuItem";
import SubMenu from "components/Menus/SubMenu";
import AnalyzeMenuItems from "components/Menus/AnalyzeMenuItems";

import {
  openTableau,
  openQlikSense,
  openPowerBI,
} from "actions/explore/download";
import { useDispatch } from "react-redux";
import { getVersionContextFromId } from "dremio-ui-common/utilities/datasetReference.js";
import { hideForNonDefaultBranch } from "dremio-ui-common/utilities/versionContext.js";

type AnalyzeMenuItemProps = {
  entity: Immutable.Map<string, any>;
  closeMenu: () => void;
};

const haveEnabledTools = (analyzeToolsConfig: any) => {
  const analyzeButtonsConfig = fetchStatusOfAnalyzeTools();

  return (
    analyzeButtonsConfig["client.tools.tableau"] ||
    analyzeButtonsConfig["client.tools.powerbi"] ||
    analyzeToolsConfig.qlik.enabled
  );
};

export default function AnalyzeMenuItem({
  closeMenu,
  entity,
}: AnalyzeMenuItemProps) {
  const dispatch = useDispatch();

  const versionContext = getVersionContextFromId(entity.get("id"));
  const isBIToolsEnabled = hideForNonDefaultBranch(versionContext);

  const handleTableauClick = useCallback(() => {
    dispatch(openTableau(entity));
    closeMenu();
  }, [entity, closeMenu, dispatch]);

  const handleQlikClick = useCallback(() => {
    dispatch(openQlikSense(entity));
    closeMenu();
  }, [entity, closeMenu, dispatch]);

  const handlePowerBIClick = useCallback(() => {
    dispatch(openPowerBI(entity));
    closeMenu();
  }, [entity, closeMenu, dispatch]);

  const analyzeToolsConfig = getAnalyzeToolsConfig(config);
  if (!isBIToolsEnabled || !haveEnabledTools(analyzeToolsConfig)) {
    return null;
  }

  return (
    <MenuItem
      rightIcon={
        <dremio-icon
          name="interface/triangle-right"
          alt={""}
          class={styles.rightIcon}
        />
      }
      menuItems={[
        <SubMenu key="analyze-with">
          <AnalyzeMenuItems
            openTableau={handleTableauClick}
            openPowerBI={handlePowerBIClick}
            openQlikSense={handleQlikClick}
            analyzeToolsConfig={analyzeToolsConfig}
          />
        </SubMenu>,
      ]}
      menuItemsPlacement="left-start"
    >
      <FormattedMessage id="Dataset.AnalyzeWith" />
    </MenuItem>
  );
}

const styles = {
  rightIcon: {
    width: 25,
    height: 25,
    marginRight: -10,
  },
};
