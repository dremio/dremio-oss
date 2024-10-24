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
import { browserHistory } from "react-router";
import { useIntl } from "react-intl";
import { Divider } from "@mui/material";
import config from "dyn-load/utils/config";
import { SHOW_ADD_FOLDER } from "@inject/pages/HomePage/components/HeaderButtonConstants";
import { SHOW_ADD_FILE } from "@inject/pages/HomePage/components/HeaderButtonConstants";
import { HANDLE_THROUGH_API } from "@inject/pages/HomePage/components/HeaderButtonConstants";
import MenuItem from "#oss/components/Menus/MenuItem";

import * as classes from "./HeaderButtonAddActions.module.less";

type HeaderButtonAddActionsProps = {
  canUploadFile: boolean;
};
const HeaderButtonAddActions = (props: HeaderButtonAddActionsProps) => {
  const { canUploadFile } = props;

  const intl = useIntl();
  const location = browserHistory.getCurrentLocation();

  let displayUploadFile = SHOW_ADD_FILE && config.allowFileUploads;
  if (HANDLE_THROUGH_API) {
    const storageData = localStorage && localStorage.getItem("supportFlags");
    const supportFlags = storageData ? JSON.parse(storageData) : null;
    if (supportFlags && supportFlags["ui.upload.allow"]) {
      displayUploadFile = true;
    }
  }

  const ACTIONS = [
    ...(canUploadFile || displayUploadFile
      ? [
          {
            name: "File.Upload",
            key: "upload",
            icon: "interface/upload",
            to: { ...location, state: { modal: "AddFileModal" } },
            tooltip: "File.Upload",
            qa: "add-file",
            iconClass: "headerButtons__uploadIcon",
            hasDivider: true,
          },
        ]
      : []),
    ...(SHOW_ADD_FOLDER
      ? [
          {
            name: "Common.NewFolder",
            key: "folder",
            icon: "interface/add-folder",
            to: { ...location, state: { modal: "AddFolderModal" } },
            tooltip: "Common.NewFolder",
            qa: "new-folder",
            iconClass: "",
          },
        ]
      : []),
  ];

  return (
    <div className={classes["headerButtons__addActions"]}>
      {ACTIONS.map((action, index) => {
        const { name, to, key, icon, iconClass, hasDivider } = action;
        const actionName = intl.formatMessage({ id: name });
        return (
          <div key={key}>
            <MenuItem
              className="full-width gutter--none"
              onClick={() => browserHistory.push(to)}
            >
              <div className={classes["headerButtons__addActions__content"]}>
                <dremio-icon
                  name={icon}
                  class={`${classes[iconClass]} icon --md margin-left--double`}
                />
                <span className={classes["headerButtons__spanGap"]}>
                  {actionName}
                </span>
              </div>
            </MenuItem>
            {hasDivider && (
              <Divider
                key={`${index}`}
                className={classes["custom-menu-divider"]}
              />
            )}
          </div>
        );
      })}
    </div>
  );
};

export default HeaderButtonAddActions;
