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
import { withRouter, WithRouterProps } from "react-router";
import { useIntl } from "react-intl";
import { Divider } from "@material-ui/core";
import PropTypes from "prop-types";
//@ts-ignore
import config from "dyn-load/utils/config";

/* @ts-ignore */
import { SHOW_ADD_FOLDER } from "@inject/pages/HomePage/components/HeaderButtonConstants";
/* @ts-ignore */
import { SHOW_ADD_FILE } from "@inject/pages/HomePage/components/HeaderButtonConstants";
/* @ts-ignore */
import { HANDLE_THROUGH_API } from "@inject/pages/HomePage/components/HeaderButtonConstants";
import MenuItem from "@app/components/Menus/MenuItem";
import { parseResourceId } from "@app/utils/pathUtils";

import * as classes from "./HeaderButtonAddActions.module.less";

type HeaderButtonAddActionsProps = {
  allowFileUpload: boolean;
  allowTable: boolean;
  canUploadFile: boolean;
};
const HeaderButtonAddActions = (
  props: HeaderButtonAddActionsProps & WithRouterProps,
  context: { username: string }
) => {
  const { allowFileUpload, allowTable, canUploadFile, location, router } =
    props;

  const intl = useIntl();

  // TODO: not necessary for software since canUploadFile includes ui.upload.allow
  // leaving here for now to not affect DCS
  let displayUploadFile = SHOW_ADD_FILE && config.allowFileUploads;
  if (HANDLE_THROUGH_API) {
    const storageData = localStorage && localStorage.getItem("supportFlags");
    const supportFlags = storageData ? JSON.parse(storageData) : null;
    if (supportFlags && supportFlags["ui.upload.allow"]) {
      displayUploadFile = true;
    }
  }
  const newQueryUrl = "/new_query";
  const resourceId = parseResourceId(location.pathname, context.username);
  const newQueryUrlParams = "?context=" + encodeURIComponent(resourceId);

  const ACTIONS = [
    ...(allowFileUpload && (canUploadFile || displayUploadFile)
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
    {
      name: "Common.NewView",
      key: "view",
      icon: "interface/add-view",
      to: {
        pathname: newQueryUrl,
        search: newQueryUrlParams,
        state: {
          createView: true,
        },
      },
      tooltip: "Common.NewView",
      qa: "new-view",
      iconClass: "",
    },
    ...(allowTable
      ? [
          {
            name: "Common.NewTable",
            key: "table",
            icon: "interface/add-dataset",
            to: {
              pathname: newQueryUrl,
              search: newQueryUrlParams,
              state: {
                createTable: true,
              },
            },
            tooltip: "Common.NewTable",
            qa: "new-table",
            iconClass: "",
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
              classname="full-width gutter--none"
              onClick={() => router.push(to)}
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

HeaderButtonAddActions.contextTypes = { username: PropTypes.string };

export default withRouter(HeaderButtonAddActions);
