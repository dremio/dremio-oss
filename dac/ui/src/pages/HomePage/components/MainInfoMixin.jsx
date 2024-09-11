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
import LinkWithRef from "@app/components/LinkWithRef/LinkWithRef";
import { IconButton } from "dremio-ui-lib/components";
import { getSettingsLocation } from "components/Menus/HomePage/DatasetMenu";
import { addProjectBase as wrapBackendLink } from "dremio-ui-common/utilities/projectBase.js";
import { getVersionContextFromId } from "dremio-ui-common/utilities/datasetReference.js";
import { shouldUseNewDatasetNavigation } from "@app/utils/datasetNavigationUtils";
import * as sqlPaths from "dremio-ui-common/paths/sqlEditor.js";

export default function (input) {
  Object.assign(input.prototype, {
    // eslint-disable-line no-restricted-properties

    // ignore first argument
    renderConvertButton(_, folderModalButton) {
      return (
        <div className="main-settings-btn convert-to-dataset">
          <IconButton
            as={LinkWithRef}
            to={folderModalButton.to ? folderModalButton.to : "."}
            tooltip={folderModalButton.tooltip}
          >
            {folderModalButton.icon}
          </IconButton>
        </div>
      );
    },

    // versionContext is needed to create ref info for versioned views.
    // without it, trying to open a view in a new tab will fail to load.
    // same solution used for DX-62856
    getShortcutButtonsData(item, entityType, btnTypes) {
      const versionContext = getVersionContextFromId(item.get("id"));
      const { type, value } = versionContext ?? {};
      const resourceId = item.getIn(["fullPathList", 0]);
      const newFullPath = JSON.stringify(item.get("fullPathList").toJS());
      const isQueryOnClickEnabled = shouldUseNewDatasetNavigation();

      const allBtns = [
        // edit button - for views
        {
          label: this.getInlineIcon("interface/edit"),
          tooltip: "Common.Edit",
          link: wrapBackendLink(
            `${item.getIn(["links", "edit"])}${
              type && value ? `&refType=${type}&refValue=${value}` : ""
            }`,
          ),
          type: btnTypes.edit,
          isShown: isQueryOnClickEnabled && entityType === "dataset",
        },
        // dataset button - for tables (files, formatted folders, and physical datasets)
        {
          label: this.getInlineIcon("navigation-bar/go-to-dataset"),
          tooltip: "Go.To.Table",
          link: wrapBackendLink(
            `${item.getIn(["links", "query"])}${
              type && value && resourceId
                ? `?refType=${type}&refValue=${value}&sourceName=${resourceId}`
                : ""
            }`,
          ),
          type: btnTypes.goToTable,
          isShown: isQueryOnClickEnabled && entityType !== "dataset",
        },
        // query button - for tables and views when query-on-click is disabled
        {
          label: this.getInlineIcon("navigation-bar/sql-runner"),
          tooltip: "Query.Dataset",
          link: {
            pathname: sqlPaths.sqlEditor.link(),
            search: `?context="${encodeURIComponent(
              resourceId,
            )}"&queryPath=${encodeURIComponent(newFullPath)}`,
          },
          type: btnTypes.query,
          isShown: !isQueryOnClickEnabled,
        },
        {
          label: this.getInlineIcon("interface/settings"),
          tooltip: "Common.Settings",
          link: getSettingsLocation(this.context.location, item, entityType),
          type: btnTypes.settings,
          isShown: true,
        },
      ];
      return allBtns;
    },

    checkToRenderConvertFolderButton(isFileSystemFolder) {
      return isFileSystemFolder;
    },

    checkToRenderConvertFileButton() {
      return true;
    },

    shouldShowDetailsPanelIcon(item) {
      const { entity: entityProp } = this.props;
      const entity = item || entityProp;
      return !!entity;
    },
  });
}
