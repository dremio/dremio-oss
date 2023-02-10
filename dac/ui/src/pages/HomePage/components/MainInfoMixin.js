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
import { IconButton } from "dremio-ui-lib";
import { getSettingsLocation } from "components/Menus/HomePage/DatasetMenu";
import { addProjectBase as wrapBackendLink } from "dremio-ui-common/utilities/projectBase.js";

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

    getShortcutButtonsData(item, entityType, btnTypes) {
      const allBtns = [
        // Per DX-13304 we leave only Edit and Cog (Settings.svg) buttons
        {
          label: this.getInlineIcon("interface/edit"),
          tooltip: "Common.Edit",
          link: wrapBackendLink(item.getIn(["links", "edit"])),
          type: btnTypes.edit,
          isShown: entityType === "dataset",
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
  });
}
