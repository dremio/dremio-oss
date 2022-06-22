import { ENTITY_TYPES } from "@app/constants/Constants";
import { manageSourceRule, manageSpaceRule } from "@app/utils/authUtils";
import { isCME } from "dyn-load/utils/versionUtils";
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
export default function (input) {
  Object.assign(input.prototype, {
    // eslint-disable-line no-restricted-properties
    getSpaceSettingsButtons() {
      const { location } = this.context;
      const { entity } = this.props;
      const buttons = [];

      if (entity.get("entityType") === ENTITY_TYPES.space) {
        let showSettingsButton = true;
        if (isCME && !isCME() && entity.get("permissions")) {
          showSettingsButton = entity.getIn([
            "permissions",
            "canEditAccessControlList",
          ]);
        }
        if (showSettingsButton) {
          buttons.push({
            qa: "settings",
            iconType: "interface/settings",
            to: {
              ...location,
              state: { modal: "SpaceModal", entityId: entity.get("id") },
            },
            authRule: manageSpaceRule,
          });
        }
      }

      return buttons;
    },

    getSourceSettingsButtons() {
      const { location } = this.context;
      const { entity } = this.props;
      const buttons = [];

      if (entity.get("entityType") === "source") {
        let showSettingsButton = true;
        if (isCME && !isCME() && entity.get("permissions")) {
          showSettingsButton = entity.getIn([
            "permissions",
            "canEditAccessControlList",
          ]);
        }
        if (showSettingsButton) {
          buttons.push({
            qa: "settings",
            iconType: "interface/settings",
            to: {
              ...location,
              state: {
                modal: "EditSourceModal",
                query: { name: entity.get("name"), type: entity.get("type") },
              },
            },
            authRule: manageSourceRule,
          });
        }
      }

      return buttons;
    },
  });
}
