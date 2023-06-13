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
import { abilities } from "utils/datasetUtils";
import datasetSettingsConfig from "@inject/pages/HomePage/components/modals/DatasetSettings/datasetSettingsConfig";
import { isVersionedSource } from "@app/utils/sourceUtils";
import { NESSIE } from "@app/constants/sourceTypes";
import { REFLECTION_ARCTIC_ENABLED } from "@app/exports/endpoints/SupportFlags/supportFlagConstants";

export default function (input) {
  Object.assign(input.prototype, {
    // eslint-disable-line no-restricted-properties
    extendContentRenderers(contentRenderers) {
      return contentRenderers;
    },

    getTabs() {
      const { entity, intl, source, isAdmin, enableCompaction, supportFlags } =
        this.props;

      if (!entity) {
        return new Immutable.OrderedMap();
      }

      const map = [];

      const { canEditFormat, canSetAccelerationUpdates } = abilities(
        entity,
        entity.get("entityType")
      );

      const arcticReflectionsEnabled =
        supportFlags?.[REFLECTION_ARCTIC_ENABLED];
      const { showFormatTab } = datasetSettingsConfig;
      const format = showFormatTab &&
        canEditFormat && ["format", intl.formatMessage({ id: "File.Format" })];

      // If a file or folder has not been converted to a dataset, hide all other tabs
      // DX-3178
      if (canEditFormat && !entity.get("queryable")) {
        map.push(format);
        return new Immutable.OrderedMap(map);
      }

      const isVersioned = isVersionedSource(source?.type);
      const showAccelerationTabs =
        !isVersioned || (isVersioned && arcticReflectionsEnabled);
      map.push(
        ["overview", intl.formatMessage({ id: "Common.Overview" })],
        format,
        showAccelerationTabs && [
          "acceleration",
          intl.formatMessage({ id: "Reflection.Reflections" }),
        ],
        showAccelerationTabs &&
          canSetAccelerationUpdates && [
            "accelerationUpdates",
            intl.formatMessage({ id: "Acceleration.RefreshPolicy" }),
          ],
        enableCompaction &&
          isAdmin &&
          source?.type === NESSIE &&
          entity.get("entityType") === "physicalDataset" && [
            "dataOptimization",
            intl.formatMessage({ id: "Data.Optimization" }),
          ]
      );

      return new Immutable.OrderedMap(map);
    },
  });
}
