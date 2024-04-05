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

import { ENTITY_TYPES } from "@app/constants/Constants";
import * as commonPaths from "dremio-ui-common/paths/common.js";
import { getSonarContext } from "dremio-ui-common/contexts/SonarContext.js";
import { rmProjectBase } from "dremio-ui-common/utilities/projectBase.js";
import { getIntlContext } from "dremio-ui-common/contexts/IntlContext.js";

class MenuUtils {
  getShowState({ disabled, columnType, availableTypes }) {
    if (availableTypes.indexOf(columnType) === -1) {
      return "none";
    } else if (!disabled) {
      return "block";
    }
    return "disabled";
  }

  showConfirmRemove({
    item,
    closeMenu,
    showConfirmationDialog,
    removeItem,
    location,
    router,
  }) {
    const { t } = getIntlContext();
    const itemName = item.get("name");

    const decodedPathname = decodeURIComponent(location?.pathname);
    const currentlyUsingSource =
      rmProjectBase(decodedPathname ?? "").split("/")[2] === itemName;
    const currentlyUsingArctic = rmProjectBase(
      decodedPathname ?? ""
    ).startsWith(`/sources/arctic/${itemName}`);

    showConfirmationDialog({
      title:
        item.get("entityType") === ENTITY_TYPES.space
          ? t("Space.Delete")
          : t("Source.Delete"),
      text: t("Delete.Confirmation", {
        name: itemName,
      }),
      confirmText: t("Common.Actions.Delete"),
      confirm: () => {
        removeItem(item);

        const projectId = getSonarContext()?.getSelectedProjectId?.();
        if (currentlyUsingSource || currentlyUsingArctic) {
          router.push(commonPaths.projectBase.link({ projectId }));
        }
      },
      confirmButtonStyle: "primary-danger",
    });
    closeMenu();
  }
}

const menuUtils = new MenuUtils();
export default menuUtils;
