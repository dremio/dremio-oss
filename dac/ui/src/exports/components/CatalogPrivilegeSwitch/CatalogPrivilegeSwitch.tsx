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

import { useResourceSnapshot, useResourceStatus } from "smart-resource/react";
import { ArcticCatalogPrivilegesResource } from "@inject/arctic/resources/ArcticCatalogPrivilegesResource";
import { SmartResource } from "smart-resource";
import { isNotSoftware } from "dyn-load/utils/versionUtils";
type Props = {
  privilege:
    | "canView"
    | "canManage"
    | "canDelete"
    | ["branch", "canCreate"]
    | ["tag", "canCreate"]
    | ["optimization", "canEdit"];
  renderEnabled: () => JSX.Element | void;
  renderDisabled?: () => JSX.Element;
  renderPending?: () => JSX.Element;
};

const nullRender = () => null;

export const CatalogPrivilegeSwitch = (props: Props): JSX.Element | null => {
  const {
    privilege,
    renderEnabled,
    renderPending = nullRender,
    renderDisabled = nullRender,
  } = props;
  if (!isNotSoftware?.()) {
    return renderEnabled() || null;
  }
  const [result] = useResourceSnapshot(
    ArcticCatalogPrivilegesResource || new SmartResource(() => null)
  );
  const status = useResourceStatus(
    ArcticCatalogPrivilegesResource || new SmartResource(() => null)
  );

  if (status === "initial" || status === "pending") {
    return renderPending();
  }
  if (!result) {
    return renderDisabled();
  }

  if (typeof privilege === "string") {
    if (!result[privilege]) {
      return renderDisabled();
    }
  } else {
    //@ts-ignore
    if (!result[privilege[0]][privilege[1]]) {
      return renderDisabled();
    }
  }

  return renderEnabled() || null;
};
