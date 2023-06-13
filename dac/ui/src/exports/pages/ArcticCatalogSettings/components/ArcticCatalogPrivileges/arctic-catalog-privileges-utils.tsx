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

import {
  GrantForGet,
  GrantForSet,
  InlineResponse200,
  Privilege,
} from "@app/exports/endpoints/ArcticCatalogGrants/ArcticCatalogGrants.types";

export const CATALOG_LEVEL_PRIVILEGES_TOOLTIP_IDS = {
  [Privilege.COMMIT]: "Arctic.Catalog.Privileges.CommitTooltip",
  [Privilege.CREATEBRANCH]: "Arctic.Catalog.Privileges.CreateBranchTooltip",
  [Privilege.CREATETAG]: "Arctic.Catalog.Privileges.CreateTagTooltip",
  [Privilege.MANAGEGRANTS]: "Arctic.Catalog.Privileges.ManageGrantsTooltip",
  [Privilege.READSETTINGS]: "Arctic.Catalog.Privileges.ReadSettingsTooltip",
  [Privilege.USAGE]: "Arctic.Catalog.Privileges.UsageTooltip",
  [Privilege.WRITESETTINGS]: "Arctic.Catalog.Privileges.WriteSettingsTooltip",
};

export const CATALOG_LEVEL_NAME_COL = "USERS/ROLES";

export const prepareCatalogPrivilegesRequestBody = (
  tableItems: GrantForGet[],
  deletedTableItems: GrantForGet[],
  granteeData: InlineResponse200 | null,
  formValues: any,
  cachedData: any
) => {
  const preparedResults: GrantForSet[] = [];
  [...tableItems, ...deletedTableItems].forEach((item) => {
    const updatedPrivileges: Privilege[] = [];
    granteeData?.availablePrivileges.forEach((priv: Privilege) => {
      if (formValues?.[`${item.granteeId}-${priv}`])
        updatedPrivileges.push(priv);
    });

    const unsavedItemWithNoPrivileges =
      !(item.granteeId in cachedData) && updatedPrivileges.length === 0;
    if (!unsavedItemWithNoPrivileges && updatedPrivileges.length > 0)
      preparedResults.push({
        granteeId: item.granteeId,
        privileges: updatedPrivileges,
      });
  });

  return preparedResults;
};
