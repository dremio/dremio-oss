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
import { useMemo } from "react";
import { useSelector } from "react-redux";
import localStorageUtils from "@inject/utils/storageUtils/localStorageUtils";
import { Controller } from "react-hook-form";
import { Checkbox } from "@mantine/core";
import { getIntlContext } from "dremio-ui-common/contexts/IntlContext.js";
import { renderPrivilegesColumns } from "dremio-ui-common/components/PrivilegesTable/privilegesTableColumns.js";
import { Avatar } from "dremio-ui-lib/components";
// @ts-ignore
import { Tooltip } from "dremio-ui-lib";
import { nameToInitials } from "#oss/exports/utilities/nameToInitials";
import SettingsBtn from "#oss/components/Buttons/SettingsBtn";
import Menu from "#oss/components/Menus/Menu";
import MenuItem from "#oss/components/Menus/MenuItem";
import { GranteeData } from "./PrivilegesPage";

import * as classes from "./PrivilegesPage.module.less";

export enum GrantActions {
  GRANT_OWNERSHIP = "GRANT_OWNERSHIP",
  DELETE = "DELETE",
}
export type GrantActionsType =
  | GrantActions.DELETE
  | GrantActions.GRANT_OWNERSHIP;

export const getPrivilegesColumns = (
  granteeData: GranteeData,
  openDialog: (type: any, dialogState: any) => void,
  nameColumnLabel: string,
  privilegeTooltipIds: any,
  owner?: string,
) => {
  const { t } = getIntlContext();
  return renderPrivilegesColumns({
    nameColumnLabel: nameColumnLabel,
    availablePrivileges: granteeData?.availablePrivileges,
    renderPrivilegeTooltip: (privilege: string) =>
      t(privilegeTooltipIds[privilege]),
    renderNameCell: (data: any) => nameCell(data, openDialog, owner),
    renderCheckboxCell: (data: any, privilege: string) => (
      <Controller
        name={`${data.granteeId}-${privilege}`}
        render={({ field, fieldState, formState }) => {
          const isDirty = fieldState.isDirty;
          return (
            <label className={classes["privileges__label"]}>
              <div className={classes["privileges__tableCell"]}>
                <Checkbox
                  {...field}
                  checked={field.value}
                  key={`${data.granteeId}-${privilege}`}
                  size="xs"
                  radius="xs"
                  disabled={formState.isSubmitting}
                />
                {isDirty && (
                  <div
                    className={classes["privileges__tableCell__dirtyIndicator"]}
                  />
                )}
              </div>
            </label>
          );
        }}
      />
    ),
  });
};

const GranteeActionMenu = (menuProps: any) => {
  const { item, closeMenu, openDialog } = menuProps;
  const { t } = getIntlContext();
  return (
    <Menu>
      <MenuItem
        key="remove-member"
        className={classes["privileges__nameCell__moreActions-remove"]}
        onClick={(): void => {
          openDialog({
            openDialog: true,
            granteeId: item.granteeId,
          });
          closeMenu();
        }}
      >
        {t("Common.Remove")}
      </MenuItem>
    </Menu>
  );
};

const nameCell = (
  item: any,
  openDialog: (type: any, dialogState: any) => void,
  owner?: string,
) => {
  const isOwner = item.granteeId === owner;
  const { t } = getIntlContext();
  return (
    <div className={classes["privileges__nameCell"]}>
      <div className={classes["privileges__nameCell__leftSide"]}>
        {item.granteeType === "USER" ? (
          <Avatar
            initials={nameToInitials(item.name)}
            className={classes["privileges__nameCell__leftIcon"]}
          />
        ) : (
          <dremio-icon
            name="interface/member-role"
            class={classes["privileges__nameCell__leftIcon"]}
            alt="member-icon"
          />
        )}
        <Tooltip title={item.name}>
          <span className={classes["privileges__nameCell__name"]}>
            {item.name}
          </span>
        </Tooltip>
      </div>
      <div className={classes["privileges__nameCell__rightSide"]}>
        <SettingsBtn
          classStr={`sqlScripts__menu-item__actions ${classes["privileges__nameCell__moreActions"]}`}
          menu={<GranteeActionMenu item={item} openDialog={openDialog} />}
          hideArrowIcon
          stopPropagation
          disabled={isOwner}
        >
          <dremio-icon
            name="interface/more"
            alt={t("Common.More")}
            class={
              classes[
                `privileges__nameCell__moreActions${isOwner ? "-disabled" : ""}`
              ]
            }
          />
        </SettingsBtn>
      </div>
    </div>
  );
};

export const getCustomChipIcon = (item: { type: string; name: string }) => {
  if (item.type === "USER") {
    return (
      <Avatar
        initials={nameToInitials(item.name)}
        className={classes["privileges__nameCell__leftIcon__menuItem"]}
      />
    );
  } else return;
};

export const useCanSearchRolesAndUsers: () => boolean[] = () => {
  const orgPrivileges = useSelector(
    (state: Record<string, any>) => state.privileges?.organization,
  );
  // const userPermissions = localStorageUtils?.getUserPermissions();
  const {
    canSearchUser,
    canSearchRole,
  }: { canSearchUser: boolean; canSearchRole: boolean } = useMemo(() => {
    return {
      canSearchUser: orgPrivileges?.users?.canView, // || userPermissions?.canCreateUser
      canSearchRole: orgPrivileges?.roles?.canView, // || userPermissions?.canCreateRole,
    };
  }, [orgPrivileges]);
  return [canSearchUser, canSearchRole];
};
