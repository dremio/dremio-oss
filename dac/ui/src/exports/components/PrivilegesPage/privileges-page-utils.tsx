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

import { Checkbox } from "@mantine/core";
// @ts-ignore
import { renderPrivilegesColumns } from "dremio-ui-common/components/PrivilegesTable/privilegesTableColumns.js";
import { Controller } from "react-hook-form";
import { Avatar } from "dremio-ui-lib/components";
// @ts-ignore
import { Tooltip } from "dremio-ui-lib";
import { nameToInitials } from "@app/exports/utilities/nameToInitials";
import { intl } from "@app/utils/intl";
import SettingsBtn from "@app/components/Buttons/SettingsBtn";
import Menu from "@app/components/Menus/Menu";
import MenuItem from "@app/components/Menus/MenuItem";
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
  isCurrentUserOrOwner?: boolean
) => {
  return renderPrivilegesColumns({
    nameColumnLabel: nameColumnLabel,
    availablePrivileges: granteeData?.availablePrivileges,
    renderPrivilegeTooltip: (privilege: string) =>
      intl.formatMessage({ id: privilegeTooltipIds[privilege] }),
    renderNameCell: (data: any) =>
      nameCell(data, openDialog, owner, isCurrentUserOrOwner),
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
  const { item, closeMenu, openDialog, isCurrentUserAnOwner } = menuProps;
  return (
    <Menu>
      {isCurrentUserAnOwner && (
        <MenuItem
          key="grant-ownership"
          onClick={(): void => {
            openDialog(GrantActions.GRANT_OWNERSHIP, {
              openDialog: true,
              granteeId: item.granteeId,
            });
            closeMenu();
          }}
        >
          {intl.formatMessage({
            id: "Admin.Privileges.GrantOwnership",
          })}
        </MenuItem>
      )}
      <MenuItem
        key="remove-member"
        classname={classes["privileges__nameCell__moreActions-remove"]}
        onClick={(): void => {
          openDialog(GrantActions.DELETE, {
            openDialog: true,
            granteeId: item.granteeId,
          });
          closeMenu();
        }}
      >
        {intl.formatMessage({ id: "Common.Remove" })}
      </MenuItem>
    </Menu>
  );
};

const nameCell = (
  item: any,
  openDialog: (type: any, dialogState: any) => void,
  owner?: string,
  isCurrentUserAnOwner?: boolean
) => {
  const isOwner = item.granteeId === owner;
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
          />
        )}
        <Tooltip title={item.name}>
          <span className={classes["privileges__nameCell__name"]}>
            {item.name}
          </span>
        </Tooltip>
      </div>
      <div className={classes["privileges__nameCell__rightSide"]}>
        {isOwner && (
          <Tooltip
            title={intl.formatMessage({ id: "Common.Owner" })}
            placement="top"
          >
            <dremio-icon
              name="interface/owner"
              class={classes["privileges__nameCell__ownerIcon"]}
            />
          </Tooltip>
        )}
        <SettingsBtn
          classStr={`sqlScripts__menu-item__actions ${classes["privileges__nameCell__moreActions"]}`}
          menu={
            <GranteeActionMenu
              item={item}
              openDialog={openDialog}
              isCurrentUserAnOwner={isCurrentUserAnOwner}
            />
          }
          hideArrowIcon
          stopPropagation
          disabled={isOwner}
        >
          <dremio-icon
            name="interface/more"
            alt={intl.formatMessage({ id: "Common.More" })}
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
