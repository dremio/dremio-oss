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

import { useEffect, useMemo } from "react";
import { useSelector } from "react-redux";

import { useResourceSnapshot } from "smart-resource/react";
import { RolesResource } from "@app/exports/resources/RolesResource";
import { UsersResource } from "@app/exports/resources/UsersResource";
import { Avatar } from "dremio-ui-lib/components";
import { nameToInitials } from "@app/exports/utilities/nameToInitials";

import * as classes from "./SelectOwnership.module.less";

export const getUserRoleIcon = (item: {
  type: "USER" | "ROLE";
  name: string;
}) => {
  if (!item.name) {
    return (
      <dremio-icon name="interface/disabled-avatar" class={classes["icon"]} />
    );
  }
  return item.type === "USER" ? (
    <Avatar initials={nameToInitials(item.name)} />
  ) : (
    <dremio-icon name="interface/role" class={classes["icon"]} />
  );
};

const getFilteredRolesAndUsers = (
  usersArg: any = [],
  rolesArg: any = [],
  orgPrivileges: any
) => {
  let users, roles;
  if (orgPrivileges) {
    roles = (orgPrivileges?.roles?.canView && rolesArg) || [];
    users = (orgPrivileges?.users?.canView && usersArg) || [];
  } else {
    roles = rolesArg || [];
    users = usersArg || [];
  }

  const updatedRoles = roles
    .filter(({ type, name }: any) => type !== "SYSTEM" || name === "PUBLIC")
    .map((role: any) => {
      return {
        ...role,
        value: role.name,
        label: role.name,
        type: "ROLE",
      };
    });

  const updatedUsers = users.map((user: any) => {
    return {
      ...user,
      value: user.username || user.name,
      label: user.username || user.name,
      name: user.username || user.name,
      type: "USER",
    };
  });
  return [...updatedRoles, ...updatedUsers];
};

export const useOwnershipOptions = (filter: string) => {
  const orgPrivileges = useSelector(
    (state: any) => state.privileges.organization
  );
  const [users] = useResourceSnapshot(UsersResource);
  const [roles] = useResourceSnapshot(RolesResource);

  useEffect(() => {
    UsersResource.fetch({ filter });
    RolesResource.fetch({ filter });
  }, [filter]);

  const results = useMemo(() => {
    return getFilteredRolesAndUsers(users, roles, orgPrivileges);
  }, [users, roles, orgPrivileges]);

  return results;
};
