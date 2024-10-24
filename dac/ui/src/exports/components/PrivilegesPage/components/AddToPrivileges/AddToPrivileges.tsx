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
import { useState, useMemo, useCallback, useEffect, forwardRef } from "react";
import { useDispatch, useSelector } from "react-redux";
import { loadGrant } from "@inject/actions/resources/grant";
import { debounce } from "lodash";
import { intl } from "#oss/utils/intl";
import { MultiSelect, Label } from "dremio-ui-lib";
import { Button } from "dremio-ui-lib/components";
import { searchRoles } from "@inject/actions/roles";
import { searchUsers } from "@inject/actions/user";
import { getFilteredRolesAndUsers } from "@inject/selectors/roles";
import {
  getCustomChipIcon,
  useCanSearchRolesAndUsers,
} from "../../privileges-page-utils";
import { GrantObject } from "../../PrivilegesPage";
import { GranteeType as CatalogGranteeType } from "#oss/exports/endpoints/ArcticCatalogGrants/ArcticCatalogGrants.types";
import { GranteeType as OrgGranteeType } from "#oss/exports/endpoints/Grants/Grants.types";
import * as classes from "./AddToPrivileges.module.less";

export type UserOrRole = {
  id: string;
  value: string;
  type: CatalogGranteeType | OrgGranteeType;
  label: string;
  name: string;
  roles: any[];
};

type AddToPrivilegesProps = {
  innerRef?: any;
  tableItems: GrantObject[];
  handleAddTableItems: any;
  disabled: boolean;
};

const AddToPrivilegesComponent = ({
  innerRef,
  tableItems,
  handleAddTableItems,
  disabled,
}: AddToPrivilegesProps) => {
  const dispatch = useDispatch();
  const [displayValues, setDisplayValues] = useState<UserOrRole[]>([]);
  const {
    searchedOptions,
    orgPrivileges,
  }: { searchedOptions: UserOrRole[]; orgPrivileges: any } = useSelector(
    (state: any) => ({
      searchedOptions: getFilteredRolesAndUsers(
        state,
        true,
        true,
      ) as unknown as UserOrRole[],
      orgPrivileges: state?.privileges?.organization,
    }),
  );

  const [canSearchUser, canSearchRole] = useCanSearchRolesAndUsers();

  useEffect(() => {
    if (canSearchRole) dispatch(searchRoles("") as any);
    if (canSearchUser) dispatch(searchUsers("") as any);
  }, [dispatch, orgPrivileges]);

  const filteredSearchOptions = useMemo(
    () =>
      searchedOptions.filter(
        (member: UserOrRole) =>
          !tableItems.find(
            (grant: GrantObject) =>
              grant?.granteeId === member.id || grant?.id?.value === member.id,
          ),
      ),
    [tableItems, searchedOptions],
  );

  const handleValuesChange = useCallback(
    (value: UserOrRole[]) => {
      const newValues: any[] = [];
      value.forEach((val: UserOrRole) => {
        const element = filteredSearchOptions.find(
          ({ id }: UserOrRole) => id === val.id,
        );
        if (element) {
          newValues.push(element);
        } else {
          const isElementFound = displayValues.find(({ id }) => id === val.id);
          if (isElementFound) {
            newValues.push(isElementFound);
          }
        }
      });
      setDisplayValues(newValues);
    },
    [filteredSearchOptions, displayValues],
  );

  const handleAddSelectedMembers = () => {
    handleAddTableItems(displayValues);
    setDisplayValues([]);
    if (canSearchRole) dispatch(searchRoles("") as any);
    if (canSearchUser) dispatch(searchUsers("") as any);
  };

  const handleSearchKeyChange = debounce((value) => {
    if (canSearchRole) {
      dispatch(searchRoles(value) as any);
    } else {
      value && dispatch(loadGrant({ name: value, type: "ROLE" }) as any);
    }
    if (canSearchUser) {
      dispatch(searchUsers(value) as any);
    } else {
      value && dispatch(loadGrant({ userName: value, type: "USER" }) as any);
    }
  }, 300);

  return (
    <>
      <div className={classes["addPrivileges__addSection"]}>
        <Label
          className={classes["addPrivileges__addLabel"]}
          value={intl.formatMessage({
            id: "Admin.Privileges.SearchLabel",
          })}
        />
        <div className={classes["addPrivileges__addActions"]}>
          <MultiSelect
            ref={innerRef}
            referToId
            value={displayValues}
            options={filteredSearchOptions}
            onChange={handleSearchKeyChange}
            displayValues={displayValues}
            handleChange={handleValuesChange}
            limitTags={3}
            disabled={disabled}
            placeholder={intl.formatMessage({
              id: "Admin.Privileges.SearchPlaceholder",
            })}
            getCustomChipIcon={getCustomChipIcon}
          />
          <Button
            variant={displayValues.length === 0 ? "secondary" : "primary"}
            disabled={displayValues.length === 0 || disabled}
            className={classes["addPrivileges__addButton"]}
            onClick={handleAddSelectedMembers}
          >
            <>
              <dremio-icon name="interface/add" alt="add" />
              {intl.formatMessage({
                id: "Admin.Privileges.Add",
              })}
            </>
          </Button>
        </div>
      </div>
    </>
  );
};

const AddToPrivileges = forwardRef((props: AddToPrivilegesProps, ref) => {
  return <AddToPrivilegesComponent {...props} innerRef={ref} />;
});
export default AddToPrivileges;
