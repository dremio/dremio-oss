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
import { debounce } from "lodash";

import { intl } from "@app/utils/intl";
// @ts-ignore
import { MultiSelect, Label } from "dremio-ui-lib";
import { Button } from "dremio-ui-lib/components";
import { searchRoles } from "@inject/actions/roles";
import { searchUsers } from "@inject/actions/user";
import { getFilteredRolesAndUsers } from "@inject/selectors/roles";
import { getCustomChipIcon } from "../../privileges-page-utils";
import { GrantObject } from "../../PrivilegesPage";
import { GranteeType as CatalogGranteeType } from "@app/exports/endpoints/ArcticCatalogGrants/ArcticCatalogGrants.types";
import { GranteeType as OrgGranteeType } from "@app/exports/endpoints/Grants/Grants.types";

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
  const [selectedValues, setSelectedValues] = useState<string[]>([]);
  const [displayValues, setDisplayValues] = useState<UserOrRole[]>([]);

  const {
    searchedOptions,
    orgPrivileges,
  }: { searchedOptions: UserOrRole[]; orgPrivileges: any } = useSelector(
    (state: any) => ({
      searchedOptions: getFilteredRolesAndUsers(
        state,
        true
      ) as unknown as UserOrRole[],
      orgPrivileges: state.privileges.organization,
    })
  );

  useEffect(() => {
    const { roles, users } = orgPrivileges;
    if (roles?.canView) dispatch(searchRoles("") as any);
    if (users?.canView) dispatch(searchUsers("") as any);
  }, [dispatch, orgPrivileges]);

  const filteredSearchOptions = useMemo(() => {
    return searchedOptions.filter(
      (member: UserOrRole) =>
        !tableItems.find((grant: GrantObject) => grant.granteeId === member.id)
    );
  }, [tableItems, searchedOptions]);

  const handleValuesChange = useCallback(
    (value: string[]) => {
      const newSelectedValues: any[] = [];
      value.forEach((val: string) => {
        const element = filteredSearchOptions.find(
          ({ label }: UserOrRole) => label === val
        );
        if (element) {
          newSelectedValues.push(element);
        } else {
          const isElementFound = displayValues.find(
            ({ label }) => label === val
          );
          if (isElementFound) {
            newSelectedValues.push(isElementFound);
          }
        }
      });
      setSelectedValues(value);
      setDisplayValues(newSelectedValues);
    },
    [filteredSearchOptions, displayValues]
  );

  const handleAddSelectedMembers = () => {
    const { roles, users } = orgPrivileges;
    handleAddTableItems(displayValues);
    setSelectedValues([]);
    setDisplayValues([]);
    if (roles?.canView) dispatch(searchRoles("") as any);
    if (users?.canView) dispatch(searchUsers("") as any);
  };

  const handleSearchKeyChange = debounce((value) => {
    const { roles, users } = orgPrivileges;
    if (roles?.canView) dispatch(searchRoles(value) as any);
    if (users?.canView) dispatch(searchUsers(value) as any);
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
            value={selectedValues}
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
            variant={selectedValues.length === 0 ? "secondary" : "primary"}
            disabled={selectedValues.length === 0 || disabled}
            className={classes["addPrivileges__addButton"]}
            onClick={handleAddSelectedMembers}
          >
            <>
              <dremio-icon name="interface/add" />
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
