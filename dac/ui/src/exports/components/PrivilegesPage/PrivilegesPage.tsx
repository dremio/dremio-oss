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

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { FormProvider, useForm } from "react-hook-form";
import { compose } from "redux";

import FormUnsavedRouteLeave from "@app/components/Forms/FormUnsavedRouteLeave";
import { FormSubmit } from "@app/exports/pages/ArcticCatalog/components/ArcticCatalogDataItemSettings/ArcticTableOptimization/components/FormSubmit/FormSubmit";
// @ts-ignore
import GrantOwnershipDialog from "dremio-ui-common/components/PrivilegesTable/components/GrantOwnershipDialog/GrantOwnershipDialog.js";
// @ts-ignore
import RemoveGranteeDialog from "dremio-ui-common/components/PrivilegesTable/components/RemoveGranteeDialog/RemoveGranteeDialog.js";
// @ts-ignore
import { PrivilegesTable } from "dremio-ui-common/components/PrivilegesTable/PrivilegesTable.js";
import AddToPrivileges, {
  UserOrRole,
} from "./components/AddToPrivileges/AddToPrivileges";
import {
  getPrivilegesColumns,
  GrantActions,
  GrantActionsType,
} from "./privileges-page-utils";
import {
  Grant,
  GrantsResponse,
  OutputGrant,
  Privilege as OrgPrivilege,
} from "@app/exports/endpoints/Grants/Grants.types";
import {
  GrantForGet,
  GrantForSet,
  InlineResponse200,
  Privilege as CatalogPrivilege,
} from "@app/exports/endpoints/ArcticCatalogGrants/ArcticCatalogGrants.types";
import EmptyPrivileges from "./components/EmptyPrivileges/EmptyPrivileges";
import { useUserDetails } from "@app/exports/providers/useUserDetails";
import { useDispatch } from "react-redux";
import { addNotification } from "@app/actions/notification";

import * as classes from "./PrivilegesPage.module.less";

export type GrantObject = GrantForGet | GrantForSet | Grant | OutputGrant;

export type GranteeData = InlineResponse200 | GrantsResponse | null;

type Privilege = OrgPrivilege | CatalogPrivilege;

type PrivilegesPageProps = {
  granteeData: GranteeData;
  isLoading: boolean;
  nameColumnLabel: string;
  privilegeTooltipIds: any;
  entityOwnerId: string;
  addedDefaultGrantValues?: any;
  handleUpdateOwnership: (id: string) => void;
  onSubmit: (
    formValues: any,
    tableItems: GrantObject[],
    deletedTableItems: GrantObject[],
    cachedData: any,
    setDirtyStateForLeaving: () => void
  ) => void;
  setChildDirtyState: any;
  granteeError?: string;
};

const INITIAL_DIALOG_STATE = {
  openDialog: false,
  granteeId: undefined,
};

const PRIVILEGES_KEY = "PRIVILEGES";

const PrivilegesPage = ({
  granteeData,
  isLoading,
  nameColumnLabel,
  privilegeTooltipIds,
  entityOwnerId,
  addedDefaultGrantValues,
  handleUpdateOwnership,
  onSubmit,
  setChildDirtyState,
  granteeError,
}: PrivilegesPageProps) => {
  const dispatch = useDispatch();
  const addPrivilegesRef = useRef(null);
  const [ownershipDialogState, setOwnershipDialogState] =
    useState(INITIAL_DIALOG_STATE);
  const [removeDialogState, setRemoveDialogState] =
    useState(INITIAL_DIALOG_STATE);
  const [user] = useUserDetails();
  const currentRolesAndUserIds = useMemo(() => {
    // Get the user id and roles' ids that the user is a member of
    if (user) {
      const ids = [user.id];
      (user.roles ?? []).forEach((role: any) => ids.push(role.id));
      return ids;
    } else return [];
  }, [user]);
  const isCurrentUserAnOwner = currentRolesAndUserIds.includes(
    entityOwnerId ?? ""
  );

  const [tableItems, setTableItems] = useState<GrantObject[]>([]);
  const [deletedTableItems, setDeletedTableItems] = useState<GrantObject[]>([]);

  // Create default values based on GET data
  const { defaultValues, cachedData }: any = useMemo(() => {
    if (!granteeData) return {};

    const cached: any = {};
    const initFormValues: any = {};
    granteeData?.grants?.forEach((grantee: GrantObject) => {
      granteeData?.availablePrivileges?.forEach((priv: Privilege) => {
        initFormValues[`${grantee.granteeId}-${priv}`] =
          grantee.privileges.includes(priv as any);
      });
      cached[grantee.granteeId] = true;
    });

    return { defaultValues: initFormValues, cachedData: cached };
  }, [granteeData]);

  const methods = useForm({
    mode: "onChange",
    defaultValues: defaultValues,
  });
  const { formState, handleSubmit, reset, setValue } = methods;

  const handleReset = useCallback(() => {
    if (granteeData?.grants) {
      setTableItems(granteeData.grants);
      setDeletedTableItems([]);
      // reset the form with default values after data is retrieved
      reset(defaultValues);
      setChildDirtyState(PRIVILEGES_KEY)(false);
    }
  }, [granteeData, reset, defaultValues, setChildDirtyState]);

  // Whenever data is retrieved, the form should reset
  useEffect(() => {
    handleReset();
  }, [handleReset]);

  useEffect(() => {
    if (formState.isDirty) {
      setChildDirtyState(PRIVILEGES_KEY)(true);
    }
  }, [formState.isDirty, setChildDirtyState]);

  useEffect(() => {
    if (granteeError) {
      dispatch(addNotification(granteeError, "error"));
    }
  }, [dispatch, granteeError]);

  const handleAddTableItems = (displayValues: UserOrRole[]) => {
    setTableItems((items: GrantObject[]) => {
      return [
        ...displayValues.map((item: UserOrRole) => ({
          granteeId: item.id,
          privileges: [],
          name: item.value,
          granteeType: item.type,
          ...(addedDefaultGrantValues ?? {}),
        })),
        ...items,
      ];
    });
    // Need to clear from delete if adding back
    setDeletedTableItems((items: GrantObject[]) =>
      items.filter(
        (deleteItem) =>
          displayValues.find(
            (updateItem) => updateItem.id === deleteItem.granteeId
          ) === undefined
      )
    );
  };

  const handleDeleteTableItem = (id: string) => {
    if (id in cachedData) {
      // Add to deletion array if it exists from cached results
      const deleteItem = tableItems.find((item) => item.granteeId === id);
      setDeletedTableItems((prevItems: any) => [...prevItems, deleteItem]);
    }

    // Remove from the UI
    granteeData?.availablePrivileges?.forEach((priv: Privilege) => {
      setValue(`${id}-${priv}`, false, { shouldDirty: true });
    });
    setTableItems((items) => items.filter((item) => item.granteeId !== id));
  };

  const handleOpenDialog = useCallback(
    (type: GrantActionsType, dialogState: any) => {
      if (type === GrantActions.GRANT_OWNERSHIP) {
        setOwnershipDialogState(dialogState);
      } else {
        setRemoveDialogState(dialogState);
      }
    },
    []
  );

  const closeDialog = () => {
    setOwnershipDialogState(INITIAL_DIALOG_STATE);
    setRemoveDialogState(INITIAL_DIALOG_STATE);
  };

  const columns = useMemo(
    () =>
      getPrivilegesColumns(
        granteeData,
        handleOpenDialog,
        nameColumnLabel,
        privilegeTooltipIds,
        entityOwnerId,
        isCurrentUserAnOwner
      ),
    [
      granteeData,
      handleOpenDialog,
      nameColumnLabel,
      privilegeTooltipIds,
      entityOwnerId,
      isCurrentUserAnOwner,
    ]
  );

  const disableSubmitButtons =
    isLoading || !formState.isDirty || !!granteeError;

  return (
    <div className={classes["privileges"]}>
      <div className={classes["privileges__page"]}>
        <AddToPrivileges
          ref={addPrivilegesRef}
          tableItems={tableItems}
          handleAddTableItems={handleAddTableItems}
          disabled={isLoading || !!granteeError}
        />
        <FormProvider {...methods}>
          <form
            onSubmit={handleSubmit((formValues: any) =>
              onSubmit(
                formValues,
                tableItems,
                deletedTableItems,
                cachedData,
                () => setChildDirtyState(PRIVILEGES_KEY)(false)
              )
            )}
            className={classes["privileges__form"]}
          >
            <div className={classes["privileges__tableWrapper"]}>
              <PrivilegesTable
                columns={columns}
                getRow={(i: number) => {
                  const data = tableItems[i];
                  return {
                    id: data?.granteeId || i,
                    data: data ? data : null,
                  };
                }}
                rowCount={tableItems?.length}
              />
              {!isLoading && !granteeError && tableItems.length === 0 && (
                <EmptyPrivileges
                  onClick={() => (addPrivilegesRef.current as any)?.click?.()}
                />
              )}
            </div>
            <FormSubmit
              onCancel={handleReset}
              disabled={disableSubmitButtons}
            />
          </form>
        </FormProvider>
      </div>
      <GrantOwnershipDialog
        open={ownershipDialogState.openDialog}
        closeDialog={closeDialog}
        granteeId={ownershipDialogState?.granteeId}
        className={classes["privileges__dialog"]}
        onUpdate={handleUpdateOwnership}
      />
      <RemoveGranteeDialog
        open={removeDialogState.openDialog}
        closeDialog={closeDialog}
        grantee={tableItems?.find(
          (item: GrantObject) => item.granteeId === removeDialogState?.granteeId
        )}
        className={classes["privileges__dialog"]}
        onRemove={handleDeleteTableItem}
      />
    </div>
  );
};

export default compose(FormUnsavedRouteLeave)(PrivilegesPage);
