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
import Message from "@app/components/Message";
import { FormSubmit } from "@inject/pages/ArcticCatalog/components/ArcticCatalogDataItemSettings/ArcticTableOptimization/components/FormSubmit/FormSubmit";
import RemoveGranteeDialog from "dremio-ui-common/components/PrivilegesTable/components/RemoveGranteeDialog/RemoveGranteeDialog.js";
import { PrivilegesTable } from "dremio-ui-common/components/PrivilegesTable/PrivilegesTable.js";
import AddToPrivileges, {
  UserOrRole,
} from "./components/AddToPrivileges/AddToPrivileges";
import { getPrivilegesColumns } from "./privileges-page-utils";
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
import SelectOwnership from "./components/SelectOwnership/SelectOwnership";

import * as classes from "./PrivilegesPage.module.less";

export type GrantObject = GrantForGet | GrantForSet | Grant | OutputGrant;

export type GranteeData = InlineResponse200 | GrantsResponse | null;

type Privilege = OrgPrivilege | CatalogPrivilege;

type PrivilegesPageProps = {
  granteeData: GranteeData;
  isLoading: boolean;
  nameColumnLabel: string;
  privilegeTooltipIds: any;
  entityName: string;
  addedDefaultGrantValues?: any;
  handleUpdateOwnership: (id: string) => void;
  onSubmit: (
    formValues: any,
    tableItems: GrantObject[],
    deletedTableItems: GrantObject[],
    cachedData: any,
    setDirtyStateForLeaving: () => void,
  ) => void;
  setChildDirtyState: any;
  pageType: "organization" | "catalog" | "entity";
  isAdmin?: boolean;
  ownership: {
    id: string;
    owner: string;
    type: "USER" | "ROLE";
    name: string;
    canUpdateOwnership?: boolean;
  } | null;
  granteeError?: string;
  buttonPlacement?: "LEFT" | "RIGHT";
  infoMessage?: string;
  message?: {
    text: string;
    messageType: string;
  };
  setMessage?: (arg: string) => void;
  insideOfTab?: boolean;
};

const INITIAL_DIALOG_STATE = {
  openDialog: false,
  granteeId: undefined,
};

const PRIVILEGES_KEY = "PRIVILEGES";

const PrivilegesPage = ({
  isAdmin,
  granteeData,
  isLoading,
  nameColumnLabel,
  privilegeTooltipIds,
  entityName,
  addedDefaultGrantValues,
  handleUpdateOwnership,
  onSubmit,
  setChildDirtyState,
  granteeError,
  buttonPlacement = "LEFT",
  infoMessage,
  message,
  ownership,
  pageType,
  setMessage,
  insideOfTab = false,
}: PrivilegesPageProps) => {
  const dispatch = useDispatch();
  const addPrivilegesRef = useRef(null);
  const [removeDialogState, setRemoveDialogState] = useState<{
    openDialog: boolean;
    granteeId?: string;
  }>(INITIAL_DIALOG_STATE);
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
    ownership?.id ?? "",
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
      insideOfTab
        ? setChildDirtyState(false)
        : setChildDirtyState(PRIVILEGES_KEY)(false);
    }
  }, [granteeData, reset, defaultValues, setChildDirtyState, insideOfTab]);

  // Whenever data is retrieved, the form should reset
  useEffect(() => {
    handleReset();
  }, [handleReset]);

  useEffect(() => {
    if (formState.isDirty) {
      insideOfTab
        ? setChildDirtyState(true)
        : setChildDirtyState(PRIVILEGES_KEY)(true);
    }
  }, [formState.isDirty, setChildDirtyState, insideOfTab]);

  useEffect(() => {
    if (granteeError && !insideOfTab) {
      dispatch(addNotification(granteeError, "error"));
    }
  }, [dispatch, granteeError, insideOfTab]);

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
            (updateItem) => updateItem.id === deleteItem.granteeId,
          ) === undefined,
      ),
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

  const handleOpenDialog = (dialogState: {
    openDialog: boolean;
    granteeId?: string;
  }) => setRemoveDialogState(dialogState);

  const closeDialog = () => {
    setRemoveDialogState(INITIAL_DIALOG_STATE);
  };

  const columns = useMemo(
    () =>
      getPrivilegesColumns(
        granteeData,
        handleOpenDialog,
        nameColumnLabel,
        privilegeTooltipIds,
        ownership?.id,
      ),
    [ownership?.id, granteeData, nameColumnLabel, privilegeTooltipIds],
  );

  const disableSubmitButtons =
    isLoading || !formState.isDirty || !!granteeError;

  return (
    <div className={classes["privileges"]}>
      <div className={classes["privileges__page"]}>
        {message?.text && (
          <Message
            message={message.text}
            messageType={message.messageType}
            onDismiss={setMessage}
          />
        )}
        {infoMessage && (
          <Message
            isDismissable={false}
            message={infoMessage}
            messageType="info"
            className={classes["privileges__page__info-message"]}
          />
        )}
        <SelectOwnership
          pageType={pageType}
          ownership={ownership}
          handleUpdateOwnership={handleUpdateOwnership}
          entityName={entityName}
          canTransferOwnership={
            ownership?.canUpdateOwnership || isCurrentUserAnOwner || isAdmin
          }
        />
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
                () =>
                  insideOfTab
                    ? setChildDirtyState(false)
                    : setChildDirtyState(PRIVILEGES_KEY)(false),
              ),
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
              buttonPlacement={buttonPlacement}
              onCancel={handleReset}
              disabled={disableSubmitButtons}
            />
          </form>
        </FormProvider>
      </div>
      <RemoveGranteeDialog
        open={removeDialogState.openDialog}
        closeDialog={closeDialog}
        grantee={tableItems?.find(
          (item: GrantObject) =>
            item.granteeId === removeDialogState?.granteeId,
        )}
        className={classes["privileges__dialog"]}
        onRemove={handleDeleteTableItem}
      />
    </div>
  );
};

export default PrivilegesPage;
