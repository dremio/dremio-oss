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

import { Select } from "@mantine/core";
import { useState } from "react";

import { getIntlContext } from "dremio-ui-common/contexts/IntlContext.js";
import { Button } from "dremio-ui-lib/components";
import { getUserRoleIcon, useOwnershipOptions } from "./utils";
import TransferOwnershipDialog from "../TransferOwnershipDialog/TransferOwnershipDialog";

import * as classes from "./SelectOwnership.module.less";

type SelectOwnershipProps = {
  entityName: string;
  canTransferOwnership?: boolean;
  handleUpdateOwnership: (id: string) => void;
  pageType: "organization" | "catalog" | "entity";
  ownership: {
    id: string;
    name: string;
    owner: string;
    type: "USER" | "ROLE";
  } | null;
};

const INITIAL_DIALOG_STATE: { openDialog: boolean } = { openDialog: false };

const SelectOwnership = (props: SelectOwnershipProps) => {
  const {
    entityName,
    canTransferOwnership,
    handleUpdateOwnership,
    ownership,
    pageType,
  } = props;
  const { t } = getIntlContext();
  const [isEditingOwnership, setIsEditingOwnership] = useState(false);
  const [search, setSearch] = useState("");
  const [newOwner, setIsNewOwner] = useState<any>(undefined);
  const [ownershipDialogState, setOwnershipDialogState] =
    useState(INITIAL_DIALOG_STATE);
  const ownershipOptions = useOwnershipOptions(search);

  const handleNewOwner = (value: string | null) => {
    setIsNewOwner(ownershipOptions.find((owner) => owner?.name === value));
  };

  const closeEditing = () => {
    setIsEditingOwnership(false);
    setSearch("");
  };

  const closeDialog = () => {
    setOwnershipDialogState(INITIAL_DIALOG_STATE);
  };

  return (
    <div
      className={`flex pt-2 gap-1 flex-col pb-2 ${classes["select-ownership"]}`}
    >
      <label>Owner</label>
      {isEditingOwnership ? (
        <div className="flex items-center flex-row gap-2">
          <div style={{ width: 440 }}>
            <Select
              searchable
              nothingFound="No options"
              data={ownershipOptions}
              onSearchChange={setSearch}
              itemComponent={({ name, type, ...others }: any) => (
                <div {...others}>
                  <div className="flex items-center flex-row gap-05">
                    {getUserRoleIcon({ name, type })}
                    {name}
                  </div>
                </div>
              )}
              zIndex={1000}
              onChange={handleNewOwner}
              onDropdownOpen={() => setSearch("")}
            />
          </div>
          <Button
            variant="tertiary"
            onClick={closeEditing}
            style={{ minWidth: "initial" }}
          >
            {t("Common.Actions.Cancel")}
          </Button>
          <Button
            variant="tertiary"
            disabled={!newOwner}
            onClick={() =>
              setOwnershipDialogState({
                openDialog: true,
              })
            }
            style={{ minWidth: "initial" }}
          >
            {t("Common.Transfer")}
          </Button>
        </div>
      ) : (
        <div className="flex items-center flex-row">
          <div
            className="color-faded flex items-center gap-05"
            style={{ width: 440 }}
          >
            {getUserRoleIcon({
              type: ownership?.type || "USER",
              name: ownership?.name || "",
            })}
            {ownership?.name || (
              <span style={{ color: "var(--text--disabled)" }}>
                {t("Admin.Privileges.NoOwner")}
              </span>
            )}
          </div>
          {canTransferOwnership && (
            <Button
              variant="tertiary"
              onClick={() => setIsEditingOwnership(true)}
            >
              {t("Admin.Privileges.TransferOwnership")}
            </Button>
          )}
        </div>
      )}
      <TransferOwnershipDialog
        open={ownershipDialogState.openDialog}
        closeDialog={closeDialog}
        newOwner={newOwner}
        onUpdate={(id: string) => {
          handleUpdateOwnership(id);
          closeDialog();
          closeEditing();
        }}
        entityName={entityName}
        pageType={pageType}
        className={classes["select-ownership__dialog"]}
      />
    </div>
  );
};

export default SelectOwnership;
