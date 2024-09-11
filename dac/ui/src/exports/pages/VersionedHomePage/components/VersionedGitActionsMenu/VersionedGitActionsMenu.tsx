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

import { FormattedMessage } from "react-intl";
import { Reference } from "@app/types/nessie";
import { CatalogPrivilegeSwitch } from "@app/exports/components/CatalogPrivilegeSwitch/CatalogPrivilegeSwitch";

import * as classes from "./VersionedGitActionsMenu.module.less";

type VersionedGitActionsMenuProps = {
  fromItem: Reference;
  handleOpenDialog: (dialogType: "TAG" | "BRANCH", dialogState: any) => void;
  closeMenu?: () => unknown;
  canDeleteTag?: boolean;
};

function VersionedGitActionsMenu({
  fromItem,
  handleOpenDialog,
  closeMenu = () => {},
  canDeleteTag,
}: VersionedGitActionsMenuProps): JSX.Element {
  const fromRef = {
    type: fromItem.type,
    name: fromItem.name,
    hash: fromItem?.hash,
  } as Reference;

  const handleOpenBranch = (e: any) => {
    handleOpenDialog("BRANCH", { openDialog: true, fromRef });
    e.stopPropagation();
    closeMenu();
  };

  const handleOpenTag = (e: any) => {
    handleOpenDialog("TAG", { openDialog: true, fromRef });
    e.stopPropagation();
    closeMenu();
  };

  const handleDeleteTag = (e: any) => {
    handleOpenDialog("TAG", {
      openDialog: true,
      fromRef,
      deleteTag: true,
    });
    e.stopPropagation();
    closeMenu();
  };

  return (
    <ul className={classes["versioned-git-actions-menu"]}>
      <CatalogPrivilegeSwitch
        privilege={["branch", "canCreate"]}
        renderEnabled={() => (
          <li
            onClick={handleOpenBranch}
            onKeyDown={(e) => {
              if (e.code === "Enter" || e.code === "Space") {
                handleOpenBranch(e);
              }
            }}
            key="create-branch"
            tabIndex={0}
          >
            <FormattedMessage id="VersionedEntity.Tags.CreateBranch" />
          </li>
        )}
      />
      <CatalogPrivilegeSwitch
        privilege={["tag", "canCreate"]}
        renderEnabled={() => (
          <li
            onClick={handleOpenTag}
            onKeyDown={(e) => {
              if (e.code === "Enter" || e.code === "Space") {
                handleOpenTag(e);
              }
            }}
            key="add-tag"
            tabIndex={0}
          >
            <FormattedMessage id="VersionedEntity.Tags.AddTag" />
          </li>
        )}
      />

      {canDeleteTag && (
        <li
          onClick={handleDeleteTag}
          onKeyDown={(e) => {
            if (e.code === "Enter" || e.code === "Space") {
              handleDeleteTag(e);
            }
          }}
          key="delete-tag"
          className={classes["delete-action"]}
          tabIndex={0}
        >
          <FormattedMessage id="VersionedEntity.Tags.DeleteTag" />
        </li>
      )}
    </ul>
  );
}

export default VersionedGitActionsMenu;
