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

import * as classes from "./ArcticGitActionsMenu.module.less";

type ArcticGitActionsMenuProps = {
  fromItem: Reference;
  handleOpenDialog: (dialogType: "TAG" | "BRANCH", dialogState: any) => void;
  closeMenu?: () => unknown;
  canDeleteTag?: boolean;
};

function ArcticGitActionsMenu({
  fromItem,
  handleOpenDialog,
  closeMenu = () => {},
  canDeleteTag,
}: ArcticGitActionsMenuProps): JSX.Element {
  const fromRef = {
    type: fromItem.type,
    name: fromItem.name,
    hash: fromItem?.hash,
  } as Reference;

  return (
    <ul className={classes["git-arctic-actions-menu"]}>
      <li
        onClick={(e: any) => {
          handleOpenDialog("BRANCH", { openDialog: true, fromRef });
          e.stopPropagation();
          closeMenu();
        }}
        key="create-branch"
      >
        <FormattedMessage id="ArcticCatalog.Tags.CreateBranch" />
      </li>
      <li
        onClick={(e: any) => {
          handleOpenDialog("TAG", { openDialog: true, fromRef });
          e.stopPropagation();
          closeMenu();
        }}
        key="add-tag"
      >
        <FormattedMessage id="ArcticCatalog.Tags.AddTag" />
      </li>
      {canDeleteTag && (
        <li
          onClick={(e: any) => {
            handleOpenDialog("TAG", {
              openDialog: true,
              fromRef,
              deleteTag: true,
            });
            e.stopPropagation();
            closeMenu();
          }}
          key="delete-tag"
          className={classes["delete-action"]}
        >
          <FormattedMessage id="ArcticCatalog.Tags.DeleteTag" />
        </li>
      )}
    </ul>
  );
}

export default ArcticGitActionsMenu;
