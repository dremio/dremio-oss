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
import { Tag } from "@app/services/nessie/client";
import { Reference } from "@app/types/nessie";

import * as classes from "./TagsActionsMenu.module.less";

type TagsActionsMenuProps = {
  tag: { type: "TAG" } & Tag;
  setDialogState: React.Dispatch<React.SetStateAction<any>>;

  // passed in by <SettingsBtn>
  closeMenu?: () => unknown;
};

function TagsActionsMenu({
  tag,
  setDialogState,
  closeMenu = () => {},
}: TagsActionsMenuProps): JSX.Element {
  const fromRef = { type: "TAG", name: tag.name, hash: tag?.hash } as Reference;

  return (
    <ul className={classes["tags-actions-menu"]}>
      <li
        onClick={(e: any) => {
          setDialogState({
            openDialog: true,
            newRefType: "BRANCH",
            fromRef,
          });
          e.stopPropagation();
          closeMenu();
        }}
        key="create-branch"
      >
        <FormattedMessage id="ArcticCatalog.Tags.CreateBranch" />
      </li>
      <li
        onClick={(e: any) => {
          setDialogState({
            openDialog: true,
            newRefType: "TAG",
            fromRef,
          });
          e.stopPropagation();
          closeMenu();
        }}
        key="add-tag"
      >
        <FormattedMessage id="ArcticCatalog.Tags.AddTag" />
      </li>
    </ul>
  );
}

export default TagsActionsMenu;
