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

import FontIcon from "@app/components/Icon/FontIcon";

import { Reference } from "@app/services/nessie/client";

import {
  DEFAULT_FORMAT_WITH_TIME_SECONDS,
  formatDate,
  formatDateRelative,
} from "@app/utils/date";

export const convertISOString = (
  ref: Reference,
  commitTime?: string
): string => {
  if (
    ref.metadata &&
    ref.metadata.commitMetaOfHEAD &&
    ref.metadata.commitMetaOfHEAD.commitTime
  )
    return formatDateRelative(
      new Date(ref.metadata.commitMetaOfHEAD.commitTime).toDateString()
    );
  else if (commitTime) {
    return formatDate(commitTime, DEFAULT_FORMAT_WITH_TIME_SECONDS);
  } else return "";
};

export const renderIcons = (
  branch: Reference,
  renderIcon: boolean,
  openCreateDialog: (branch: Reference) => void,
  openDeleteDialog?: (branch: Reference) => void,
  isDefault?: boolean
): JSX.Element => {
  return isDefault ? (
    <span
      onClick={() => openCreateDialog(branch)}
      className="branch-list-item-right-icon"
    >
      <FontIcon
        type={"GitBranch"}
        theme={{ Icon: { width: "19px", height: "20px" } }}
      />
    </span>
  ) : renderIcon ? (
    <>
      <span
        onClick={() => openCreateDialog(branch)}
        className="branch-list-item-left-icon"
      >
        <FontIcon
          type={"GitBranch"}
          theme={{ Icon: { width: "19px", height: "20px" } }}
        />
      </span>
      <span
        onClick={() => openDeleteDialog && openDeleteDialog(branch)}
        className="branch-list-item-right-icon"
      >
        <FontIcon
          type={"TrashDark"}
          theme={{ Icon: { width: "22px", height: "24px" } }}
        />
      </span>
    </>
  ) : (
    <></>
  );
};
