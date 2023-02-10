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

import { Reference } from "@app/types/nessie";
import { intl } from "@app/utils/intl";
import SettingsBtn from "@app/components/Buttons/SettingsBtn";
import Menu from "@app/components/Menus/Menu";
import MenuItem from "@app/components/Menus/MenuItem";
import moment from "@app/utils/dayjs";

// @ts-ignore
import { Tooltip } from "dremio-ui-lib";

import {
  DEFAULT_FORMAT_WITH_TIME_SECONDS,
  formatDate,
  formatDateRelative,
  formatDateSince,
} from "@app/utils/date";

export const convertISOStringWithTooltip = (
  commitTime: string,
  options?: {
    isRelative?: boolean;
  }
): string | JSX.Element => {
  if (options?.isRelative) {
    const pastSevenDays = moment().subtract(6, "days").startOf("day");
    const curCommitTime = moment(commitTime);

    if (curCommitTime > pastSevenDays) {
      return (
        <Tooltip
          title={formatDate(commitTime.toString(), "MMM DD, YYYY, h:mmA")}
        >
          <span>{formatDateRelative(new Date(commitTime).toString())}</span>
        </Tooltip>
      );
    }

    return formatDateSince(
      new Date(commitTime).toString(),
      "MMM DD, YYYY, h:mmA"
    );
  } else if (commitTime) {
    return formatDate(commitTime, DEFAULT_FORMAT_WITH_TIME_SECONDS);
  } else {
    return "";
  }
};

export const renderIcons = (
  branch: Reference,
  renderIcon: boolean,
  isArcticSource: boolean,
  goToDataset: () => void,
  openCreateDialog: (arg: Reference, isDefault?: boolean) => void,
  openDeleteDialog?: (arg: Reference) => void,
  openMergeDialog?: (arg: Reference) => void,
  isDefault?: boolean,
  openTagDialog?: (arg: Reference, isDefault?: boolean) => void
): JSX.Element => {
  const renderMenu = () => {
    return (
      <Menu>
        {openTagDialog && (
          <MenuItem onClick={() => openTagDialog(branch, isDefault)}>
            <span className="branch-list-menu-item">
              {intl.formatMessage({ id: "ArcticCatalog.Tags.AddTag" })}
            </span>
          </MenuItem>
        )}
        {openDeleteDialog && (
          <MenuItem onClick={() => openDeleteDialog(branch)}>
            <span className="branch-list-menu-item-delete">
              {intl.formatMessage({ id: "Common.Delete" })}
            </span>
          </MenuItem>
        )}
      </Menu>
    );
  };

  const renderCreateProject = (defaultBranch?: boolean) => {
    return (
      <span
        onClick={() => openCreateDialog(branch, defaultBranch)}
        className="branch-list-item-icon"
      >
        <Tooltip
          title={intl.formatMessage({ id: "RepoView.CreateBranch" })}
          placement="top"
        >
          <dremio-icon name="vcs/create-branch" />
        </Tooltip>
      </span>
    );
  };

  const renderGoToDataset = () => {
    if (!isArcticSource) {
      return (
        <span onClick={goToDataset} className="branch-list-item-icon">
          <Tooltip
            title={intl.formatMessage({ id: "Go.To.Data" })}
            placement="top"
          >
            <dremio-icon name="interface/goto-dataset" />
          </Tooltip>
        </span>
      );
    }
  };

  const renderMerge = () => {
    return (
      <span
        onClick={() => openMergeDialog && openMergeDialog(branch)}
        className="branch-list-item-icon"
      >
        <Tooltip
          title={intl.formatMessage({ id: "Common.Merge" })}
          placement="top"
        >
          <dremio-icon name="vcs/merge" />
        </Tooltip>
      </span>
    );
  };

  const renderSettings = () => {
    return (
      <SettingsBtn
        classStr="branch-list-item-settings-icon"
        handleSettingsClose={() => {}}
        handleSettingsOpen={() => {}}
        menu={renderMenu()}
        hideArrowIcon
      >
        <dremio-icon
          name="interface/more"
          alt={intl.formatMessage({ id: "Common.More" })}
        />
      </SettingsBtn>
    );
  };

  return isDefault && renderIcon ? (
    <>
      {renderGoToDataset()}
      {renderMerge()}
      {renderCreateProject(isDefault)}
      {renderSettings()}
    </>
  ) : renderIcon ? (
    <>
      {renderGoToDataset()}
      {renderMerge()}
      {renderCreateProject()}
      {renderSettings()}
    </>
  ) : (
    <></>
  );
};
