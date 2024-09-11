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
import SettingsBtn from "@app/components/Buttons/SettingsBtn";
// @ts-ignore
import { IconButton } from "dremio-ui-lib";
import { Avatar } from "dremio-ui-lib/components";
import { VersionedPageTabsType } from "@app/exports/pages/VersionedHomePage/VersionedHomePage";
import { Tag } from "@app/services/nessie/client";
import { nameToInitials } from "@app/exports/utilities/nameToInitials";
import VersionedGitActionsMenu from "../../VersionedGitActionsMenu/VersionedGitActionsMenu";
import { convertISOStringWithTooltip } from "@app/pages/NessieHomePage/components/RepoView/components/RepoViewBody/components/RepoViewBranchList/utils";
import { getShortHash } from "@app/utils/nessieUtils";
import CopyButton from "components/Buttons/CopyButton";

export const getTagsTableColumns = () => {
  return [
    {
      key: "tagName",
      label: <FormattedMessage id="VersionedEntity.Tags.TagName" />,
      disableSort: true,
    },
    {
      key: "author",
      label: <FormattedMessage id="Common.Author" />,
      disableSort: true,
    },
    {
      key: "commitID",
      label: <FormattedMessage id="VersionedEntity.Commits.CommitID" />,
      disableSort: true,
    },
    {
      key: "commitMessage",
      label: <FormattedMessage id="VersionedEntity.Commits.CommitMessage" />,
      disableSort: true,
    },
    {
      key: "commitTime",
      label: <FormattedMessage id="Common.CommitTime" />,
    },
  ];
};

export const generateTableRows = (
  data: ({ type: "TAG" } & Tag)[],
  goToDataTab: (tab: VersionedPageTabsType, tag: { type: "TAG" } & Tag) => void,
  handleOpenDialog: (dialogType: "TAG" | "BRANCH", dialogState: any) => void,
) => {
  const tableData: any[] = [];

  data.forEach((tag) => {
    const commitData = tag?.metadata?.commitMetaOfHEAD;
    const author = commitData?.authors?.[0];

    tableData.push({
      id: tag.hash,
      rowClassName: tag.hash,
      data: {
        tagName: {
          node: () => (
            <div className="tag-name">
              <dremio-icon name="vcs/tag" class="tag-name__icon" />
              <span className="tag-name__name">{tag.name}</span>
            </div>
          ),
        },
        author: {
          node: () => (
            <div className="author">
              {author && (
                <>
                  <Avatar initials={nameToInitials(author ?? "")} />
                  <span className="author__name">{author}</span>
                </>
              )}
            </div>
          ),
        },
        commitID: {
          node: () => (
            <div className="commit-id">
              <dremio-icon name="vcs/commit" class="commit-id__icon" />
              <span className="commit-id__id">
                {getShortHash(tag.hash || "")}
              </span>
              <span>
                <CopyButton
                  text={tag.hash || ""}
                  title={
                    <FormattedMessage id="VersionedEntity.Commits.CommitID.Copy" />
                  }
                />
              </span>
            </div>
          ),
        },
        commitMessage: {
          node: () => (
            <div className="commit-message">{commitData?.message}</div>
          ),
        },
        commitTime: {
          node: () => (
            <div className="commit-time">
              <span className="commit-time__time">
                {commitData?.commitTime
                  ? convertISOStringWithTooltip(
                      commitData.commitTime.toString(),
                      { isRelative: true },
                    )
                  : ""}
              </span>
              <span className="commit-time__buttons">
                <IconButton
                  tooltip="VersionedEntity.Commits.GoToData"
                  onClick={() => {
                    goToDataTab("data", tag);
                  }}
                  className="commit-time__buttons--data"
                >
                  <dremio-icon name="interface/goto-dataset" />
                </IconButton>
                <SettingsBtn
                  classStr="commit-time__buttons--more"
                  menu={
                    <VersionedGitActionsMenu
                      fromItem={tag}
                      handleOpenDialog={handleOpenDialog}
                      canDeleteTag
                    />
                  }
                  hideArrowIcon
                  stopPropagation
                >
                  <dremio-icon name="interface/more" class="more-icon" />
                </SettingsBtn>
              </span>
            </div>
          ),
        },
      },
    });
  });

  return tableData;
};
