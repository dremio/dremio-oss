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
import TagsActionsMenu from "./components/TagsActionsMenu/TagsActionsMenu";
import SettingsBtn from "@app/components/Buttons/SettingsBtn";
// @ts-ignore
import { IconButton } from "dremio-ui-lib";
import { Avatar } from "dremio-ui-lib/dist-esm";
import { ArcticCatalogTabsType } from "@app/exports/pages/ArcticCatalog/ArcticCatalog";
import { DEFAULT_FORMAT_WITH_TIME_SECONDS, formatDate } from "@app/utils/date";
import { Tag } from "@app/services/nessie/client";
import { nameToInitials } from "@app/exports/utilities/nameToInitials";

export const getTagsTableColumns = () => {
  return [
    {
      key: "tagName",
      label: <FormattedMessage id="ArcticCatalog.Tags.TagName" />,
      disableSort: true,
    },
    {
      key: "author",
      label: <FormattedMessage id="Common.Author" />,
      disableSort: true,
    },
    {
      key: "commitID",
      label: <FormattedMessage id="ArcticCatalog.Commits.CommitID" />,
      disableSort: true,
    },
    {
      key: "commitMessage",
      label: <FormattedMessage id="ArcticCatalog.Commits.CommitMessage" />,
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
  handleTabNavigation: (
    tag: { type: "TAG" } & Tag,
    tab: ArcticCatalogTabsType
  ) => void,
  setDialogState: React.Dispatch<React.SetStateAction<any>>
) => {
  const tableData: any[] = [];

  data.forEach((tag) => {
    const commitData = tag?.metadata?.commitMetaOfHEAD;

    tableData.push({
      id: commitData?.hash,
      rowClassName: commitData?.hash,
      data: {
        tagName: {
          node: () => (
            <div className="tag-name">
              <dremio-icon name="vcs/tag" class="tag-name__icon" />
              <span className="tag-name__name">{tag?.name}</span>
            </div>
          ),
        },
        author: {
          node: () => (
            <div className="author">
              <Avatar initials={nameToInitials(commitData?.author ?? "")} />
              <span className="author__name">{commitData?.author}</span>
            </div>
          ),
        },
        commitID: {
          node: () => (
            <div className="commit-id">
              <dremio-icon name="vcs/commit" class="commit-id__icon" />
              <span className="commit-id__id">
                {commitData?.hash?.slice(-6)}
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
                {formatDate(
                  commitData?.commitTime + "",
                  DEFAULT_FORMAT_WITH_TIME_SECONDS
                )}
              </span>
              <span className="commit-time__buttons">
                <IconButton
                  tooltip="ArcticCatalog.Commits.GoToData"
                  onClick={() => {
                    handleTabNavigation(tag, "data");
                  }}
                  className="commit-time__buttons--data"
                >
                  <dremio-icon name="interface/goto-dataset" />
                </IconButton>
                <SettingsBtn
                  classStr="commit-time__buttons--more"
                  menu={
                    <TagsActionsMenu
                      tag={tag}
                      setDialogState={setDialogState}
                    />
                  }
                  tooltip="Common.More"
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
