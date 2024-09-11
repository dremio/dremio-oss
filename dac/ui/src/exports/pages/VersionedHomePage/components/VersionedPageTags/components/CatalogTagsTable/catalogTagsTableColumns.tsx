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

import { type Column } from "leantable/react";
import {
  Skeleton,
  Avatar,
  IconButton,
  CopyButton,
} from "dremio-ui-lib/components";
import { nameToInitials } from "@app/exports/utilities/nameToInitials";
import { getIntlContext } from "dremio-ui-common/contexts/IntlContext.js";
import { SortableHeaderCell } from "dremio-ui-common/components/TableCells/SortableHeaderCell.js";
import { convertISOStringWithTooltip } from "@app/pages/NessieHomePage/components/RepoView/components/RepoViewBody/components/RepoViewBranchList/utils";
import VersionedGitActionsMenu from "@app/exports/pages/VersionedHomePage/components/VersionedGitActionsMenu/VersionedGitActionsMenu";
import { getShortHash } from "@app/utils/nessieUtils";
import { Tag } from "@app/services/nessie/client";
import { intl } from "@app/utils/intl";
import SettingsPopover from "@app/components/Buttons/SettingsPopover";

export enum TAGS_COLUMNS {
  tagName = "tagName",
  author = "author",
  commitID = "commitID",
  commitMessage = "commitMessage",
  commitTime = "commitTime",
}

export const getTagsColumnLabels = () => {
  const { t } = getIntlContext();
  return {
    [TAGS_COLUMNS.tagName]: t("History.Catalog.TagName"),
    [TAGS_COLUMNS.author]: t("History.Catalog.Author"),
    [TAGS_COLUMNS.commitID]: t("History.Catalog.CommitID"),
    [TAGS_COLUMNS.commitMessage]: t("History.Catalog.CommitMessage"),
    [TAGS_COLUMNS.commitTime]: t("History.Catalog.CommitTime"),
  };
};

export const catalogTagsTableColumns = (
  goToDataTab: (
    tab:
      | "data"
      | "commits"
      | "tags"
      | "branches"
      | "settings"
      | "commit"
      | "jobs",
    tag: { type: "TAG" } & Tag,
  ) => void,
  handleOpenDialog: (dialogType: "TAG" | "BRANCH", dialogState: any) => void,
): Column<any>[] => {
  const catalogColumnLabels = getTagsColumnLabels();
  const { t } = getIntlContext();
  return [
    {
      class: "leantable-sticky-column leantable-sticky-column--left",
      id: TAGS_COLUMNS.tagName,
      renderHeaderCell: () => catalogColumnLabels[TAGS_COLUMNS.tagName],
      renderCell: (row: any) => {
        if (!row.data) {
          return <Skeleton width="20ch" />;
        }
        return (
          <div
            className="tag-name"
            style={{ maxWidth: "20ch", width: "max-content" }}
          >
            <dremio-icon name="vcs/tag" class="tag-name__icon" />
            <span className="tag-name__name">{row.data?.name}</span>
          </div>
        );
      },
    },
    {
      id: TAGS_COLUMNS.author,
      renderHeaderCell: () => catalogColumnLabels[TAGS_COLUMNS.author],
      renderCell: (row: any) => {
        if (!row.data) {
          return <Skeleton width="20ch" />;
        }
        const author = row.data.metadata?.commitMetaOfHEAD?.authors?.[0];
        return (
          <div
            className="author"
            style={{ width: "max-content", maxWidth: "40ch" }}
          >
            {author && (
              <>
                <Avatar initials={nameToInitials(author ?? "")} />
                <span className="author__name">{author}</span>
              </>
            )}
          </div>
        );
      },
    },
    {
      id: TAGS_COLUMNS.commitID,
      renderHeaderCell: () => catalogColumnLabels[TAGS_COLUMNS.commitID],
      renderCell: (row: any) => {
        if (!row.data) {
          return <Skeleton width="20ch" />;
        }
        return (
          <div className="commit-id">
            <dremio-icon name="vcs/commit" class="commit-id__icon" />
            <span className="commit-id__id">
              {getShortHash(row.data.hash || "")}
            </span>
            <span>
              <CopyButton
                contents={row.data.hash || ""}
                copyTooltipLabel={intl.formatMessage({
                  id: "VersionedEntity.Commits.CommitID.Copy",
                })}
                size="L"
                className="copy-button"
              />
            </span>
          </div>
        );
      },
    },

    {
      id: TAGS_COLUMNS.commitMessage,
      renderHeaderCell: () => catalogColumnLabels[TAGS_COLUMNS.commitMessage],
      renderCell: (row: any) => {
        if (!row.data) {
          return <Skeleton width="20ch" />;
        }
        const message = row.data.metadata?.commitMetaOfHEAD?.message;
        return (
          <div
            className="commit-message"
            style={{
              width: "max-content",
              maxWidth: "50ch",
              padding: "4px 0",
            }}
          >
            <div
              className="text-ellipsis-2"
              style={{
                whiteSpace: "normal",
                lineHeight: "17px",
                maxHeight: 50,
              }}
            >
              {message}
            </div>
          </div>
        );
      },
    },
    {
      id: TAGS_COLUMNS.commitTime,
      renderHeaderCell: () => (
        <SortableHeaderCell columnId={TAGS_COLUMNS.commitTime}>
          {catalogColumnLabels[TAGS_COLUMNS.commitTime]}
        </SortableHeaderCell>
      ),
      renderCell: (row: any) => {
        if (!row.data) {
          return <Skeleton width="20ch" />;
        }
        const commitTime = row.data.metadata?.commitMetaOfHEAD?.commitTime;
        return (
          <div className="commit-time">
            <span className="commit-time__time">
              {convertISOStringWithTooltip(commitTime.toString() ?? "", {
                isRelative: true,
              })}
            </span>
          </div>
        );
      },
      sortable: true,
    },
    {
      id: "actions",
      renderHeaderCell: () => " ",
      class:
        "leantable-row-hover-visibility leantable-sticky-column leantable-sticky-column--right leantable--align-right",
      renderCell: (row: any) => {
        if (!row.data) {
          return <Skeleton width="12ch" />;
        }
        return (
          <span className="commit-time__buttons" id={row.data.id}>
            <IconButton
              tooltip={t("History.Catalog.GoToData")}
              onClick={() => {
                goToDataTab("data", row.data);
              }}
              className="commit-time__buttons--data"
            >
              <dremio-icon name="interface/goto-dataset" />
            </IconButton>
            <SettingsPopover
              content={
                <VersionedGitActionsMenu
                  fromItem={row.data}
                  handleOpenDialog={handleOpenDialog}
                  canDeleteTag
                />
              }
              hideArrowIcon
              onOpen={() => {
                const el = document
                  .getElementById(row.data.id)
                  ?.closest(".leantable-sticky-column");
                (el as HTMLElement).style.zIndex = "201";
              }}
              onClose={() => {
                const el = document
                  .getElementById(row.data.id)
                  ?.closest(".leantable-sticky-column");
                (el as HTMLElement).style.zIndex = "200";
              }}
            >
              <dremio-icon name="interface/more" class="more-icon" />
            </SettingsPopover>
          </span>
        );
      },
    },
  ];
};
