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
import { LogEntry } from "@app/services/nessie/client";
import { isNotSoftware } from "dyn-load/utils/versionUtils";
import VersionedGitActionsMenu from "@app/exports/pages/VersionedHomePage/components/VersionedGitActionsMenu/VersionedGitActionsMenu";
import { getShortHash } from "@app/utils/nessieUtils";
import { Link } from "react-router";
import { intl } from "@app/utils/intl";
import SettingsPopover from "@app/components/Buttons/SettingsPopover";

export enum COMMITS_COLUMNS {
  author = "author",
  commitID = "commitID",
  commitMessage = "commitMessage",
  commitTime = "commitTime",
}

export const getCommitsColumnLabels = () => {
  const { t } = getIntlContext();
  return {
    [COMMITS_COLUMNS.author]: t("History.Catalog.Author"),
    [COMMITS_COLUMNS.commitID]: t("History.Catalog.CommitID"),
    [COMMITS_COLUMNS.commitMessage]: t("History.Catalog.CommitMessage"),
    [COMMITS_COLUMNS.commitTime]: t("History.Catalog.CommitTime"),
  };
};

export const catalogCommitsTableColumns = (
  getCommitLink: (id: string) => string,
  goToDataTab: (
    tab:
      | "data"
      | "commits"
      | "tags"
      | "branches"
      | "settings"
      | "commit"
      | "jobs",
    item: LogEntry,
  ) => void,
  handleOpenDialog: (type: "TAG" | "BRANCH", dialogState: any) => void,
  reference: any,
  privileges: Record<string, any> | null,
): Column<any>[] => {
  const catalogColumnLabels = getCommitsColumnLabels();
  const { t } = getIntlContext();
  return [
    {
      class: "leantable-sticky-column leantable-sticky-column--left",
      id: COMMITS_COLUMNS.commitID,
      renderHeaderCell: () => catalogColumnLabels[COMMITS_COLUMNS.commitID],
      renderCell: (row: any) => {
        if (!row.data) {
          return <Skeleton width="20ch" />;
        }
        return (
          <div
            className="commit-id"
            style={{ maxWidth: "20ch", width: "max-content" }}
          >
            <dremio-icon name="vcs/commit" class="commit-id__icon" />
            <Link
              className="commit-id__id"
              to={{ pathname: getCommitLink(row.data.commitMeta?.hash) }}
            >
              {getShortHash(row.data.commitMeta?.hash || "")}
            </Link>
            <span>
              <CopyButton
                contents={row.data.commitMeta.hash || ""}
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
      id: COMMITS_COLUMNS.author,
      renderHeaderCell: () => catalogColumnLabels[COMMITS_COLUMNS.author],
      renderCell: (row: any) => {
        if (!row.data) {
          return <Skeleton width="20ch" />;
        }
        const author = row.data.commitMeta?.authors?.[0];
        return (
          <div
            className="author"
            style={{ width: "max-content", maxWidth: "40ch" }}
          >
            <Avatar initials={nameToInitials(author ?? "")} />
            <span className="author__name">{author}</span>
          </div>
        );
      },
    },
    {
      id: COMMITS_COLUMNS.commitMessage,
      renderHeaderCell: () =>
        catalogColumnLabels[COMMITS_COLUMNS.commitMessage],
      renderCell: (row: any) => {
        if (!row.data) {
          return <Skeleton width="20ch" />;
        }
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
              {row.data.commitMeta?.message}
            </div>
          </div>
        );
      },
    },
    {
      id: COMMITS_COLUMNS.commitTime,
      renderHeaderCell: () => (
        <SortableHeaderCell columnId={COMMITS_COLUMNS.commitTime}>
          {catalogColumnLabels[COMMITS_COLUMNS.commitTime]}
        </SortableHeaderCell>
      ),
      renderCell: (row: any) => {
        if (!row.data) {
          return <Skeleton width="20ch" />;
        }
        return (
          <div className="commit-time">
            <span className="commit-time__time">
              {convertISOStringWithTooltip(
                row.data.commitMeta?.commitTime?.toString() ?? "",
                { isRelative: true },
              )}
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
          <span
            className="commit-time__buttons"
            id={row.data?.commitMeta?.hash}
          >
            <IconButton
              tooltip={t("History.Catalog.GoToData")}
              onClick={(e: any) => {
                e.stopPropagation();
                goToDataTab("data", row.data);
              }}
              className="commit-time__buttons--data"
            >
              <dremio-icon name="interface/goto-dataset" alt="" />
            </IconButton>
            {(!isNotSoftware?.() ||
              privileges?.branch.canCreate ||
              privileges?.tag.canCreate) && (
              <SettingsPopover
                content={
                  <VersionedGitActionsMenu
                    fromItem={{
                      type: "BRANCH",
                      name: reference?.name,
                      hash: row.data.commitMeta?.hash,
                    }}
                    handleOpenDialog={handleOpenDialog}
                  />
                }
                hideArrowIcon
                onOpen={() => {
                  const el = document
                    .getElementById(row.data?.commitMeta?.hash)
                    ?.closest(".leantable-sticky-column");
                  (el as HTMLElement).style.zIndex = "201";
                }}
                onClose={() => {
                  const el = document
                    .getElementById(row.data?.commitMeta?.hash)
                    ?.closest(".leantable-sticky-column");
                  (el as HTMLElement).style.zIndex = "200";
                }}
              >
                <dremio-icon name="interface/more" class="more-icon" />
              </SettingsPopover>
            )}
          </span>
        );
      },
    },
  ];
};
