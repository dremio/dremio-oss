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

import * as React from "react";
import { FormattedMessage } from "react-intl";
import { Avatar } from "@mui/material";
import CommitHash from "@app/pages/HomePage/components/BranchPicker/components/CommitBrowser/components/CommitHash/CommitHash";
import { DefaultApi, LogEntry, LogResponse } from "@app/services/nessie/client";
import { convertISOStringWithTooltip } from "../../../RepoView/components/RepoViewBody/components/RepoViewBranchList/utils";
import { Reference } from "@app/types/nessie";

export const columns = [
  {
    key: "name",
    label: <FormattedMessage id="Common.Author" />,
    width: 200,
  },
  {
    key: "commit",
    label: <FormattedMessage id="HomePageHeader.CommitBtn" />,
  },
  {
    key: "description",
    label: <FormattedMessage id="BranchHistory.Commits.DescriptionTags" />,
  },
  {
    key: "commitTime",
    label: <FormattedMessage id="Common.CommitTime" />,
  },
];

const stringAvatar = (name: string | undefined) => {
  if (!name) return { children: null };
  const splitName = name.split(" ");
  return {
    children:
      splitName.length > 1
        ? `${splitName[0][0].toUpperCase()}${splitName[1][0].toUpperCase()}`
        : `${splitName[0][0].toUpperCase()}`,
  };
};

const handleLoadMoreCommits = async (
  branchName: string,
  commitLog: LogResponse,
  setCommitLog: React.Dispatch<React.SetStateAction<LogResponse>>,
  api: DefaultApi
) => {
  try {
    const moreCommits = await api.getCommitLog({
      ref: branchName,
      maxRecords: 100,
      pageToken: commitLog.token,
    });

    setCommitLog({
      ...commitLog,
      logEntries: [...commitLog.logEntries, ...moreCommits.logEntries],
      token: moreCommits.token,
    });
  } catch (error) {
    // TODO error handling
  }
};

const createTableRow = (
  index: number,
  branch: Reference,
  entry: LogEntry,
  api: DefaultApi,
  commitLog?: LogResponse,
  setCommitLog?: React.Dispatch<React.SetStateAction<LogResponse>>
) => {
  const commitHash = entry.commitMeta ? entry.commitMeta.hash : "";

  return commitLog && commitLog.token && setCommitLog
    ? {
        id: index,
        rowClassName: "row" + index,
        data: {
          name: { node: () => <></> },
          commit: { node: () => <></> },
          description: {
            node: () => (
              <a
                href="#"
                className="load-more"
                onClick={(e) => {
                  e.preventDefault();
                  handleLoadMoreCommits(
                    branch.name,
                    commitLog,
                    setCommitLog,
                    api
                  );
                }}
              >
                <FormattedMessage id="BranchHistory.Commits.LoadMore" />
              </a>
            ),
          },
          commitTime: { node: () => <></> },
        },
      }
    : {
        id: index,
        rowClassName: "row" + index,
        data: {
          name: {
            node: () =>
              entry.commitMeta && (
                <div className="commit-author">
                  <span className="commit-author-avatar">
                    <Avatar {...stringAvatar(entry.commitMeta.author)} />
                  </span>
                  <span className="commit-author-username">
                    {entry.commitMeta.author}
                  </span>
                </div>
              ),
          },
          commit: {
            node: () => (
              <div className="commit-hash">
                {commitHash && <CommitHash branch={branch} hash={commitHash} />}
              </div>
            ),
          },
          description: {
            node: () => (
              <div className="commit-message">
                {entry.commitMeta && entry.commitMeta.message}
              </div>
            ),
          },
          commitTime: {
            node: () => (
              <div className="commit-timestamp">
                {entry.commitMeta &&
                  entry.commitMeta.commitTime &&
                  convertISOStringWithTooltip(
                    entry?.commitMeta?.commitTime?.toString() ?? ""
                  )}
              </div>
            ),
          },
        },
      };
};

export const createTableData = (
  branch: Reference,
  commitLog: LogResponse,
  setCommitLog: React.Dispatch<React.SetStateAction<LogResponse>>,
  api: DefaultApi
) => {
  const rows: any[] = [];

  if (commitLog.logEntries) {
    for (const [i] of commitLog.logEntries.entries()) {
      rows.push(createTableRow(i, branch, commitLog.logEntries[i], api));
    }

    if (commitLog.token) {
      rows.push(
        createTableRow(
          commitLog.logEntries.length,
          branch,
          {} as LogEntry,
          api,
          commitLog,
          setCommitLog
        )
      );
    }
  }

  return rows;
};
