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
import { Avatar } from "dremio-ui-lib/dist-esm";
import { LogEntry } from "@app/services/nessie/client";
import { DEFAULT_FORMAT_WITH_TIME_SECONDS, formatDate } from "@app/utils/date";
import { nameToInitials } from "@app/exports/utilities/nameToInitials";

export const getCommitsTableColumns = () => {
  return [
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

export const generateTableRows = (data: LogEntry[]) => {
  const tableData: any[] = [];

  data.forEach((entry) => {
    const commitData = entry?.commitMeta;

    tableData.push({
      id: commitData?.hash,
      rowClassName: commitData?.hash,
      data: {
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
                {commitData?.hash?.substring(0, 6)}
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
              {formatDate(
                commitData?.commitTime + "",
                DEFAULT_FORMAT_WITH_TIME_SECONDS
              )}
            </div>
          ),
        },
      },
    });
  });

  return tableData;
};
