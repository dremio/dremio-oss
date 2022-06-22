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
import { useIntl } from "react-intl";
import { CommitMeta, LogEntry } from "@app/services/nessie/client";
import {
  DEFAULT_FORMAT_WITH_TIME,
  formatDate,
  formatDateSince,
} from "@app/utils/date";
import { Tooltip } from "@material-ui/core";
import classNames from "classnames";
import { useRef } from "react";

import CommitHash from "../CommitHash/CommitHash";
import UserIcon from "../UserIcon/UserIcon";
import "./CommitEntry.less";

function CommitEntryTooltip({
  commit,
  anchorRef,
  created,
  children,
  branch,
}: {
  created: string;
  anchorRef: any;
  commit: CommitMeta;
  children: any;
  branch: string;
}) {
  const intl = useIntl();
  return (
    <Tooltip
      interactive
      enterDelay={750}
      enterNextDelay={500}
      placement="right"
      PopperProps={{ anchorEl: anchorRef.current }}
      title={
        <div className="commitEntryTooltip">
          {commit.hash && <CommitHash branch={branch} hash={commit.hash} />}
          <table>
            <tbody>
              <tr>
                <td>{intl.formatMessage({ id: "Common.Description" })}:</td>
                <td>{commit.message}</td>
              </tr>
              <tr>
                <td>{intl.formatMessage({ id: "Common.Author" })}:</td>
                <td>{commit.author}</td>
              </tr>
              <tr>
                <td>{intl.formatMessage({ id: "Common.Created" })}:</td>
                <td>{created}</td>
              </tr>
            </tbody>
          </table>
        </div>
      }
    >
      {children}
    </Tooltip>
  );
}

function CommitEntry({
  logEntry,
  onClick = (_) => {},
  isSelected,
  branch,
  disabled,
}: {
  logEntry: LogEntry;
  onClick?: (arg: LogEntry) => void;
  isSelected?: boolean;
  branch: string;
  disabled?: boolean;
}) {
  const commit = logEntry.commitMeta!;
  const user = commit.author || "";
  const ref = useRef(null);

  return (
    <div className={classNames("commitEntry", { isSelected, disabled })}>
      {!disabled && (
        <CommitEntryTooltip
          commit={commit}
          created={formatDate(commit.commitTime + "", DEFAULT_FORMAT_WITH_TIME)}
          anchorRef={ref}
          branch={branch}
        >
          <button
            className="commitEntry-select"
            onClick={(_) => onClick(logEntry)}
          />
        </CommitEntryTooltip>
      )}
      <div className="commitEntry-desc text-ellipsis">{commit.message}</div>
      <div className="commitEntry-content" ref={ref}>
        <span className="commitEntry-userInfo">
          {user && <UserIcon user={user} />}
          <span className="commitEntry-userName text-ellipsis">{user}</span>
        </span>
        {commit.hash && (
          <CommitHash disabled={disabled} hash={commit.hash} branch={branch} />
        )}
        <span className="commitEntry-time text-ellipsis">
          {formatDateSince(commit.commitTime + "")}
        </span>
      </div>
    </div>
  );
}
export default CommitEntry;
