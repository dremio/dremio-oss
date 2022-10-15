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
import debounce from "lodash/debounce";
import classNames from "classnames";
import { SearchField } from "@app/components/Fields";
import {
  Branch,
  DefaultApi,
  FetchOption,
  LogEntry,
  LogResponse,
} from "@app/services/nessie/client";
import { useCallback, useEffect, useMemo, useReducer } from "react";
import InfiniteScroller from "../InfiniteScroller/InfiniteScroller";
import CommitEntry from "./components/CommitEntry/CommitEntry";
import { CommitBrowserReducer, formatQuery, initialState } from "./utils";

import "./CommitBrowser.less";

const LIST_ITEM_HEIGHT = 64;
const PAGE_SIZE = 100;

function CommitBrowser({
  hasSearch = true,
  branch,
  path,
  onClick,
  onDataChange,
  selectedHash,
  disabled,
  pageSize = PAGE_SIZE,
  api,
}: {
  branch: Branch;
  path?: string[];
  hasSearch?: boolean;
  onDataChange?: (arg: LogResponse | undefined) => void;
  onClick?: (arg: LogEntry) => void;
  selectedHash?: string | null;
  disabled?: boolean;
  pageSize?: number;
  api: DefaultApi;
}) {
  const [{ search, data, numRows }, dispatch] = useReducer(
    CommitBrowserReducer,
    initialState
  );

  useEffect(() => {
    if (onDataChange && data) onDataChange(data);
  }, [data, onDataChange]);

  const onSearch = useMemo(
    () =>
      debounce(function (value) {
        dispatch({ type: "SET_SEARCH", value });
      }, 250),
    []
  );

  const { logEntries, token } = useMemo(
    () => (!data ? { logEntries: [], token: undefined } : data),
    [data]
  );

  const loadMoreRows = useCallback(
    async function () {
      const value = await api.getCommitLog({
        ref: branch.name,
        pageToken: token,
        maxRecords: pageSize,
        filter: formatQuery(search, path),
        fetch: !path?.length ? FetchOption.Minimal : FetchOption.All,
      });
      dispatch({ type: "SET_DATA", value });
    },
    [branch.name, token, search, path, pageSize, api]
  );

  return (
    <div className={classNames("commitBrowser", { disabled })}>
      <div className="commitBrowser-content">
        {hasSearch && (
          <div className="commitBrowser-search">
            <SearchField
              showIcon
              disabled={disabled}
              onChange={onSearch}
              placeholder={"Search"}
            />
          </div>
        )}
        <div className="commitBrowser-listWrapper">
          <InfiniteScroller
            key={search} // Reset when search changes
            rowHeight={LIST_ITEM_HEIGHT}
            data={logEntries}
            loadData={loadMoreRows}
            numRows={numRows}
          >
            {(index) => {
              const curEntry = logEntries[index];
              const curHash = curEntry?.commitMeta?.hash;
              const isSelected = !!selectedHash && selectedHash === curHash;
              return (
                <CommitEntry
                  logEntry={logEntries[index]}
                  onClick={onClick}
                  isSelected={isSelected}
                  branch={branch.name}
                  disabled={disabled}
                />
              );
            }}
          </InfiniteScroller>
        </div>
      </div>
    </div>
  );
}

export default CommitBrowser;
