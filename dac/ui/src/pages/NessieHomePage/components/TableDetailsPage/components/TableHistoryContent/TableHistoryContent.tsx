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
import CommitBrowser from "@app/pages/HomePage/components/BranchPicker/components/CommitBrowser/CommitBrowser";
import {
  LogEntryV2 as LogEntry,
  LogResponseV2 as LogResponse,
} from "@app/services/nessie/client";
import { useCallback, useEffect, useRef, useState } from "react";
import { useNessieContext } from "../../../../utils/context";
import CommitDetails from "../../../CommitDetails/CommitDetails";

import "./TableHistoryContent.less";

function TableHistoryContent({
  path,
  tableName,
}: {
  path: string[];
  tableName?: string;
}) {
  const {
    state: { reference },
    apiV2,
  } = useNessieContext();
  const [commit, setCommit] = useState<LogEntry>();
  const [list, setList] = useState<LogResponse | undefined>();
  const count = useRef(0);

  useEffect(() => {
    const entries = list?.logEntries || [];
    if (count.current !== 1 || !entries.length) return;
    setCommit(entries[0]); // Set current commit to first in list on load
  }, [list]);

  const onDataChange = useCallback(function (arg: LogResponse | undefined) {
    count.current++;
    setList(arg);
  }, []);

  const commitMeta = commit?.commitMeta;

  return (
    <div className="tableDetailsPage-content">
      <span className="tableDetailsPage-commits">
        {!!reference && (
          <CommitBrowser
            tableName={tableName}
            pageSize={25}
            path={path}
            hasSearch={false}
            branch={reference}
            onDataChange={onDataChange}
            selectedHash={commitMeta?.hash}
            onClick={setCommit}
            api={apiV2}
          />
        )}
      </span>
      {commitMeta && reference && (
        <CommitDetails branch={reference} commitMeta={commitMeta} />
      )}
    </div>
  );
}

export default TableHistoryContent;
