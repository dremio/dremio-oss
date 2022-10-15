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

import { useMemo } from "react";
import StatefulTableViewer from "@app/components/StatefulTableViewer";
import { LogEntry } from "@app/services/nessie/client";
import { generateTableRows, getCommitsTableColumns } from "./utils";

import "./ArcticCatalogCommitsTable.less";

type ArcticCatalogCommitsTableProps = {
  commits: LogEntry[];
  onRowClick: (rowId: Record<string, any>) => void;
  loadNextPage: (index: number) => void;
};

function ArcticCatalogCommitsTable({
  commits,
  onRowClick,
  loadNextPage,
}: ArcticCatalogCommitsTableProps) {
  const tableData = useMemo(() => {
    return generateTableRows(commits);
  }, [commits]);

  return (
    <StatefulTableViewer
      virtualized
      disableZebraStripes
      columns={getCommitsTableColumns()}
      tableData={tableData}
      rowHeight={40}
      onClick={onRowClick}
      className="arctic-commits-table"
      loadNextRecords={loadNextPage}
    />
  );
}

export default ArcticCatalogCommitsTable;
