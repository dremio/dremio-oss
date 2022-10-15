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
import { parseNamespaceUrl } from "@app/utils/nessieUtils";
import { useMemo } from "react";
import { WithRouterProps } from "react-router";
import PageBreadcrumbHeader from "../PageBreadcrumbHeader/PageBreadcrumbHeader";
import TableHistoryContent from "./components/TableHistoryContent/TableHistoryContent";
import TableHistoryHeader from "./components/TableHistoryHeader/TableHistoryHeader";

import "./TableDetailsPage.less";

function TableDetailsPage({
  location,
}: {
  location: WithRouterProps["location"];
}) {
  const isTable = location.pathname.startsWith("/table/");
  const [path, namespace, tableName] = useMemo(() => {
    const split =
      parseNamespaceUrl(location.pathname, isTable ? "table" : "view") || [];
    const tName = split.pop() || "";
    return [split, split.map((c) => decodeURIComponent(c)).join("."), tName];
  }, [location, isTable]);

  return (
    <div className="tableDetailsPage">
      <PageBreadcrumbHeader />
      <TableHistoryHeader
        type={isTable ? "ICEBERG_TABLE" : "ICEBERG_VIEW"}
        namespace={namespace}
        tableName={tableName}
      />
      <TableHistoryContent path={path} />
    </div>
  );
}

export default TableDetailsPage;
