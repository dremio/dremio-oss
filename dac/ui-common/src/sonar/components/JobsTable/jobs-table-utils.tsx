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

import { getIntlContext } from "../../../contexts/IntlContext";
// @ts-ignore
import { Tooltip } from "dremio-ui-lib/components";

enum QueryEngineType {
  UI_RUN = "UI_RUN",
  UI_PREVIEW = "UI_PREVIEW",
  UI_INTERNAL_PREVIEW = "UI_INTERNAL_PREVIEW",
  UI_INTERNAL_RUN = "UI_INTERNAL_RUN",
  UI_INITIAL_PREVIEW = "UI_INITIAL_PREVIEW",
  PREPARE_INTERNAL = "PREPARE_INTERNAL",
  UI_EXPORT = "UI_EXPORT",
  ODBC = "ODBC",
  D2D = "D2D",
  JDBC = "JDBC",
  REST = "REST",
  ACCELERATOR_CREATE = "ACCELERATOR_CREATE",
  ACCELERATOR_EXPLAIN = "ACCELERATOR_EXPLAIN",
  ACCELERATOR_DROP = "ACCELERATOR_DROP",
  ACCELERATOR_OPTIMIZE = "ACCELERATOR_OPTIMIZE",
  FLIGHT = "FLIGHT",
  METADATA_REFRESH = "METADATA_REFRESH",
  INTERNAL_ICEBERG_METADATA_DROP = "INTERNAL_ICEBERG_METADATA_DROP",
  UNKNOWN = "UNKNOWN",
}

enum QueryRequestType {
  REQUEST_TYPE_UNSPECIFIED = "REQUEST_TYPE_UNSPECIFIED",
  GET_CATALOGS = "GET_CATALOGS",
  GET_COLUMNS = "GET_COLUMNS",
  GET_SCHEMAS = "GET_SCHEMAS",
  GET_TABLES = "GET_TABLES",
  CREATE_PREPARE = "CREATE_PREPARE",
  EXECUTE_PREPARE = "EXECUTE_PREPARE",
  RUN_SQL = "RUN_SQL",
  GET_SERVER_META = "GET_SERVER_META",
}

const ellipsisStyles: any = {
  whiteSpace: "nowrap",
  overflow: "hidden",
  textOverflow: "ellipsis",
};

export const TableCellWithTooltip = ({
  tooltipContent,
  styles = {},
}: {
  tooltipContent: string | React.ReactElement;
  styles?: any;
}) => {
  return (
    <Tooltip
      portal
      content={
        <div
          className="dremio-prose"
          style={{ width: "max-content", maxWidth: "40ch" }}
        >
          {tooltipContent}
        </div>
      }
    >
      <div
        style={{
          ...styles,
          ...ellipsisStyles,
        }}
      >
        {tooltipContent}
      </div>
    </Tooltip>
  );
};

export const getQueryTypeLabel = (job: any) => {
  const { t } = getIntlContext();
  const requestType = job.requestType;
  const isPrepareCreate = requestType === QueryRequestType.CREATE_PREPARE;
  const isPrepareExecute = requestType === QueryRequestType.EXECUTE_PREPARE;

  switch (job.queryType) {
    case QueryEngineType.UI_RUN:
      return t("Sonar.Job.QueryType.UIRun");
    case QueryEngineType.UI_PREVIEW:
      return t("Sonar.Job.QueryType.UIPreview");
    case QueryEngineType.UI_INTERNAL_PREVIEW:
    case QueryEngineType.UI_INTERNAL_RUN:
    case QueryEngineType.UI_INITIAL_PREVIEW:
    case QueryEngineType.PREPARE_INTERNAL:
      return t("Sonar.Job.QueryType.Internal");
    case QueryEngineType.UI_EXPORT:
      return t("Sonar.Job.QueryType.UIDownload");
    case QueryEngineType.ODBC:
      if (isPrepareCreate) {
        return t("Sonar.Job.QueryType.ODBCCreate");
      } else if (isPrepareExecute) {
        return t("Sonar.Job.QueryType.ODBCExecute");
      }
      return t("Sonar.Job.QueryType.ODBCClient");
    case QueryEngineType.D2D:
      return t("Sonar.Job.QueryType.D2DClient");
    case QueryEngineType.JDBC:
      if (isPrepareCreate) {
        return t("Sonar.Job.QueryType.JDBCCreate");
      } else if (isPrepareExecute) {
        return t("Sonar.Job.QueryType.JDBCExecute");
      }
      return t("Sonar.Job.QueryType.JDBCClient");
    case QueryEngineType.REST:
      return t("Sonar.Job.QueryType.RESTApp");
    case QueryEngineType.ACCELERATOR_CREATE:
      return t("Sonar.Job.QueryType.AcceleratorCreation");
    case QueryEngineType.ACCELERATOR_EXPLAIN:
      return t("Sonar.Job.QueryType.AcceleratorRefresh");
    case QueryEngineType.ACCELERATOR_DROP:
      return t("Sonar.Job.QueryType.AcceleratorRemoval");
    case QueryEngineType.ACCELERATOR_OPTIMIZE:
      return t("Sonar.Job.QueryType.AcceleratorOptimize");
    case QueryEngineType.FLIGHT:
      if (isPrepareCreate) {
        return t("Sonar.Job.QueryType.FlightCreate");
      } else if (isPrepareExecute) {
        return t("Sonar.Job.QueryType.FlightExecute");
      }
      return t("Sonar.Job.QueryType.FlightClient");
    case QueryEngineType.METADATA_REFRESH:
      return t("Sonar.Job.QueryType.MetadataRefresh");
    case QueryEngineType.INTERNAL_ICEBERG_METADATA_DROP:
      return t("Sonar.Job.QueryType.InternalIcebergMetadataRemoval");
    case QueryEngineType.UNKNOWN:
    default:
      return t("Sonar.File.Unknown");
  }
};
