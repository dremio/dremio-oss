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

export const getFormatMessageIdForQueryType = (jobDetails) => {
  const requestType = jobDetails.get("requestType");
  const isPrepareCreate = requestType === "CREATE_PREPARE";
  const isPrepareExecute = requestType === "EXECUTE_PREPARE";

  switch (jobDetails.get("queryType")) {
    case "UI_RUN":
      return "Job.UIRun";
    case "UI_PREVIEW":
      return "Job.UIPreview";
    case "UI_INTERNAL_PREVIEW":
    case "UI_INTERNAL_RUN":
    case "UI_INITIAL_PREVIEW":
    case "PREPARE_INTERNAL":
      return "Job.Internal";
    case "UI_EXPORT":
      return "Job.UIDownload";
    case "ODBC":
      if (isPrepareCreate) {
        return "Job.ODBCCreate";
      } else if (isPrepareExecute) {
        return "Job.ODBCExecute";
      }
      return "Job.ODBCClient";
    case "JDBC":
      if (isPrepareCreate) {
        return "Job.JDBCCreate";
      } else if (isPrepareExecute) {
        return "Job.JDBCExecute";
      }
      return "Job.JDBCClient";
    case "REST":
      return "Job.RESTApp";
    case "ACCELERATOR_CREATE":
      return "Job.AcceleratorCreation";
    case "ACCELERATOR_EXPLAIN":
      return "Job.AcceleratorRefresh";
    case "ACCELERATOR_DROP":
      return "Job.AcceleratorRemoval";
    case "FLIGHT":
      if (isPrepareCreate) {
        return "Job.FlightCreate";
      } else if (isPrepareExecute) {
        return "Job.FlightExecute";
      }
      return "Job.FlightClient";
    case "METADATA_REFRESH":
      return "Job.MetadataRefresh";
    case "INTERNAL_ICEBERG_METADATA_DROP":
      return "Job.InternalIcebergMetadataRemoval";
    case "UNKNOWN":
    default:
      return "File.Unknown";
  }
};

export const getQueueInfo = (jobDetails) => {
  return { label: "Common.Queue", content: jobDetails.get("wlmQueue") };
};

export const GetIsSocketForSingleJob = () => true;
