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
import {
  DATASET_TYPES_TO_DREMIO_ICON,
  DATASET_TYPES_TO_ICEBERG_TYPES,
} from "@app/constants/datasetTypes";
import { NESSIE, ARCTIC } from "@app/constants/sourceTypes";
import { formatMessage } from "./locale";

const FILE_TYPES_TO_ICON_TYPES = {
  database: "sources/SAMPLEDB", //Check this one manually
  table: "entities/dataset",
  dataset: "entities/dataset-view",
  physicalDatasets: "entities/dataset-table",
};

const FILE_TYPES_TO_ICEBERG_ICON_TYPES = {
  table: "entities/iceberg-table",
  dataset: "entities/iceberg-view",
  physicalDatasets: "entities/iceberg-table",
};

export function getIconDataTypeFromEntity(entity) {
  const fileType = entity.get("fileType");
  if (fileType === "folder") {
    if (entity.get("queryable")) {
      return "entities/purple-folder";
    }
    return "entities/blue-folder";
  }
  if (fileType === "file") {
    if (entity.get("queryable")) {
      return "entities/dataset-table";
    }
    return "entities/empty-file";
  }
  return FILE_TYPES_TO_ICON_TYPES[fileType] || "entities/empty-file";
}

export function getIcebergIconTypeFromEntity(entity) {
  const fileType = entity.get("fileType");
  if (["table", "dataset", "physicalDatasets"].includes(fileType)) {
    return FILE_TYPES_TO_ICEBERG_ICON_TYPES[fileType];
  } else return getIconDataTypeFromEntity(entity);
}

export function getIconDataTypeFromDatasetType(datasetType) {
  return DATASET_TYPES_TO_DREMIO_ICON[datasetType];
}

const STATUSES_ICON_POSTFIX = {
  good: "",
  bad: "-bad",
  degraded: "-degraded",
};

const getSourceIcon = (sourceType) => {
  if (NESSIE === sourceType) {
    return "entities/nessie-source";
  } else if (ARCTIC === sourceType) {
    return "entities/arctic-source";
  } else {
    return "entities/datalake-source";
  }
};

export function getIconStatusDatabase(status, sourceType) {
  return getSourceIcon(sourceType) + (STATUSES_ICON_POSTFIX[status] || "");
}

export function getIconByEntityType(type, isVersioned) {
  switch (type && type.toUpperCase()) {
    case "DATASET":
    case "VIRTUAL":
    case "VIRTUAL_DATASET":
      if (isVersioned) {
        return DATASET_TYPES_TO_ICEBERG_TYPES[type];
      } else {
        return "entities/dataset-view";
      }
    case "PHYSICALDATASET":
    case "PHYSICAL_DATASET":
    case "PHYSICAL":
    case "TABLE":
      if (isVersioned) {
        return DATASET_TYPES_TO_ICEBERG_TYPES[type];
      } else {
        return "entities/dataset-table";
      }
    case "SPACE":
      return "entities/space";
    case "HOME":
      return "entities/home";
    case "FILE":
      return "entities/dataset-table";
    case "PHYSICAL_DATASET_SOURCE_FILE":
      return "entities/dataset-table";
    case "PHYSICAL_DATASET_SOURCE_FOLDER":
      return "entities/purple-folder";
    case "PHYSICAL_DATASET_HOME_FILE":
      return "entities/dataset-table";
    case "FOLDER":
      return "entities/blue-folder";
    case "SOURCE_ROOT":
      return "sources/SAMPLEDB";
    case "OTHERS":
      return "entities/dataset-other";
    default:
      return "entities/empty-file";
  }
}

export function oldGetIconByEntityType(type, isVersioned) {
  switch (type && type.toUpperCase()) {
    case "DATASET":
    case "VIRTUAL":
    case "VIRTUAL_DATASET":
      if (isVersioned) {
        return DATASET_TYPES_TO_ICEBERG_TYPES[type];
      } else {
        return "VirtualDataset";
      }
    case "PHYSICALDATASET":
    case "PHYSICAL_DATASET":
    case "PHYSICAL":
    case "TABLE":
      if (isVersioned) {
        return DATASET_TYPES_TO_ICEBERG_TYPES[type];
      } else {
        return "PhysicalDataset";
      }
    case "SPACE":
      return "Space";
    case "HOME":
      return "Home";
    case "FILE":
      return "File";
    case "PHYSICAL_DATASET_SOURCE_FILE":
      return "File";
    case "PHYSICAL_DATASET_SOURCE_FOLDER":
      return "FolderData";
    case "PHYSICAL_DATASET_HOME_FILE":
      return "File";
    case "FOLDER":
      return "Folder";
    case "SOURCE_ROOT":
      return "Database";
    case "OTHERS":
      return "OtherDataSets";
    default:
      return "FileEmpty";
  }
}

export function getSourceStatusIcon(sourceStatus, sourceType) {
  const iconType =
    sourceStatus === null
      ? getSourceIcon(sourceType)
      : getIconStatusDatabase(sourceStatus, sourceType);
  return iconType;
}

export function getFormatMessageIdByEntityType(type) {
  switch (type && type.toUpperCase()) {
    case "DATASET":
    case "VIRTUAL":
    case "VIRTUAL_DATASET":
      return "Dataset.VirtualDataset";
    case "PHYSICALDATASET":
    case "PHYSICAL_DATASET":
    case "PHYSICAL":
    case "TABLE":
      return "Dataset.PhysicalDataset";
    case "SPACE":
      return "Space.Space";
    case "HOME":
      return "Common.Home";
    case "FILE":
    case "PHYSICAL_DATASET_SOURCE_FILE":
    case "PHYSICAL_DATASET_HOME_FILE":
      return "File.File";
    case "PHYSICAL_DATASET_SOURCE_FOLDER":
      return "Folder.FolderData";
    case "FOLDER":
      return "Folder.Folder";
    default:
      return "File.FileEmpty";
  }
}

export function getIconAltTextByEntityType(entityType) {
  return formatMessage(getFormatMessageIdByEntityType(entityType));
}

function getFormatMessageIdBySourceType(sourceType) {
  if (NESSIE === sourceType) {
    return "Source.NessieSource";
  } else if (ARCTIC === sourceType) {
    return "Source.ArcticSource";
  } else {
    return "Source.Source";
  }
}

export function getIconAltTextBySourceType(sourceType) {
  return formatMessage(getFormatMessageIdBySourceType(sourceType));
}
