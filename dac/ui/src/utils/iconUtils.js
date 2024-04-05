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
  DATASET_TYPES_TO_ICEBERG_TYPES,
  DATASET_TYPES_TO_ICON_TYPES,
} from "@app/constants/datasetTypes";
import { NESSIE, ARCTIC } from "@app/constants/sourceTypes";
import { formatMessage } from "./locale";

const FILE_TYPES_TO_ICON_TYPES = {
  database: "Database",
  table: "PhysicalDataset",
  dataset: "VirtualDataset",
  physicalDatasets: "PhysicalDataset",
};

const FILE_TYPES_TO_ICEBERG_ICON_TYPES = {
  table: "IcebergTable",
  dataset: "IcebergView",
  physicalDatasets: "IcebergTable",
};

export function getIconDataTypeFromEntity(entity) {
  const fileType = entity.get("fileType");
  if (fileType === "folder") {
    if (entity.get("queryable")) {
      return "FolderData";
    }
    return "Folder";
  }
  if (fileType === "file") {
    if (entity.get("queryable")) {
      return "File";
    }
    return "FileEmpty";
  }
  return FILE_TYPES_TO_ICON_TYPES[fileType] || "FileEmpty";
}

export function getIcebergIconTypeFromEntity(entity) {
  const fileType = entity.get("fileType");
  if (["table", "dataset", "physicalDatasets"].includes(fileType)) {
    return FILE_TYPES_TO_ICEBERG_ICON_TYPES[fileType];
  } else return getIconDataTypeFromEntity(entity);
}

export function getIconDataTypeFromDatasetType(datasetType) {
  return DATASET_TYPES_TO_ICON_TYPES[datasetType];
}

const STATUSES_ICON_POSTFIX = {
  good: "",
  bad: "-Bad",
  degraded: "-Degraded",
};

const getSourceIcon = (sourceType) => {
  if (NESSIE === sourceType) {
    return "Repository";
  } else if (ARCTIC === sourceType) {
    return "ArcticCatalog";
  } else {
    return "Database";
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

export function getFormatMessageIdByEntityIconType(iconType) {
  switch (iconType) {
    case "VirtualDataset":
      return "Dataset.VirtualDataset";
    case "PhysicalDataset":
      return "Dataset.PhysicalDataset";
    case "IcebergView":
      return "Dataset.IcebergView";
    case "IcebergTable":
      return "Dataset.IcebergTable";
    case "Space":
      return "Space.Space";
    case "Database":
      return "Source.Source";
    case "ArcticCatalog":
    case "ArcticCatalog-Bad":
    case "ArcticCatalog-Degraded":
      return "Source.ArcticSource";
    case "Repository":
    case "Repository-Bad":
    case "Repository-Degraded":
      return "Source.NessieSource";
    case "Home":
      return "Common.Home";
    case "File":
      return "File.File";
    case "FolderData":
      return "Folder.FolderData";
    case "Folder":
      return "Folder.Folder";
    default:
      return "File.FileEmpty";
  }
}

export function getIconAltTextByEntityIconType(iconType) {
  return formatMessage(getFormatMessageIdByEntityIconType(iconType));
}
