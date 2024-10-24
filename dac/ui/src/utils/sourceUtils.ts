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
import { intl } from "#oss/utils/intl";
import {
  NESSIE,
  NAS,
  HDFS,
  HIVE,
  HIVE3,
  RESTCATALOG,
  GCS,
  AZURE_STORAGE,
  S3,
  AWSGLUE,
  POLARIS,
  UNITY,
} from "#oss/constants/sourceTypes";

export function sourceTypesIncludeS3(sourceTypes: { sourceType: string }[]) {
  return sourceTypes && !!sourceTypes.find((type) => type.sourceType === "S3");
}

export function sourceTypesIncludeSampleSource(
  sourceTypes: { sourceType: string }[],
) {
  return (
    sourceTypes &&
    !!sourceTypes.find((type) => type.sourceType === "SAMPLE_SOURCE")
  );
}

export function isVersionedSource(type: string) {
  switch (type) {
    case NESSIE:
      return true;
    default:
      return false;
  }
}

export function isNessieSource(type?: string) {
  return type === NESSIE;
}

export function isArcticSource(type?: string) {
  return false;
}

export const isIcebergSource = (sourceType: string) => {
  return (
    NAS === sourceType ||
    HDFS === sourceType ||
    NESSIE === sourceType ||
    HIVE === sourceType ||
    HIVE3 === sourceType ||
    RESTCATALOG === sourceType ||
    GCS === sourceType ||
    UNITY === sourceType ||
    POLARIS === sourceType ||
    AZURE_STORAGE === sourceType ||
    S3 === sourceType ||
    AWSGLUE === sourceType
  );
};

export const getSourceIcon = (sourceType: string) => {
  if (NESSIE === sourceType) {
    return "entities/nessie-source";
  } else {
    return "entities/datalake-source";
  }
};

export const showSourceIcon = (sourceType: string) => {
  return true;
};

export const getAddSourceModalTitle = (sourceType: string) => {
  return undefined;
};

export const getEditSourceModalTitle = (sourceType: string, name: string) => {
  return intl.formatMessage({ id: "Source.EditSource" });
};
