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
  PartitionTransformations,
  ReflectionDataType,
} from "dremio-ui-common/sonar/reflections/ReflectionDataTypes.js";

export const PARTITION_TRANSFORMATION_LOWER_LIMIT = 1;
export const PARTITION_TRANSFORMATION_UPPER_LIMIT = 2147483647;

export const ColumnTypeIcons: { [key in ReflectionDataType]: string } = {
  [ReflectionDataType.DECIMAL]: "decimal",
  [ReflectionDataType.INTEGER]: "num",
  [ReflectionDataType.BIGINT]: "num",
  [ReflectionDataType.FLOAT]: "double",
  [ReflectionDataType.DOUBLE]: "double",
  [ReflectionDataType.VARCHAR]: "varchar",
  [ReflectionDataType.VARBINARY]: "varbinary",
  [ReflectionDataType.BOOLEAN]: "boolean",
  [ReflectionDataType.DATE]: "date",
  [ReflectionDataType.TIME]: "time",
  [ReflectionDataType.TIMESTAMP]: "datetime",
  [ReflectionDataType.INTERVAL]: "datetime",
  [ReflectionDataType.STRUCT]: "struct",
  [ReflectionDataType.LIST]: "array",
  [ReflectionDataType.MAP]: "map",
};

export const MenuOptions: { [key in PartitionTransformations]: string } = {
  [PartitionTransformations.IDENTITY]: "Common.Original",
  [PartitionTransformations.BUCKET]:
    "Acceleration.PartitionTransformation.Bucket",
  [PartitionTransformations.TRUNCATE]:
    "Acceleration.PartitionTransformation.Truncate",
  [PartitionTransformations.HOUR]: "Common.Hour",
  [PartitionTransformations.DAY]: "Common.Day",
  [PartitionTransformations.MONTH]: "Common.Month",
  [PartitionTransformations.YEAR]: "Common.Year",
};

/**
 * Format used to submit / fetch reflections
 */
export type PartitionTransformationFormat = {
  name: string;
  transform?: {
    type: PartitionTransformations;
    bucketTransform?: {
      bucketCount: number;
    };
    truncateTransform?: {
      truncateLength: number;
    };
  };
};

/**
 *
 * @param reflection The reflection to check for partitionFields
 * @returns An updated array of partitionFields to include in the API payload
 */
export const formatPartitionFields = (
  reflection: Record<string, any>
): PartitionTransformationFormat[] => {
  const updatedPartitionFields = [];

  for (const partitionField of reflection.partitionFields) {
    if (partitionField.name.constructor === Object) {
      updatedPartitionFields.push(partitionField.name);
    } else {
      updatedPartitionFields.push(partitionField);
    }
  }

  return updatedPartitionFields;
};

/**
 * Format used to track partition values in the Acceleration Form
 */
type PartitionFieldFormat = { name: string | Record<string, any> };

/**
 *
 * @param reflection The reflection to check for partitionFields
 * @returns An updated array of partitionFields to initiate the acceleration form
 */
export const preparePartitionFieldsAsFormValues = (
  reflection: Record<string, any>
): PartitionFieldFormat[] => {
  const updatedPartitionFields = [];

  for (const partitionField of reflection.partitionFields as PartitionTransformationFormat[]) {
    if (partitionField.transform) {
      updatedPartitionFields.push({
        name: {
          name: partitionField.name,
          transform: partitionField.transform,
        },
      });
    } else {
      updatedPartitionFields.push(partitionField);
    }
  }

  return updatedPartitionFields;
};
