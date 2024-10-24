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

export const catalogReferenceEntityToProperties = (
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  properties: any,
): any => {
  switch (properties.type) {
    case "CONTAINER": {
      return {
        id: properties.id,
        path: properties.path,
        type: properties.containerType,
      };
    }
    case "DATASET":
      return {
        id: properties.id,
        path: properties.path,
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        type: `DATASET_${properties.datasetType}` as any,
      };
    default:
      return {
        id: properties.id,
        path: properties.path,
        type: properties.type,
      };
  }
};

const requiresQuotes = /\W/;

export const pathString =
  (getPath: () => string[]) =>
  (SEPARATOR: string = "."): string => {
    return getPath()
      .map((part) => (requiresQuotes.test(part) ? `"${part}"` : part))
      .join(SEPARATOR);
  };

export const mappedType = {
  PHYSICAL_DATASET: "DATASET_PROMOTED",
  VIRTUAL_DATASET: "DATASET_VIRTUAL",
} as const;
