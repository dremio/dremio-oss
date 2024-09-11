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
//@ts-nocheck
import { FC } from "react";
import type { CatalogReference } from "@dremio/dremio-js/interfaces";

const iconMap = {
  DATASET_DIRECT: "entities/dataset-table",
  DATASET_PROMOTED: "entities/purple-folder",
  DATASET_VIRTUAL: "entities/dataset-view",
  FILE: "entities/file",
  FOLDER: "entities/blue-folder",
  FUNCTION: "sql-editor/function",
  HOME: "entities/home",
  SOURCE: "sources/SAMPLEDB",
  SPACE: "entities/space",
};

const oldTypes = {
  PHYSICAL_DATASET: iconMap["DATASET_DIRECT"],
  PHYSICAL_DATASET_SOURCE_FOLDER: iconMap["DATASET_PROMOTED"],
  VIRTUAL_DATASET: iconMap["DATASET_VIRTUAL"],
  PHYSICAL_DATASET_SOURCE_FILE: iconMap["FILE"],
};

export const CatalogReferenceIcon: FC<{
  catalogReference: CatalogReference;
}> = (props) => {
  const iconName =
    iconMap[props.catalogReference.type] ||
    oldTypes[props.catalogReference.type] ||
    "entities/empty-file";

  if (iconName === "entities/empty-file") {
    console.log(props.catalogReference.type);
  }

  return <dremio-icon name={iconName} alt=""></dremio-icon>;
};
