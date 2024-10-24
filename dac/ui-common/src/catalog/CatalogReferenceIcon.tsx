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
import type { FC } from "react";
import type { CatalogReference } from "@dremio/dremio-js/interfaces";
import { getIntlContext } from "../contexts/IntlContext";

const iconMap = {
  DATASET_DIRECT: "catalog/dataset/table",
  DATASET_PROMOTED: "catalog/dataset/promoted",
  DATASET_VIRTUAL: "catalog/dataset/view",
  FILE: "entities/file",
  FOLDER: "catalog/folder",
  FUNCTION: "catalog/function",
  HOME: "entities/home",
  SOURCE: "sources/SAMPLEDB",
  SPACE: "entities/space",
};

const mapOldToNewType = (type: string): keyof typeof iconMap => {
  switch (type) {
    case "PHYSICAL_DATASET":
      return "DATASET_DIRECT";
    case "PHYSICAL_DATASET_SOURCE_FOLDER":
      return "DATASET_PROMOTED";
    case "VIRTUAL_DATASET":
      return "DATASET_VIRTUAL";
    case "PHYSICAL_DATASET_SOURCE_FILE":
      return "FILE";
    default:
      //@ts-ignore
      return type;
  }
};

export const CatalogReferenceIcon: FC<{
  catalogReference: CatalogReference;
}> = (props) => {
  const { t } = getIntlContext();

  const type = mapOldToNewType(props.catalogReference.type);

  const iconName = iconMap[type] || "entities/empty-file";

  const tKey = `Catalog.Reference.Type.${type}.Label`;

  const label = t(tKey);

  return (
    <dremio-icon
      name={iconName}
      alt={label !== tKey ? label : ""}
      title={label !== tKey ? label : undefined}
    ></dremio-icon>
  );
};
