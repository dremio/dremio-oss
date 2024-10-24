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

import { type FC } from "react";
import type {
  CatalogObject,
  CommunitySource,
} from "@dremio/dremio-js/interfaces";

const getIconForSource = (source: CommunitySource) => {
  switch (source.type) {
    case "NESSIE":
      return <dremio-icon name="entities/nessie-source" class="h-3 w-3" />;
    case "ARCTIC":
      return <dremio-icon name="brand/arctic-catalog-source" class="h-3 w-3" />;
    default:
      return <dremio-icon name="entities/datalake-source" class="h-3 w-3" />;
    // return <dremio-icon name={`source/${source.type}`}/>
  }
};

export const CatalogObjectIcon: FC<{ catalogObject: CatalogObject }> = (
  props,
) => {
  switch (props.catalogObject.referenceType) {
    case "HOME":
      return <dremio-icon name="entities/home" class="h-3 w-3 icon-primary" />;
    case "SPACE":
      return <dremio-icon name="entities/space" class="h-3 w-3" />;
    case "SOURCE":
      return getIconForSource(props.catalogObject as CommunitySource);
    case "FOLDER":
      return <dremio-icon name="catalog/folder" class="h-3 w-3" />;
    // case "VersionedDataset": {
    //   if (
    //     (props.catalogObject as CommunityDataset).type === "DATASET_VIRTUAL"
    //   ) {
    //     return <dremio-icon name="entities/iceberg-view"></dremio-icon>;
    //   } else {
    //     return <dremio-icon name="entities/iceberg-table"></dremio-icon>;
    //   }
    // }
    case "FILE":
      return <dremio-icon name="entities/file" class="h-3 w-3" />;
    case "DATASET_DIRECT":
      return <dremio-icon name="catalog/dataset/table" class="h-3 w-3" />;
    case "DATASET_VIRTUAL":
      return <dremio-icon name="catalog/dataset/view" class="h-3 w-3" />;
    case "DATASET_PROMOTED":
      return <dremio-icon name="catalog/dataset/promoted" class="h-3 w-3" />;
    case "FUNCTION":
      return (
        <dremio-icon name="catalog/function" class="h-3 w-3 icon-primary" />
      );
    default:
      return <dremio-icon name="data-types/TypeOther" class="h-3 w-3" />;
    // return props.catalogObject.referenceType;
  }
};
