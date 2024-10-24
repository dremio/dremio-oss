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

import Immutable from "immutable";

import { ENTITY_TYPES } from "#oss/constants/Constants";
import { getCanEditWiki } from "#oss/pages/HomePage/components/WikiView";

export const isEntityWikiEditAllowed = (entity?: Immutable.Map<any, any>) => {
  if (entity?.get("datasetType")) {
    return entity?.get("entityId") && true;
  } else {
    return getCanEditWiki(entity);
  }
};

export function getEntityTypeFromObject(entity?: Immutable.Map<any, any>) {
  const type = entity?.get("homeConfig")
    ? ENTITY_TYPES.home // home doesn't have a 'type' property
    : entity?.get("fileType") ||
      entity?.get("datasetType") ||
      entity?.get("entityType") ||
      entity?.get("type") ||
      ENTITY_TYPES.folder; // folder as a list item doesn't have a 'type' property
  return type;
}

export const transformEntity = (entity: any) => {
  const selfLink = entity?.links?.self || "";
  if (selfLink.includes("/source/") && selfLink.split("/").length === 3)
    entity.entityType = "source";
  if (selfLink.includes("/space/") && selfLink.split("/").length === 3)
    entity.entityType = "space";
  return entity;
};

export const withEntityProps =
  <T,>(WrappedComponent: React.ComponentClass) =>
  (props: T) => {
    return <WrappedComponent {...props} />;
  };
