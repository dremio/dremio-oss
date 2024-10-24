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
import { LIST, MAP, STRUCT } from "#oss/constants/DataTypes";
import { isEmpty } from "lodash";
import exploreUtils from "#oss/utils/explore/exploreUtils";

export const transformTypeURLMapper = (
  transform: Immutable.Map<string, any>,
) => {
  const transformType = transform.get("transformType");
  const columnType = transform.get("columnType");

  if (transformType === "split") {
    return transformType;
  }

  switch (columnType) {
    case MAP:
      return `${transformType}_map`;
    case STRUCT:
      return `${transformType}_struct`;
    case LIST:
      return `${transformType}_list`;
    default:
      return transformType;
  }
};

export const loadTransformCardsWrapper = <
  T extends {
    dataset: Immutable.Map<string, any>;
    transform: Immutable.Map<string, any>;
    loadTransformCards: (
      data: Record<string, any>,
      transform: Immutable.Map<string, any>,
      dataset: Immutable.Map<string, any>,
      actionType: string,
    ) => Promise<any>;
  },
>({
  dataset,
  transform,
  loadTransformCards,
}: T) => {
  const method = transform.get("method");

  if (
    (exploreUtils.needSelection(method) &&
      !exploreUtils.transformHasSelection(transform)) ||
    !exploreUtils.needsToLoadCardFormValuesFromServer(transform)
  ) {
    return Promise.resolve();
  }

  const transformSelection = transform.get("selection").toJS();
  const columnName = transform.get("columnName");
  const actionType = transformTypeURLMapper(transform);
  const selection = !isEmpty(transformSelection)
    ? transformSelection
    : exploreUtils.getDefaultSelection(columnName);

  return loadTransformCards(selection, transform, dataset, actionType);
};
