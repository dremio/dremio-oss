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

import ResourcePin from "@app/components/ResourcePin";
import Immutable from "immutable";

export const getCatalogData = (
  items: Immutable.List<any>,
  sort: null | Map<string, string>,
  sortingMap: any,
  filter: string,
): Immutable.Iterable<number, any> => {
  const filteredItems = filter
    ? items.filter((s) =>
        s.get("name").toLowerCase().includes(filter.toLowerCase()),
      )
    : items;

  if (!sort || sort.size === 0) {
    return filteredItems.sortBy(
      (value) => value.get("name").toLowerCase(),
      (a, b) => (a < b ? -1 : a > b ? 1 : 0),
    );
  }

  const sortEntry = sort.keys().next().value;
  const sortValue = sort.values().next().value;
  return filteredItems.sortBy(
    (value) =>
      typeof value.get(sortingMap[sortEntry]) === "string"
        ? value.get(sortingMap[sortEntry]).toLowerCase()
        : value.get(sortingMap[sortEntry]),
    (a, b) =>
      typeof a === "string"
        ? stringSort(a, b, sortValue)
        : numberSort(a, b, sortValue),
  );
};

const stringSort = (
  a: string,
  b: string,
  sortValue: "ascending" | "descending",
) => {
  return sortValue === "ascending"
    ? a < b
      ? -1
      : a > b
        ? 1
        : 0
    : a > b
      ? -1
      : a < b
        ? 1
        : 0;
};

const numberSort = (
  a: number,
  b: number,
  sortValue: "ascending" | "descending",
) => {
  return sortValue === "ascending" ? a - b : b - a;
};

export const renderResourcePin = (id: string) => {
  return <ResourcePin entityId={id} />;
};

export const renderSourceDetailsIcon = (
  location: any,
  item: Immutable.Map<any, any>,
  renderIcon: (item: Immutable.Map<any, any>) => JSX.Element,
) => {
  return renderIcon(item);
};
