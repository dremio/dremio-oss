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
import _ from "lodash";
import { List } from "immutable";
import { humanSorter, getSortValue } from "@app/utils/sort";

export const SortDirection = {
  ASC: "ASC",
  DESC: "DESC",
};

export const findDeepestChild = (parent: any) => {
  let result = { depth: 0, element: parent };
  _.forEach(parent.children, (idx: any) => {
    const child = idx;
    const childResult = findDeepestChild(child);
    if (childResult.depth + 1 > result.depth) {
      result = {
        depth: 1 + childResult.depth,
        element: childResult.element,
      };
    }
  });
  return result;
};

export const getSortedTableData = (
  tableData: any,
  sortBy: string,
  sortDirection: string
) => {
  if (List.isList(tableData)) {
    return sortBy
      ? tableData
          .sortBy(
            (item: any) => getSortValue(item, sortBy, sortDirection),
            humanSorter
          )
          .update((table: any) =>
            sortDirection === SortDirection.DESC ? table.reverse() : table
          )
      : tableData;
  }
  if (sortBy) {
    const sortedData = [...tableData] // keeping the order of the original list intact
      .sort((val1, val2) => {
        return humanSorter(
          getSortValue(val1, sortBy, sortDirection),
          getSortValue(val2, sortBy, sortDirection)
        );
      });
    return sortDirection === SortDirection.DESC
      ? sortedData.reverse()
      : sortedData;
  }
  return tableData;
};

export const getSortIconName = ({
  sortDirection,
  sortBy,
  columnKey,
  defaultDescending = false,
}: {
  sortDirection: string;
  columnKey: string;
  sortBy: string;
  defaultDescending: boolean;
}) => {
  let sortSrc = "interface/arrow-up";
  if (sortBy === columnKey) {
    if (sortDirection === SortDirection.ASC) {
      sortSrc = "interface/arrow-up";
    } else if (sortDirection === SortDirection.DESC) {
      sortSrc = "interface/arrow-down";
    }
  } else {
    if (defaultDescending) {
      // the jobs page is built to start on desc order
      sortSrc = "interface/arrow-down";
    } else {
      sortSrc = "interface/arrow-up";
    }
  }

  return sortSrc;
};
