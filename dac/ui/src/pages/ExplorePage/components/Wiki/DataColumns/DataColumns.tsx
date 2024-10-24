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

import { DataColumn } from "#oss/pages/ExplorePage/components/DataColumns/DataColumn";
import * as classes from "./DataColumns.module.less";
import clsx from "clsx";

interface DataColumnsProps {
  className?: string;
  searchTerm?: string;
  columns: {
    isPartitioned: boolean;
    isSorted: boolean;
    name: string;
    type: string;
  }[];
}

const DataColumns = ({ className, columns, searchTerm }: DataColumnsProps) => {
  return (
    <div className={clsx(classes["wrapper"], className)}>
      <div className="flex">
        <div className={classes["columnWrapper"]}>
          {columns.map((column) => {
            return (
              <DataColumn
                key={column?.name}
                className={classes["columnCls"]}
                {...column}
                detailsView
                searchTerm={searchTerm}
              />
            );
          })}
        </div>
      </div>
    </div>
  );
};

export default DataColumns;
