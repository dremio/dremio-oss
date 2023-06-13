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

import { ModifiedSQLFunction as SQLFunction } from "@app/endpoints/SQLFunctions/listSQLFunctions";
import { ModelFunctionFunctionCategoriesEnum as FunctionCategories } from "@app/types/sqlFunctions";

export const FunctionCategoryLabels = {
  [FunctionCategories.AGGREGATE]: "Aggregate",
  [FunctionCategories.BINARY]: "Binary",
  [FunctionCategories.BOOLEAN]: "Boolean",
  [FunctionCategories.BITWISE]: "Bitwise",
  [FunctionCategories.CHARACTER]: "Character",
  [FunctionCategories.CONDITIONAL]: "Conditional",
  [FunctionCategories.CONTEXT]: "Context",
  [FunctionCategories.CONVERSION]: "Conversion",
  [FunctionCategories.DATETIME]: "Date/Time",
  [FunctionCategories.DATATYPE]: "Data type",
  [FunctionCategories.DIRECTORY]: "Directory",
  [FunctionCategories.GEOSPATIAL]: "Geospatial",
  [FunctionCategories.MATH]: "Math",
  [FunctionCategories.WINDOW]: "Window",
};

export const SQLFunctionCategories = Object.values(FunctionCategories).map(
  (cat: any) => ({
    label: FunctionCategoryLabels[cat],
    id: cat,
  })
);

export const sortAndFilterSQLFunctions = (
  functions: SQLFunction[],
  categories: FunctionCategories[],
  searchKey: string
) => {
  const functionsInCategories =
    categories.length === 0
      ? functions
      : functions.filter((fn: SQLFunction) =>
          fn.functionCategories
            ? fn.functionCategories.some((cat: FunctionCategories) => {
                return categories.includes(cat);
              })
            : false
        );

  return searchKey !== ""
    ? functionsInCategories.filter((func: any) =>
        func.name.toLowerCase().includes(searchKey.toLowerCase())
      )
    : functionsInCategories;
};
