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
import { parser } from "./parser/index.js";

export type ParsedQuery = {
  searchText: string;
  filters: { keyword: string; value: string }[];
};

export const parseQueryText = (queryText: string): ParsedQuery => {
  const tree = parser.parse(queryText);
  const parsedQuery: ParsedQuery = {
    searchText: "",
    filters: [],
  };
  let currentString: string | undefined;
  let currentFilter: { keyword: string; value: string } | undefined;
  tree.iterate({
    enter: (node) => {
      switch (node.name) {
        case "QuotedString":
          currentString = queryText.slice(node.from + 1, node.to - 1);
          break;
        case "UnquotedString":
          currentString = queryText.slice(node.from, node.to);
          break;
        case "Filter":
          currentFilter = {} as any;
          break;
        case "FilterKeyword":
          currentFilter!["keyword"] = queryText.slice(node.from, node.to - 1);
          break;
        case "FilterValue":
          currentFilter!["value"] = queryText.slice(node.from, node.to);
          break;
      }
    },
    leave: (node) => {
      switch (node.name) {
        case "SearchText":
          if (!currentString) {
            throw new Error(
              "Expected currentString to be defined when leaving `SearchText` node"
            );
          }
          parsedQuery.searchText = currentString;
          break;
        case "Filter": {
          if (!currentFilter) {
            throw new Error(
              "Expected a filter object to be defined when leaving `Filter` node"
            );
          }
          parsedQuery.filters.push(currentFilter);
          currentFilter = undefined;
          break;
        }
      }
    },
  });
  return parsedQuery;
};
