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

import { columnHighlight, columnNameContent } from "./DataColumn.less";

type HighlightedColumnNameProps = {
  searchTerm: string;
  columnName: string;
};

const HighlightedColumnName = ({
  columnName,
  searchTerm,
}: HighlightedColumnNameProps) => {
  const renderHighlightedName = () => {
    let highlightName = columnName;
    if (
      searchTerm &&
      columnName.toLowerCase().includes(searchTerm?.toLowerCase())
    ) {
      const firstPartIndex = columnName
        .toLowerCase()
        .indexOf(searchTerm?.toLowerCase());
      const firstPart = columnName.substring(0, firstPartIndex);
      const highlightPart = columnName.substr(
        firstPartIndex,
        searchTerm.length,
      );
      const lastPart = columnName.substr(firstPartIndex + searchTerm.length);
      highlightName = (
        <>
          <span>{firstPart}</span>
          <span className={columnHighlight}>{highlightPart}</span>
          <span>{lastPart}</span>
        </>
      );
    }
    return highlightName;
  };

  return <div className={columnNameContent}>{renderHighlightedName()}</div>;
};

export default HighlightedColumnName;
