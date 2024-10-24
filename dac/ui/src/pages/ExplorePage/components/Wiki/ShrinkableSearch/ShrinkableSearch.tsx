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

import { useState } from "react";
import { SearchField } from "#oss/components/Fields";
import { useIntl } from "react-intl";
import { IconButton } from "dremio-ui-lib";
import * as classes from "./ShrinkableSearch.module.less";

interface ShrinkableSearchProps {
  search: (searchVal: string) => void;
  tooltip: string;
}

const ShrinkableSearch = ({ search, tooltip }: ShrinkableSearchProps) => {
  const [searchActive, setSearchActive] = useState(false);
  const intl = useIntl();

  return searchActive ? (
    <SearchField
      placeholder={intl.formatMessage({ id: "Wiki.SearchColumns" })}
      onChange={search}
      showCloseIcon
      showIcon
      onBlur={(e: { target: { value: string } }) => {
        if (e.target.value.trim() !== "") {
          // if filter onblur is needed add it here
        } else {
          setSearchActive(false);
        }
      }}
      autoFocus
      className={classes["searchField"]}
    />
  ) : (
    <IconButton
      tooltip={tooltip}
      onClick={() => setSearchActive(true)}
      className={classes["searchButton"]}
    >
      <dremio-icon name="interface/search" class="icon-primary" />
    </IconButton>
  );
};

export default ShrinkableSearch;
