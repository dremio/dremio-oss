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
import Art from "@app/components/Art";
import { useIntl } from "react-intl";
import Immutable from "immutable";
import * as classes from "./SearchSource.module.less";

type SearchSourceProps = {
  sources: Immutable.Map<any, any>;
  updateSources: (
    filteredList: Immutable.Iterable<any, any>,
    searchValue: string
  ) => void;
};

const SearchSource = ({ sources, updateSources }: SearchSourceProps) => {
  const intl = useIntl();
  const filterSources = (searchValue: string) => {
    const searchString = searchValue.toLowerCase();
    const filteredList = sources.filter((source: any) => {
      const label = source.label.toLowerCase();
      if (label.indexOf(searchString) > -1) {
        return source;
      }
    });
    updateSources(filteredList, searchString);
  };

  const onChange = (e: any) => {
    filterSources(e.target.value);
  };

  return (
    <div className={classes["source-search-container"]}>
      <span>
        <Art
          title="Source.SearchSources"
          alt="Source.SearchSources"
          src="Search.svg"
        />
      </span>
      <input
        key="textInput"
        type="text"
        onChange={onChange}
        placeholder={intl.formatMessage({ id: "Source.SearchDataSource" })}
        className={classes["source-search-container__input"]}
      />
    </div>
  );
};

export default SearchSource;
