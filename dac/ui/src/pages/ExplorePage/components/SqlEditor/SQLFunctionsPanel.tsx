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

import { useMemo, useState } from "react";
import { intl } from "#oss/utils/intl";
import { debounce } from "lodash";
import Immutable from "immutable";

import FilterSelectMenu from "#oss/components/Fields/FilterSelectMenu";
import SearchField from "#oss/components/Fields/SearchField";
import {
  FunctionCategoryLabels,
  sortAndFilterSQLFunctions,
} from "#oss/utils/sqlFunctionUtils";
import { ModelFunctionFunctionCategoriesEnum as Categories } from "#oss/types/sqlFunctions";
import LoadingOverlay from "#oss/components/LoadingOverlay";
import EmptyStateContainer from "#oss/pages/HomePage/components/EmptyStateContainer";
import { useSqlFunctions } from "./hooks/useSqlFunctions";
import MemoizedSQLFunctionItem from "./SQLFunctionItem";

import * as classes from "./SQLFunctionsPanel.module.less";

type SQLFunctionsPanelProps = {
  height: number;
  dragType: string;
  addFuncToSqlEditor: (name: string, args?: string) => void;
};

const SQLFunctionsPanel = ({
  height,
  addFuncToSqlEditor,
  dragType,
}: SQLFunctionsPanelProps) => {
  const [sqlFunctions, sqlFunctionsErr] = useSqlFunctions();
  const documentedFunctions = useMemo(() => {
    return (sqlFunctions || []).filter((func) => func.description != null);
  }, [sqlFunctions]);
  const [searchKey, setSearchKey] = useState<string>("");
  const [selectedCategories, setCategories] = useState<Categories[]>([]);
  const [activeItem, setActiveItem] = useState<string | null>(null);
  const panelDisabled = documentedFunctions == null;
  const resetDisabled = selectedCategories?.length === 0;

  const sqlFunctionCategories = useMemo(() => {
    const categories: Categories[] = [];
    documentedFunctions.forEach((func) => {
      if (func.functionCategories) {
        func.functionCategories.forEach((cat) => {
          if (!categories.includes(cat)) categories.push(cat);
        });
      }
    });
    return categories.sort().map((cat) => ({
      label:
        FunctionCategoryLabels[cat] ||
        (cat as any).charAt(0).toUpperCase() +
          (cat as any).toLowerCase().slice(1),
      id: cat,
    }));
  }, [documentedFunctions]);

  const memoizedSQLFunctions = useMemo(() => {
    if (!documentedFunctions) return [];

    return sortAndFilterSQLFunctions(
      documentedFunctions,
      selectedCategories,
      searchKey,
    );
  }, [documentedFunctions, searchKey, selectedCategories]);

  const debounceSearch = debounce((val: string) => {
    setSearchKey(val);
  }, 500);

  const handleRowClick = (key: string) => {
    setActiveItem(key === activeItem ? null : key);
  };

  const onUnselectCategory = (id: Categories) => {
    setCategories(selectedCategories.filter((cat) => id !== cat));
  };

  const onSelectCategory = (id: Categories) => {
    setCategories([...selectedCategories, id]);
  };

  const onResetCategories = () => {
    setCategories([]);
  };

  const MenuHeader = (
    <div className={classes["sql-help-panel__filterHeader"]}>
      {intl.formatMessage({ id: "Dataset.FunctionCategories" })}
      <span
        className={`${classes["sql-help-panel__resetFns"]} ${
          resetDisabled ? classes["--disabled"] : ""
        }`}
        onClick={onResetCategories}
      >
        {intl.formatMessage({ id: "Common.Reset" })}
      </span>
    </div>
  );

  return (
    <div
      className={`${classes["sql-help-panel"]} sql-help-panel`}
      style={{ height }}
    >
      <>
        <div
          className={`${classes["sql-help-panel__filters"]} ${
            panelDisabled ? classes["--disabled"] : ""
          }`}
        >
          <SearchField
            placeholder="Search Functions"
            onChange={(val: string) => debounceSearch(val)}
            showCloseIcon
            showIcon
            disabled={panelDisabled}
            className={classes["sql-help-panel__search"]}
          />
          <FilterSelectMenu
            iconStyle={{
              height: "24px",
              width: "24px",
              display: "flex",
              justifyContent: "center",
              alignItems: "center",
            }}
            noSearch
            iconId={`sql-editor/${
              selectedCategories.length ? "filter-active" : "filter-empty"
            }`}
            selectedToTop={false}
            showSelectedLabel={false}
            onItemSelect={onSelectCategory}
            onItemUnselect={onUnselectCategory}
            selectedValues={Immutable.fromJS(selectedCategories)}
            items={sqlFunctionCategories}
            label=""
            menuHeader={MenuHeader}
            name="categories"
            popoverContentClass={classes["sql-help-panel__filterDropdown"]}
            iconTooltip={intl.formatMessage({
              id: "Dataset.FilterFunctionCategories",
            })}
          />
        </div>
        <div
          className="sql-help-panel__functions"
          style={{
            height: height - 64,
            overflow: "auto",
            contain: "content",
          }}
        >
          {sqlFunctions != null && memoizedSQLFunctions.length > 0 ? (
            memoizedSQLFunctions.map((_, index) => (
              <MemoizedSQLFunctionItem
                key={memoizedSQLFunctions[index].key}
                sqlFunction={memoizedSQLFunctions[index]}
                addFuncToSqlEditor={addFuncToSqlEditor}
                onRowClick={(key: string) => handleRowClick(key)}
                isActiveRow={memoizedSQLFunctions[index].key === activeItem}
                dragType={dragType}
                searchKey={searchKey}
              />
            ))
          ) : sqlFunctions == null && sqlFunctionsErr == null ? (
            <LoadingOverlay />
          ) : (
            <EmptyStateContainer
              title={
                sqlFunctionsErr == null
                  ? "Dataset.NoFunctionsFound"
                  : "Dataset.FunctionsError"
              }
              className={classes["sql-help-panel__noResults"]}
              titleValues={{ search: searchKey }}
              titleClassName={classes["sql-help-panel__noResults__title"]}
            />
          )}
        </div>
      </>
    </div>
  );
};

export default SQLFunctionsPanel;
