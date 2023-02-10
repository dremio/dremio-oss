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

import { useEffect, useMemo, useRef, useState } from "react";
import { intl } from "@app/utils/intl";
import { debounce } from "lodash";
import Immutable from "immutable";
import {
  AutoSizer,
  CellMeasurer,
  CellMeasurerCache,
  List,
} from "react-virtualized";

import { useResourceSnapshot } from "smart-resource/react";
import FilterSelectMenu from "@app/components/Fields/FilterSelectMenu";
import SearchField from "@app/components/Fields/SearchField";
import SQLFunctionsResource from "@app/resources/SQLFunctionsResource";
import MemoizedSQLFunctionItem from "./SQLFunctionItem";
import {
  sortAndFilterSQLFunctions,
  SQLFunctionCategories,
} from "@app/utils/sqlFunctionUtils";
import { ModelFunctionFunctionCategoriesEnum as Categories } from "@app/types/sqlFunctions";
import LoadingOverlay from "@app/components/LoadingOverlay";
import EmptyStateContainer from "@app/pages/HomePage/components/EmptyStateContainer";

import * as classes from "./SQLFunctionsPanel.module.less";

const cellCache = new CellMeasurerCache({
  fixedWidth: true,
  defaultHeight: 106,
  minHeight: 68,
});

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
  const [sqlFunctions = []] = useResourceSnapshot(SQLFunctionsResource);
  const [searchKey, setSearchKey] = useState<string>("");
  const [selectedCategories, setCategories] = useState<Categories[]>([]);
  const [activeItem, setActiveItem] = useState<string | null>(null);
  const listRef = useRef<any>();
  const listIndices = useRef<{
    startIndex: number;
    stopIndex: number;
  } | null>();
  const panelDisabled = sqlFunctions == null;
  const resetDisabled = selectedCategories?.length === 0;

  useEffect(() => {
    if (!sqlFunctions) SQLFunctionsResource.fetch();
  }, [sqlFunctions]);

  const memoizedSQLFunctions = useMemo(() => {
    if (!sqlFunctions) return [];

    return sortAndFilterSQLFunctions(
      sqlFunctions,
      selectedCategories,
      searchKey
    );
  }, [sqlFunctions, searchKey, selectedCategories]);

  const debounceSearch = debounce((val: string) => {
    setSearchKey(val);
  }, 500);

  const resetRangeCacheAndHeight = (startIndex: number, stopIndex?: number) => {
    const curStopIndex = stopIndex ?? startIndex;
    for (let idx = startIndex; idx <= curStopIndex; idx++) {
      cellCache?.clear(idx, 0);
      listRef.current?.recomputeRowHeights?.(idx);
    }
  };

  const handleRowClick = (key: string, index: number) => {
    if (listIndices?.current) {
      // Reset previous item in view
      resetRangeCacheAndHeight(
        listIndices?.current.startIndex,
        listIndices?.current.stopIndex
      );
    }

    setActiveItem(key === activeItem ? null : key);
    resetRangeCacheAndHeight(index);
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

  // Need to reset cache when searching/category filtering
  useEffect(() => {
    cellCache?.clearAll();
    listRef.current?.recomputeRowHeights?.();
  }, [memoizedSQLFunctions.length]);

  // Debouncing prevents max updates error
  const debouncedRenderedRows = debounce(
    (startIndex: number, stopIndex: number) => {
      if (
        listIndices?.current?.startIndex !== startIndex &&
        listIndices?.current?.stopIndex !== stopIndex
      ) {
        listIndices.current = { startIndex, stopIndex };
        const start = startIndex === 0 ? startIndex : startIndex - 1;
        const stop =
          stopIndex === memoizedSQLFunctions.length ? stopIndex : stopIndex + 1;
        resetRangeCacheAndHeight(start, stop);
      }
    },
    1
  );

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
            items={SQLFunctionCategories}
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
          }}
        >
          <AutoSizer>
            {({ width, height: panelHeight }) => {
              return (
                <List
                  ref={(ref) => (listRef.current = ref)}
                  deferredMeasurementCache={cellCache}
                  noRowsRenderer={() =>
                    sqlFunctions == null ? (
                      <LoadingOverlay />
                    ) : (
                      <EmptyStateContainer
                        title="Dataset.NoFunctionsFound"
                        className={classes["sql-help-panel__noResults"]}
                        titleValues={{ search: searchKey }}
                        titleClassName={
                          classes["sql-help-panel__noResults__title"]
                        }
                      />
                    )
                  }
                  onRowsRendered={({ startIndex, stopIndex }) =>
                    debouncedRenderedRows(startIndex, stopIndex)
                  }
                  rowRenderer={({ index, key, parent, style }) => {
                    return (
                      <CellMeasurer
                        cache={cellCache}
                        key={key}
                        parent={parent}
                        rowIndex={index}
                        columnIndex={0}
                      >
                        <div key={key} style={style}>
                          <MemoizedSQLFunctionItem
                            key={memoizedSQLFunctions[index].key}
                            sqlFunction={memoizedSQLFunctions[index]}
                            addFuncToSqlEditor={addFuncToSqlEditor}
                            onRowClick={(key: string) =>
                              handleRowClick(key, index)
                            }
                            isActiveRow={
                              memoizedSQLFunctions[index].key === activeItem
                            }
                            dragType={dragType}
                          />
                        </div>
                      </CellMeasurer>
                    );
                  }}
                  height={panelHeight}
                  rowCount={memoizedSQLFunctions.length}
                  rowHeight={cellCache?.rowHeight}
                  width={width}
                />
              );
            }}
          </AutoSizer>
        </div>
      </>
    </div>
  );
};

export default SQLFunctionsPanel;
