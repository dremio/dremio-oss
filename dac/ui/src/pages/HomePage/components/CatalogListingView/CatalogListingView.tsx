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

import { intl } from "@app/utils/intl";
import { SearchField } from "@app/components/Fields";
import { CatalogListingTable } from "dremio-ui-common/sonar/components/CatalogListingTable/CatalogListingTable.js";
import { useEffect, useRef } from "react";
import Mousetrap from "mousetrap";
import { debounce } from "lodash";
import EmptyStateContainer from "../EmptyStateContainer";

import * as classes from "./CatalogListingView.module.less";

type CatalogListingViewProps = {
  title: React.ReactNode;
  columns: any;
  getRow: (i: number) => any;
  onColumnsSorted: (sortedColumns: Map<string, string>) => void;
  rowCount: number;
  rightHeaderButtons?: React.ReactNode;
  leftHeaderButtons?: React.ReactNode;
  onFilter?: (filter: string) => void;
  showButtonDivider?: boolean;
  leftHeaderStyles?: any;
};

const CatalogListingView = ({
  title,
  columns,
  getRow,
  rowCount,
  onColumnsSorted,
  rightHeaderButtons = null,
  leftHeaderButtons = null,
  onFilter,
  showButtonDivider,
  leftHeaderStyles = {},
}: CatalogListingViewProps) => {
  const searchFieldRef = useRef<any>();

  useEffect(() => {
    Mousetrap.bind(["command+f", "ctrl+f"], () => {
      searchFieldRef.current?.focus();
      return false;
    });

    return () => {
      Mousetrap.unbind(["command+f", "ctrl+f"]);
    };
  }, []);

  const debounceFilter = debounce((val: string) => onFilter?.(val), 250);

  return (
    <div className={classes["catalog-listing-view"]}>
      <div className={classes["catalog-listing-view__header"]}>
        <div
          className={classes["catalog-listing-view__header-left"]}
          style={{ ...leftHeaderStyles }}
        >
          {title}
        </div>
        <div className={classes["catalog-listing-view__header-right"]}>
          {leftHeaderButtons && (
            <div className="flex items-center flex-row">
              {leftHeaderButtons}
            </div>
          )}
          {onFilter && (
            <SearchField
              ref={(searchField) => (searchFieldRef.current = searchField)}
              onChange={debounceFilter}
              placeholder={intl.formatMessage({
                id: "Dataset.FilterEllipsis",
              })}
              showCloseIcon
              showIcon
              closeIconTheme={
                showButtonDivider && {
                  Icon: {
                    width: 22,
                    height: 22,
                  },
                  Container: {
                    cursor: "pointer",
                    position: "absolute",
                    right: 3,
                    top: 0,
                    bottom: 0,
                    margin: "auto",
                    marginRight: "16px",
                    width: 22,
                    height: 22,
                  },
                }
              }
              style={{ marginRight: 6, width: 240 }}
              className={`${classes["catalog-listing-view__search-field"]} ${
                showButtonDivider &&
                classes["catalog-listing-view__search-field-divider"]
              }`}
            />
          )}
          {rightHeaderButtons && (
            <div className="flex items-center flex-row gp-1">
              {rightHeaderButtons}
            </div>
          )}
        </div>
      </div>
      <div className={classes["catalog-listing-view__table"]}>
        {rowCount === 0 ? (
          <div className={classes["catalog-listing-view__empty"]}>
            <EmptyStateContainer
              icon="interface/empty-content"
              title={intl.formatMessage({
                id: "Catalog.Source.Empty.Title",
              })}
            >
              {searchFieldRef.current?.state.value
                ? intl.formatMessage(
                    {
                      id: "Catalog.Source.Empty.HintSearch",
                    },
                    {
                      search: searchFieldRef.current?.state.value,
                    }
                  )
                : null}
            </EmptyStateContainer>
          </div>
        ) : (
          <CatalogListingTable
            getRow={getRow}
            columns={columns}
            rowCount={rowCount}
            onColumnsSorted={onColumnsSorted}
          />
        )}
      </div>
    </div>
  );
};

export default CatalogListingView;
