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
import { FormattedMessage } from "react-intl";
import { useCallback, useRef, useState } from "react";
import {
  AutoSizer,
  InfiniteLoader,
  List,
  ListRowProps,
} from "react-virtualized";
import "./InfiniteScroller.less";
import { Spinner } from "dremio-ui-lib/components";

const LIST_ITEM_HEIGHT = 32;

type InfiniteScrollerProps<T> = {
  rowHeight?: number;
  data: T[];
  numRows: number;
  children: (index: number) => JSX.Element;
  loadData: () => void;
};

function InfiniteScroller<T>({
  data,
  numRows = 1,
  children: renderRow,
  loadData,
  rowHeight = LIST_ITEM_HEIGHT,
}: InfiniteScrollerProps<T>) {
  const [hasError, setHasError] = useState(false);

  function retry() {
    setHasError(false);
    loadMoreRows();
  }

  const loadMoreRows = useCallback(
    async function () {
      try {
        await loadData();
        setHasError(false);
      } catch (e) {
        setHasError(true);
      } finally {
        if (listRef.current) listRef.current.forceUpdate();
      }
    },
    [loadData],
  );

  const isRowLoaded = useCallback(
    function ({ index }: any) {
      if (data.length === 0) return false;
      return index < data.length;
    },
    [data],
  );

  function rowRenderer(row: ListRowProps) {
    const { style, key, index } = row;
    if (isRowLoaded(row)) {
      return (
        <div key={key} style={style}>
          {renderRow(index)}
        </div>
      );
    } else if (hasError) {
      return (
        <div key={key} style={style} className="infiniteScroller__error">
          <a
            href="#"
            onClick={(e) => {
              e.preventDefault();
              retry();
            }}
          >
            <FormattedMessage id="Common.Retry" />
            <dremio-icon
              name="interface/warning"
              class="mr-05"
              style={{
                color: "var(--fill--warning--solid)",
              }}
            />
          </a>
        </div>
      );
    } else {
      return (
        <div key={key} style={style} className="infiniteScroller__loading">
          <Spinner />
        </div>
      );
    }
  }

  const listRef = useRef<List | null>(null);

  return (
    <div className="infiniteScroller">
      <InfiniteLoader
        isRowLoaded={isRowLoaded}
        loadMoreRows={loadMoreRows}
        rowCount={numRows}
      >
        {({ onRowsRendered, registerChild }: any) => (
          <AutoSizer>
            {({ height, width }) => (
              <List
                ref={(ref) => {
                  listRef.current = ref;
                  registerChild(ref);
                }}
                onRowsRendered={onRowsRendered}
                rowRenderer={rowRenderer}
                rowCount={numRows}
                rowHeight={rowHeight}
                height={height || 600}
                width={width || 150}
                data={data} //Rerender when data changes
              />
            )}
          </AutoSizer>
        )}
      </InfiniteLoader>
    </div>
  );
}

export default InfiniteScroller;
