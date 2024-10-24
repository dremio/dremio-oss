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

/*
**** Scrollable Table Component ****

This component was bootstrapped with [React Virtualised MultiGrid](https://github.com/bvaughn/react-virtualized/blob/master/docs/MultiGrid.md).

## How does it work

This multi grid works at it's base with two tables, one fixed and the other scrollable. There is no inherent concept of row in it's DOM structure
(all are individual cells) but it does return row index and column index in the `cellRenderer`.

Refer to Table in `ClusterListView.js` for implementation and `provisioningConstants.js` for style, width, minWidth etc. additions.

### Things to note apart from docs

- Most of the styles are to be passed to tables and there can be any number of customisations for header and cells.
- No need to add tooltips to the cell nodes as it will automatically show our default tooltip with ellipsis if the text overflows.
- It can also render tooltips which are nested inside HTML elements(by taking the innermost html text).
- If you want to add actions (edit, delete etc.) to the end of the row, just name that column as `action` and it will show up on hover of the row.
- Please provide `width` and `minWidth` along with column data as this prevents dragging a column(resizing) too short.
- Disable tooltip option now available for table if you want to retain the old textwithhelp component.
- Show Icon header support is available

To be updated as new use cases come up...
*/
import { useState, useEffect, useCallback, useMemo } from "react";
import { AutoSizer, InfiniteLoader, MultiGrid } from "react-virtualized";
import Draggable from "react-draggable";
import classNames from "clsx";
import { List } from "immutable";
import clsx from "clsx";
// @ts-ignore
import { Tooltip } from "dremio-ui-lib";
import { intl } from "#oss/utils/intl";
import {
  findDeepestChild,
  getSortedTableData,
  getSortIconName,
  SortDirection,
} from "./TableUtils";
import * as tableClasses from "./ScrollableTable.module.less";
import "./ScrollableTable.less";
import "./TableHeader.less";

const ROW_HEIGHT = 40;
const HEADER_HEIGHT = 30;

export type ScrollableTableProps = {
  tableData: any;
  columns: any;
  rowHeight: number;
  style: any;
  resetScrollTop: boolean;
  scrollToIndex: number;
  enableHorizontalScroll: boolean;
  resizableColumn: boolean;
  disableSort: boolean;
  disableTooltip: boolean | undefined;
  showIconHeaders: any;
  fixedColumnCount: number | undefined;
  intl: any;
  router: any;
  defaultSortBy: string;
  defaultSortDirection: string;
  onClickId: string | undefined;
  onClick: (opts: any) => void | undefined;
  sortRecords: () => void;
  loadNextRecords: (index: number) => any;
  renderCell: (opts: any, column: string) => any;
  renderCellNew: (opts: any) => any;
  showConfirmationDialog: (content: any) => void;
};

const ScrollableTable: React.ReactNode = (props: ScrollableTableProps) => {
  const {
    tableData,
    columns,
    style,
    onClick,
    fixedColumnCount,
    showIconHeaders,
    disableTooltip,
    onClickId,
    ...tableProps
  } = props;
  const [tableColumns, setTableColumns] = useState([]);
  const [gridState, setGridState] = useState<MultiGrid | null>(null);
  const [resizedTableWidth, setResizedTableWidth] = useState(160);
  const [sortBy, setSortBy] = useState(props.defaultSortBy);
  const [sortDirection, setSortDirection] = useState(
    props.defaultSortDirection,
  );
  const [showAction, setShowAction] = useState<boolean[]>([]);
  const [showTooltip, setShowTooltip] = useState<{ [key: string]: string }>({});
  const [fixedTableShadow, setFixedTableShadow] = useState("");
  const [overflow, setOverflow] = useState<boolean>(false);
  const [widthFactor, setWidthFactor] = useState<number | undefined>(undefined);
  // const [scrollToRow, setScrollToRow] = useState<number | undefined>(undefined);
  const [dragging, setDragging] = useState<boolean>(false);
  const tableSize = List.isList(tableData) ? tableData.size : tableData.length;
  const isEmpty = tableSize === 0;
  const baseStyle = isEmpty ? { height: HEADER_HEIGHT } : { height: "100%" };

  useEffect(() => {
    if (columns) {
      let count = 0;
      const wrapperWidth =
        document.getElementsByClassName("view-state-wrapper").length > 0
          ? document.getElementsByClassName("view-state-wrapper")[0].clientWidth
          : document.documentElement.clientWidth;
      columns.forEach((col: any) => {
        if (col.isFixedWidth) count += 1;
      });
      const equalWidth = wrapperWidth / (columns.length - count);
      columns.map((col: any) => {
        // on first time load distribute columns across the page, column width shouldn't be less than minWidth though
        let iconHeader = true;
        if (showIconHeaders && showIconHeaders[col.key]) {
          iconHeader = false;
        }
        if (col.label && iconHeader) {
          if (equalWidth + 4 > col.minWidth) {
            col.width = equalWidth + 4;
          } else {
            col.width = col.minWidth;
          }
        }
        return col;
      });
      setTableColumns(columns);
      setTableWidth();
      gridState && gridState.recomputeGridSize();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [columns]);

  const sortedTable = useMemo(() => {
    if (!onClickId) return;
    let sortedTableData = getSortedTableData(
      props.tableData,
      sortBy,
      sortDirection,
    );
    sortedTableData = List.isList(sortedTableData)
      ? sortedTableData.toArray()
      : sortedTableData;
    return sortedTableData;
  }, [sortBy, sortDirection, props.tableData]);

  useEffect(() => {
    gridState && gridState.recomputeGridSize();
    gridState && gridState.forceUpdateGrids();
  }, [resizedTableWidth, gridState]);

  const setTableWidth = () => {
    // Minimum width to acommodate the additional column icons
    let width = 160;
    // re-calculate the columns width after an additional column is added
    for (let i = 0; i < columns.length; i++) {
      width += columns[i].width;
    }
    setResizedTableWidth(width);
  };

  const resizeColumn = (dataKey: any, deltaX: number) => {
    let oldColumnWidth = 0;
    let currentColumnWidth = 0;
    tableColumns.map((obj: any) => {
      if (obj.key === dataKey) {
        oldColumnWidth = obj.width || 100;
        currentColumnWidth = Math.max(
          obj.minWidth,
          (obj.width || 100) + deltaX,
        );
        obj.width = currentColumnWidth;
        obj.flexGrow = 0;
        obj.flexShrink = 0;
      }
      return obj;
    });
    // adjusts the table width to accomdate the resized column
    const newTableWidth =
      resizedTableWidth - oldColumnWidth + currentColumnWidth;
    setTableColumns(tableColumns);
    setResizedTableWidth(newTableWidth);
    gridState && gridState.recomputeGridSize();
    hideActions();
  };

  const showActions = (
    rowIndex: number,
    cellKey: string,
    e: any,
    showFirstLevelTooltip: boolean | undefined,
  ) => {
    const parentElement = e.currentTarget;
    let element = parentElement;
    // to add normal tooltip when you have a nested html element
    const child = findDeepestChild(parentElement);
    if (child.depth > 0) {
      element = child.element;
    }
    // disable tooltips for entire table if tooltips are handled at element level themselves
    if (
      element.clientWidth < element.scrollWidth &&
      !disableTooltip &&
      !showFirstLevelTooltip
    ) {
      showTooltip[cellKey] = element.innerText;
    }
    showAction.map((row, index) => {
      if (index !== rowIndex) showAction[index] = false;
      else showAction[rowIndex] = true;
      return row;
    });
    showAction[rowIndex] = true;
    setShowTooltip(showTooltip);
    setShowAction(showAction);
    gridState && gridState.recomputeGridSize();
  };

  const hideActions = () => {
    setShowTooltip({});
    setShowAction([]);
    gridState && gridState.recomputeGridSize();
  };

  const getColumnWidth = (e: { index: number }) => {
    const col: any = columns[e.index];
    const colWidth: number = col ? (col.width ? col.width : 100) : 100;
    return colWidth;
  };

  const showShadowOnFixedTable = (data: any) => {
    if (data.clientWidth && data.scrollLeft > 0) {
      setFixedTableShadow("fixed-grid-shadow");
    } else {
      setFixedTableShadow("");
    }
  };

  // useEffect(() => {
  //   gridState && gridState.recomputeGridSize();
  //   // eslint-disable-next-line react-hooks/exhaustive-deps
  // }, [fixedColCount]);

  // Checks if the list overflows
  const handleOverflow = useCallback(
    (node: HTMLDivElement | null) => {
      if (node) {
        const offset = 0;
        const bottom = node.getBoundingClientRect().bottom;
        // The below if else statement will set the factor to multiply 55 with because the list gets cut off only after the second element overflows
        if (
          window.innerHeight + ROW_HEIGHT * 2 > bottom &&
          window.innerHeight + ROW_HEIGHT < bottom
        ) {
          setWidthFactor(1);
        } else {
          setWidthFactor(2);
        }
        setOverflow(
          !(
            bottom + offset >= 0 &&
            bottom - offset <= window.innerHeight + ROW_HEIGHT
          ),
        );
      }
    },
    [tableSize],
  );

  const renderCell = (rowData: any) => {
    // not reliable, throwing more issues than it is helping :p commenting out for now
    // fix for scroll issue in react-virtualised where fixed and scrollable grid are not in sync. ref-https://github.com/bvaughn/react-virtualized/issues/1473
    // if (
    //   rowData.isScrolling &&
    //   rowData.parent.state.scrollLeft === 0 &&
    //   fixedColCount !== 0
    // ) {
    //   setFixedColCount(undefined);
    //   setScrollToRow(undefined);
    // } else if (!rowData.isScrolling && fixedColCount === undefined) {
    //   setFixedColCount(fixedColumnCount);
    // }
    const { columnIndex, rowIndex, key } = rowData;
    const column: any = tableColumns[columnIndex];
    const {
      label,
      isFixedWidth,
      headerStyle,
      disableSort,
      showFirstLevelTooltip,
      columnAlignment,
    } = column;
    const tableData = getSortedTableData(
      props.tableData,
      sortBy,
      sortDirection,
    );
    const tableDatatest = List.isList(tableData)
      ? tableData.toArray()
      : tableData;
    const headerClassName = classNames(
      "text",
      "virtualizedTable__headerContent",
    );
    const cellVal =
      rowIndex > 0 ? tableDatatest[rowIndex - 1].data[column.key].node() : "";
    const headerLabel =
      showIconHeaders && showIconHeaders[column.key]
        ? showIconHeaders[column.key].node().label
        : label;
    const rowStyle = { ...rowData.style };

    const sortSrc = getSortIconName({
      sortDirection,
      columnKey: column.key,
      sortBy,
      defaultDescending: false,
    });

    const sortingIcon = (
      <>
        {!disableSort && label && (
          <dremio-icon
            class={clsx("headerCell__sorting-icon", {
              ["headerCell__sorting-icon--selected"]: sortBy === column.key,
              ["headerCell__sorting-icon--unsortable"]: disableSort,
            })}
            name={sortSrc}
            alt={
              sortDirection === "DESC"
                ? intl.formatMessage({ id: "Common.Sort.Alt.Desc" })
                : intl.formatMessage({ id: "Common.Sort.Alt.Asc" })
            }
          />
        )}
      </>
    );

    const renderCellWithOrWithoutAction = (
      showActionCondition?: boolean,
      type?: string,
    ) => {
      return (
        <span
          className={clsx(
            "cell text-ellipsis",
            tableClasses[
              showActionCondition ? "cellWithAction" : "cellWithoutAction"
            ],
            tableClasses["tableCell"],
            {
              [tableClasses["tableCell--alignedRight"]]:
                columnAlignment === "alignRight",
            },
          )}
          style={{
            ...rowStyle,
            ...column.style,
          }}
          key={key}
          onMouseEnter={(e) =>
            showActions(rowIndex, key, e, showFirstLevelTooltip)
          }
        >
          {showActionCondition !== undefined
            ? showActionCondition && cellVal
            : type === "action"
              ? ""
              : cellVal}
        </span>
      );
    };
    // to render column header
    if (rowData.rowIndex === 0) {
      return (
        <div
          data-qa={`column-${column.key}`}
          key={key}
          className={clsx("headerCell", "headerCell--withBorderBottom", {
            ["headerCell--sortable"]:
              !disableSort && label && sortBy === column.key,
            ["headerCell--dragging"]: dragging,
            ["headerCell--alignRight"]: columnAlignment === "alignRight",
          })}
          style={{ ...headerStyle, ...rowStyle }}
          onClick={(e) => {
            e.stopPropagation();
            e.preventDefault();
            if (!disableSort && column.key === sortBy) {
              setSortDirection(
                sortDirection === SortDirection.ASC
                  ? SortDirection.DESC
                  : SortDirection.ASC,
              );
            }
            if (!disableSort && column.key !== sortBy) {
              setSortBy(column.key);
              setSortDirection(SortDirection.ASC);
            }
          }}
        >
          <div className={headerClassName}>
            {columnAlignment === "alignRight" && sortingIcon}
            {headerLabel}
            {columnAlignment !== "alignRight" && sortingIcon}
          </div>
          {label &&
            !isFixedWidth &&
            !(
              columnIndex + 1 === fixedColumnCount && fixedTableShadow !== ""
            ) && (
              <div
                data-qa="resizable-column-test"
                onClick={(e) => {
                  // to stop drag stop event to propogate through column header click(sort) event
                  e.stopPropagation();
                  e.preventDefault();
                }}
              >
                <Draggable
                  axis="x"
                  defaultClassName="DragHandle"
                  defaultClassNameDragging="DragHandleActive"
                  onDrag={(e) => {
                    e.stopPropagation();
                    e.preventDefault();
                  }}
                  onStart={(e) => {
                    setDragging(true);
                    e.stopPropagation();
                    e.preventDefault();
                  }}
                  onStop={(event, data) => {
                    event.stopPropagation();
                    event.preventDefault();
                    setDragging(false);
                    resizeColumn(column.key, data.x);
                  }}
                  position={{ x: 0, y: 0 }}
                >
                  <div
                    className={clsx(
                      "draggableHeaderContent__pipe",
                      "headerCell__dragPipe",
                    )}
                  >
                    |
                  </div>
                </Draggable>
              </div>
            )}
        </div>
      );
    } else {
      // to render rest of the table
      if (showAction[rowData.rowIndex]) {
        rowStyle.background = "var(--fill--primary--hover)";
      }
      if (column.key === "action") {
        return renderCellWithOrWithoutAction(
          showAction[rowData.rowIndex],
          "action",
        );
      } else if (showTooltip[rowData.key]) {
        return (
          <Tooltip title={showTooltip[rowData.key]}>
            {renderCellWithOrWithoutAction()}
          </Tooltip>
        );
      } else {
        return renderCellWithOrWithoutAction();
      }
    }
  };
  // for infinite loading in grid
  const infiniteLoaderChildFunction = (
    { onRowsRendered }: any,
    width: number,
    height: number,
  ) => {
    const { tableData } = props;

    if (tableColumns.length > 0) {
      return (
        <MultiGrid
          {...tableProps}
          onSectionRendered={(data) => {
            onSectionRendered(data, onRowsRendered);
          }}
          ref={(grid) => {
            setGridState(grid);
          }}
          cellRenderer={(opts) => {
            return (
              <div
                data-qa={`table-${opts.rowIndex}-${opts.columnIndex}`}
                key={opts.key}
                style={
                  typeof onClick === "function"
                    ? { cursor: "pointer" }
                    : { display: "block" }
                }
                onClick={() =>
                  onClick &&
                  onClick(
                    onClickId
                      ? sortedTable[opts.rowIndex - 1]?.data[onClickId]
                          .clickValue
                      : opts.rowIndex,
                  )
                }
              >
                {tableColumns.length > 0 ? renderCell(opts) : null}
              </div>
            );
          }}
          columnWidth={getColumnWidth}
          columnCount={tableColumns.length}
          height={
            onClickId === "engine" && overflow
              ? height - (widthFactor ? widthFactor * 55 : 0)
              : height
          }
          rowHeight={ROW_HEIGHT}
          rowCount={tableSize + 1}
          width={width}
          onScroll={(data) => {
            showShadowOnFixedTable(data);
          }}
          data={tableData}
          columns={tableColumns}
          // scrollToRow={scrollToRow}
          fixedColumnCount={fixedColumnCount}
          fixedRowCount={1}
          classNameBottomLeftGrid={fixedTableShadow}
        />
      );
    } else {
      return null;
    }
  };

  const onSectionRendered = (
    { rowStartIndex, rowStopIndex }: any,
    onRowsRendered: any,
  ) => {
    const startIndex = rowStartIndex;
    const stopIndex = rowStopIndex;
    onRowsRendered({
      startIndex,
      stopIndex,
    });
  };

  const isRowLoaded = ({ index }: { index: number }) => {
    return index < tableSize - 1;
  };

  const loadMoreRows = (range: { startIndex: number; stopIndex: number }) => {
    const rangeVal = range;
    // so it doesn't go to top when new data is fetched
    // setScrollToRow(rangeVal.stopIndex);
    return props.loadNextRecords && props.loadNextRecords(rangeVal.stopIndex);
  };
  const tempTableHeight = tableSize * 40 + 55;
  const tableHeight =
    tempTableHeight >
    document.getElementsByClassName("accelerations-table")[0]?.clientHeight
      ? document.getElementsByClassName("accelerations-table")[0]?.clientHeight
      : tempTableHeight;
  return (
    <div
      className={clsx("scrollable-table", tableClasses["base"])}
      style={{
        // style,
        ...baseStyle,
        height: tableHeight,
        flexGrow: 0,
      }}
      ref={(node) => handleOverflow(node)}
      data-qa="scrollable-table-test"
      onMouseLeave={() => hideActions()}
    >
      <AutoSizer>
        {({ width, height }) => {
          return (
            <InfiniteLoader
              isRowLoaded={isRowLoaded}
              loadMoreRows={loadMoreRows}
              rowCount={tableSize}
              minimumBatchSize={100}
            >
              {(loaderProps) =>
                infiniteLoaderChildFunction(
                  loaderProps,
                  // Test env defaults to 0 and fails so need to set width and height
                  width || 100,
                  height || 100,
                )
              }
            </InfiniteLoader>
          );
        }}
      </AutoSizer>
    </div>
  );
};
export const TestScrollableTable = ScrollableTable;

export default TestScrollableTable;
