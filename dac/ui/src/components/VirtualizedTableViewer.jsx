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
import { Component } from "react";
import clsx from "clsx";
import PropTypes from "prop-types";
import { AutoSizer, Column, Table } from "react-virtualized";
import Draggable from "react-draggable";
import classNames from "clsx";
import Immutable, { List } from "immutable";

import { humanSorter, getSortValue } from "@app/utils/sort";
import { stopPropagation } from "../utils/reactEventUtils";
import {
  getSortIconName,
  SortDirection,
} from "@app/components/Table/TableUtils";
import { virtualizedRow } from "./VirtualizedTableViewer.less";
import { intl } from "@app/utils/intl";
import "@app/components/Table/TableHeader.less";

const ROW_HEIGHT = 30;
const HEADER_HEIGHT = 30;
const TABLE_BOTTOM_CUSHION = 10;
const MIN_COLUMN_WIDTH = 25;

// todo: make this determined by the render time on the current machine
const DEFERRED_SPEED_THRESHOLD = 5;

class VirtualizedTableViewer extends Component {
  static propTypes = {
    tableData: PropTypes.oneOfType([
      PropTypes.instanceOf(Immutable.List),
      PropTypes.array,
    ]),
    rowHeight: PropTypes.number,
    className: PropTypes.string,
    columns: PropTypes.array,
    defaultSortBy: PropTypes.string,
    defaultSortDirection: PropTypes.string,
    style: PropTypes.object,
    resetScrollTop: PropTypes.bool,
    onClick: PropTypes.func,
    enableHorizontalScroll: PropTypes.bool,
    resizableColumn: PropTypes.bool,
    loadNextRecords: PropTypes.func,
    scrollToIndex: PropTypes.number,
    sortRecords: PropTypes.func,
    disableSort: PropTypes.bool,
    showIconHeaders: PropTypes.object,
    defaultDescending: PropTypes.bool,
    disableZebraStripes: PropTypes.any, // for Jobs Page specific styling with no zebra stripes
    onCellRightClick: PropTypes.func,
    // other props passed into react-virtualized Table
  };

  static defaultProps = {
    tableData: Immutable.List(),
    rowHeight: ROW_HEIGHT,
  };

  state = {
    sortBy: this.props.defaultSortBy,
    sortDirection: this.props.defaultSortDirection,
    tableColumns: [],
    resizedTableWidth: 0,
  };

  setColumns = () => {
    this.setState({
      tableColumns: this.props.columns,
    });
  };

  setTableWidth = () => {
    // Minimum width to acommadate the additional column icons
    let width = 160;
    // re-calculate the columns width after an additional column is added
    for (let i = 0; i < this.props.columns.length; i++) {
      width += this.props.columns[i].width;
    }
    this.setState({ resizedTableWidth: width });
  };

  componentDidMount() {
    this.setColumns();
    this.setTableWidth();
  }

  componentDidUpdate(previousProps) {
    if (previousProps.columns !== this.props.columns) {
      this.setColumns();
      this.setTableWidth();
    }
  }

  lastScrollTop = 0;
  lastScrollTime = Date.now();
  lastSpeed = 0;

  sort = ({ sortBy, sortDirection }) => {
    const { sortRecords } = this.props;
    const sortRecordsDirection = sortRecords && sortRecords(sortBy);
    this.setState({
      sortBy,
      sortDirection: sortRecordsDirection || sortDirection,
    });
  };

  rowClassName(rowData, index) {
    const { disableZebraStripes } = this.props;
    if (!disableZebraStripes) {
      //zebra stripes set on odd rows below
      return classNames(
        ((rowData && rowData.rowClassName) || "") +
          " " +
          (index % 2 ? "odd" : "even"),
        virtualizedRow,
        "virtualized-row",
      ); // Adding virtualizedRow for keeping the Row styles stable wrt another class
    } else {
      return classNames(
        (rowData && rowData.rowClassName) || "",
        virtualizedRow,
        "virtualized-row",
        index !== -1 ? "ReactVirtualized__Table--noZebraStripesRows" : null,
      ); // Adding virtualizedRow for keeping the Row styles stable wrt another class
    }
  }

  handleScroll = ({ scrollTop }) => {
    const speed =
      Math.abs(scrollTop - this.lastScrollTop) /
      (Date.now() - this.lastScrollTime);

    if (speed < DEFERRED_SPEED_THRESHOLD) {
      DeferredRenderer._flush(); // going slow enough, can afford to flush as we go
    }
    DeferredRenderer._scheduleFlush(); // ALWAYS schedule, for any renders that happen after the onScroll

    this.lastScrollTop = scrollTop;
    this.lastScrollTime = Date.now();
    this.lastSpeed = speed;
  };

  getSortedTableData = () => {
    const { tableData } = this.props;
    const { sortBy, sortDirection } = this.state;
    if (List.isList(tableData)) {
      return sortBy
        ? tableData
            .sortBy(
              (item) => getSortValue(item, sortBy, sortDirection),
              humanSorter,
            )
            .update((table) =>
              sortDirection === SortDirection.DESC ? table.reverse() : table,
            )
        : tableData;
    }
    if (sortBy) {
      const sortedData = [...tableData] // keeping the order of the original list intact
        .sort((val1, val2) => {
          return humanSorter(
            getSortValue(val1, sortBy, sortDirection),
            getSortValue(val2, sortBy, sortDirection),
          );
        });
      return sortDirection === SortDirection.DESC
        ? sortedData.reverse()
        : sortedData;
    }
    return tableData;
  };

  renderHeader = (
    { label, dataKey, sortBy, sortDirection, disableSort },
    item,
  ) => {
    const { defaultDescending } = this.props;
    const isSorted = sortBy === dataKey;
    const headerClassName = classNames(
      "text",
      "virtualizedTable__headerContent",
      {
        "sort-asc": isSorted && sortDirection === SortDirection.ASC,
        "sort-desc": isSorted && sortDirection === SortDirection.DESC,
      },
    );

    const sortSrc = getSortIconName({
      sortDirection,
      columnKey: dataKey,
      sortBy,
      defaultDescending,
    });

    const sortingIcon = (
      <>
        {!disableSort && label && (
          <dremio-icon
            class={clsx("headerCell__sorting-icon", {
              ["headerCell__sorting-icon--selected"]: sortBy === dataKey,
              ["headerCell__sorting-icon--unsortable"]: disableSort,
            })}
            name={sortSrc}
            alt={
              sortDirection === SortDirection.DESC
                ? intl.formatMessage({ id: "Common.Sort.Alt.Desc" })
                : intl.formatMessage({ id: "Common.Sort.Alt.Asc" })
            }
          />
        )}
      </>
    );

    const infoContentStyle = {};
    if (isSorted) {
      // sort icon with - 4px to put infoContent closer to sort icon. See .sort-icon() mixin
      infoContentStyle.marginLeft = 20;
    }

    return (
      <>
        {item.columnAlignment === "alignRight" && sortingIcon}
        <div className={headerClassName}>
          {label === undefined ? dataKey : label}
        </div>
        {item.columnAlignment !== "alignRight" && sortingIcon}
      </>
    );
  };

  renderCell({ rowData, isScrolling }, column) {
    // NOTE: factoring in this.lastSpeed here is too slow
    return (
      <DeferredRenderer
        defer={isScrolling}
        render={(index) =>
          rowData.data[column].node && rowData.data[column].node(index)
        }
        index={
          rowData.data[column].rowIndex ? rowData.data[column].rowIndex : -1
        }
      />
    );
  }

  getRow = (sortedTableData, index) => {
    this.props.loadNextRecords && this.props.loadNextRecords(index);
    let newRowData;
    if (List.isList(sortedTableData)) {
      newRowData = sortedTableData.get(index);
    } else {
      newRowData = sortedTableData[index];
    }
    if (newRowData && newRowData.data && newRowData.data.buttons) {
      newRowData.data.buttons.rowIndex = index;
    }
    return newRowData;
  };

  draggableHeaderRenderer = (rowData, item) => {
    const { showIconHeaders } = this.props;
    const key = showIconHeaders[rowData.dataKey]
      ? showIconHeaders[rowData.dataKey].node()
      : rowData;
    return (
      <div className={clsx("draggableHeaderContent", "text")}>
        <div
          className={clsx("draggableHeaderInnerText", {
            "headerCell--alignRight": item.columnAlignment === "alignRight",
          })}
        >
          {this.renderHeader(key, item)}
        </div>
        <div className="maxZIndex" onClick={(e) => stopPropagation(e)}>
          <Draggable
            axis="x"
            defaultClassName="DragHandle"
            defaultClassNameDragging="DragHandleActive"
            onStop={(event, data) => this.resizeColumn(rowData.dataKey, data.x)}
            position={{
              x: 0,
              y: 0,
            }}
          >
            <span
              className={clsx(
                "draggableHeaderContent__pipe headerCell__dragPipe",
              )}
            >
              {item.isDraggable ? "|" : ""}
            </span>
          </Draggable>
        </div>
      </div>
    );
  };

  resizeColumn = (dataKey, deltaX) => {
    const { tableColumns, resizedTableWidth } = this.state;
    const thisColumn = tableColumns.find((obj) => {
      return obj.key === dataKey;
    });
    const oldColumnWidth = thisColumn.width;
    thisColumn.width = Math.max(MIN_COLUMN_WIDTH, thisColumn.width + deltaX);
    thisColumn.flexGrow = 0;
    thisColumn.flexShrink = 0;
    // adjusts the table width to accomdate the resized column
    const newTableWidth = resizedTableWidth - oldColumnWidth + thisColumn.width;
    this.setState({
      tableColumns,
      resizedTableWidth: newTableWidth,
    });
  };

  render() {
    const {
      tableData,
      rowHeight,
      style,
      resetScrollTop,
      scrollToIndex,
      sortRecords,
      onClick,
      enableHorizontalScroll,
      resizableColumn,
      disableSort,
      onCellRightClick,
      ...tableProps
    } = this.props;
    const { sortBy, sortDirection, tableColumns, resizedTableWidth } =
      this.state;

    const tableSize = List.isList(tableData)
      ? tableData.size
      : tableData.length;
    const sortedTableData = sortRecords ? tableData : this.getSortedTableData();
    const isEmpty = tableSize === 0;
    const baseStyle = isEmpty ? { height: HEADER_HEIGHT } : { height: "100%" };
    const scrollStyle = enableHorizontalScroll ? { overflowX: "auto" } : {};

    return (
      <div
        style={{
          ...styles.base,
          ...baseStyle,
          ...(style || {}),
          ...scrollStyle,
        }}
        className="virtualized-table-viewer"
      >
        <AutoSizer className="auto-sizer">
          {({ width, height }) => {
            const tableHeight = height - TABLE_BOTTOM_CUSHION;
            return (
              <Table
                scrollTop={resetScrollTop ? 0 : undefined} // it's needed for DX-7140
                onScroll={this.handleScroll}
                scrollToIndex={scrollToIndex}
                headerHeight={HEADER_HEIGHT}
                rowCount={tableSize}
                rowClassName={({ index }) =>
                  this.rowClassName(this.getRow(sortedTableData, index), index)
                }
                rowHeight={rowHeight}
                rowGetter={({ index }) => this.getRow(sortedTableData, index)}
                sortDirection={sortDirection}
                sortBy={sortBy}
                height={tableHeight}
                width={
                  resizedTableWidth > width && !isEmpty
                    ? resizedTableWidth
                    : width
                }
                sort={this.sort}
                {...tableProps}
              >
                {tableColumns.map((item) => {
                  return (
                    <Column
                      key={item.key || item.dataKey}
                      dataKey={item.key}
                      className={
                        item.className ||
                        `column-${item.key}  ${
                          item.textEllipsesAlignedLeft
                            ? "rightAlignedEllipsesText"
                            : null
                        }`
                      }
                      headerClassName={item.headerClassName}
                      label={item.label}
                      style={item.style}
                      headerStyle={item.headerStyle}
                      headerRenderer={
                        !resizableColumn
                          ? (options) => this.renderHeader(options, item)
                          : (data) => this.draggableHeaderRenderer(data, item)
                      }
                      width={item.isFixedWidth ? item.width : width || 100}
                      minWidth={item.width || 100}
                      flexGrow={enableHorizontalScroll ? 0 : item.flexGrow}
                      disableSort={item.disableSort || disableSort}
                      cellRenderer={(opts) => {
                        const tabIndex =
                          opts.rowData.data.sql &&
                          opts.rowData.data.sql.tabIndex;
                        return (
                          <div
                            className="text"
                            onClick={() => {
                              !item.disabledClick &&
                                onClick &&
                                onClick(tabIndex ? tabIndex : opts);
                            }}
                            {...(onCellRightClick && {
                              onContextMenu: (e) => onCellRightClick(e, opts),
                            })}
                          >
                            {this.renderCell(opts, item.key)}
                          </div>
                        );
                      }}
                    />
                  );
                })}
              </Table>
            );
          }}
        </AutoSizer>
      </div>
    );
  }
}

const styles = {
  base: {
    flexGrow: 1,
    width: "100%",
    overflow: "hidden",
  },
};

export class DeferredRenderer extends Component {
  static _deferredRendererSet = new Set();
  static _deferredRendererId = 0;
  static _flush() {
    for (const comp of this._deferredRendererSet) {
      comp.setState({ initial: false });
    }
    this._deferredRendererSet = new Set();
  }
  static _scheduleFlush() {
    cancelAnimationFrame(this._deferredRendererId);
    this._deferredRendererId = requestAnimationFrame(() => {
      this._deferredRendererId = requestAnimationFrame(() => this._flush());
    });
  }

  static propTypes = {
    render: PropTypes.func.isRequired,
    defer: PropTypes.bool,
    index: PropTypes.number,
  };
  static defaultProps = {
    defer: true,
  };
  state = {
    initial: this.props.defer,
  };
  UNSAFE_componentWillMount() {
    if (this.props.defer) this.constructor._deferredRendererSet.add(this);
  }
  componentWillUnmount() {
    this.constructor._deferredRendererSet.delete(this);
  }

  render() {
    const { index } = this.props;
    if (this.state.initial) return null;
    return this.props.render(index);
  }
}
export default VirtualizedTableViewer;
