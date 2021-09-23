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
import { Component } from 'react';
import Radium from 'radium';
import PropTypes from 'prop-types';
import { AutoSizer, Column, Table } from 'react-virtualized';
import Draggable from 'react-draggable';
import classNames from 'classnames';
import Immutable, { List } from 'immutable';

import { humanSorter, getSortValue } from '@app/utils/sort';
import { virtualizedRow } from './VirtualizedTableViewer.less';

const ROW_HEIGHT = 30;
const HEADER_HEIGHT = 30;
const TABLE_BOTTOM_CUSHION = 10;
const MIN_COLUMN_WIDTH = 130;
export const SortDirection = {
  ASC: 'ASC',
  DESC: 'DESC'
};

// todo: make this determined by the render time on the current machine
const DEFERRED_SPEED_THRESHOLD = 5;

@Radium
export default class VirtualizedTableViewer extends Component {
  static propTypes = {
    tableData: PropTypes.oneOfType([
      PropTypes.instanceOf(Immutable.List),
      PropTypes.array
    ]),
    className: PropTypes.string,
    columns: PropTypes.array,
    defaultSortBy: PropTypes.string,
    defaultSortDirection: PropTypes.string,
    style: PropTypes.object,
    resetScrollTop: PropTypes.bool,
    onClick: PropTypes.func,
    enableHorizontalScroll: PropTypes.bool,
    tableWidth: PropTypes.number,
    resizableColumn: PropTypes.bool,
    loadNextRecords: PropTypes.func,
    scrollToIndex: PropTypes.number,
    sortRecords: PropTypes.func,
    disableSort: PropTypes.bool,
    showIconHeaders: PropTypes.object
    // other props passed into react-virtualized Table
  };

  static defaultProps = {
    tableData: Immutable.List(),
    tableWidth: window.screen.width
  };

  state = {
    sortBy: this.props.defaultSortBy,
    sortDirection: this.props.defaultSortDirection,
    tableColumns: []
  };

  setColumns = () => {
    this.setState({
      tableColumns: this.props.columns
    });
  }

  componentDidMount() {
    this.setColumns();
  }

  componentDidUpdate(previousProps) {
    if (previousProps.columns !== this.props.columns) {
      this.setColumns();
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
      sortDirection: sortRecordsDirection || sortDirection
    });
  };

  rowClassName(rowData, index) {
    return classNames(((rowData && rowData.rowClassName) || '') + ' ' + (index % 2 ? 'odd' : 'even'), virtualizedRow, 'virtualized-row'); // Adding virtualizedRow for keeping the Row styles stable wrt another class
  }

  handleScroll = ({ scrollTop }) => {
    const speed = Math.abs(scrollTop - this.lastScrollTop) / (Date.now() - this.lastScrollTime);

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
      return sortBy ?
        tableData
          .sortBy(item => getSortValue(item, sortBy, sortDirection), humanSorter)
          .update(table =>
            sortDirection === SortDirection.DESC ? table.reverse() : table
          ) :
        tableData;
    }
    if (sortBy) {
      const sortedData = [...tableData] // keeping the order of the original list intact
        .sort((val1, val2) => {
          return humanSorter(getSortValue(val1, sortBy, sortDirection), getSortValue(val2, sortBy, sortDirection));
        });
      return sortDirection === SortDirection.DESC ? sortedData.reverse() : sortedData;
    }
    return tableData;
  }

  renderHeader = ({ label, dataKey, sortBy, sortDirection },
    /* column */ { style, infoContent, headerStyle }) => {
    const isSorted = sortBy === dataKey;
    const headerClassName = classNames(
      'virtualizedTable__headerContent',
      {
        'sort-asc': isSorted && sortDirection === SortDirection.ASC,
        'sort-desc': isSorted && sortDirection === SortDirection.DESC
      }
    );
    const infoContentStyle = {};
    if (isSorted) {
      // sort icon with - 4px to put infoContent closer to sort icon. See .sort-icon() mixin
      infoContentStyle.marginLeft = 20;
    }
    return (
      <div style={{ display: 'flex', alignItems: 'center', cursor: 'default', ...style, ...headerStyle }}>
        <div className={headerClassName} > {label === undefined ? dataKey : label} </div>
        {infoContent ?
          <span style={infoContentStyle}>
            {infoContent}
          </span>
          : null}
      </div>
    );
  };

  renderCell({ rowData, isScrolling }, column) {
    // NOTE: factoring in this.lastSpeed here is too slow
    return <DeferredRenderer defer={isScrolling} render={() => rowData.data[column].node()} />;
  }

  getRow = (sortedTableData, index) => {
    this.props.loadNextRecords && this.props.loadNextRecords(index);
    return List.isList(sortedTableData) ? sortedTableData.get(index) : sortedTableData[index];
  }

  draggableHeaderRenderer = (rowData, item) => {
    const { showIconHeaders } = this.props;
    const key = showIconHeaders[rowData.dataKey] ? showIconHeaders[rowData.dataKey].node() : rowData;
    return (
      <div className='draggableHeaderContent' >
        <div>{this.renderHeader(key, item)}</div>
        <Draggable
          axis='x'
          defaultClassName='DragHandle'
          defaultClassNameDragging='DragHandleActive'
          onStop={(event, data) =>
            this.resizeColumn(
              rowData.dataKey,
              data.x
            )
          }
          position={{
            x: 0,
            y: 0
          }}
          zIndex={999}
        >
          <div className='draggableHeaderContent__pipe' >{item.isDraggable ? '|' : ''}</div>
        </Draggable>
      </div>

    );
  }

  resizeColumn = (dataKey, deltaX) => {
    const { tableColumns } = this.state;
    const thisColumn = tableColumns.find(obj => {
      return obj.key === dataKey;
    });
    thisColumn.width = Math.max(MIN_COLUMN_WIDTH, thisColumn.width + deltaX);
    thisColumn.flexGrow = 0;
    thisColumn.flexShrink = 0;
    this.setState({
      tableColumns
    });
  }

  render() {
    const {
      tableData, style, resetScrollTop, scrollToIndex, sortRecords,
      onClick, enableHorizontalScroll, tableWidth, resizableColumn, disableSort, ...tableProps
    } = this.props;
    const {
      sortBy,
      sortDirection,
      tableColumns
    } = this.state;
    const tableSize = List.isList(tableData) ? tableData.size : tableData.length;
    const sortedTableData = sortRecords ? tableData : this.getSortedTableData();
    const isEmpty = tableSize === 0;
    const baseStyle = isEmpty ? { height: HEADER_HEIGHT } : { height: '100%' };
    const scrollStyle = enableHorizontalScroll ? { overflowX: 'auto' } : {};
    const tableColumnsWidth = tableWidth > window.screen.width && !isEmpty;
    return (
      <div style={[styles.base, baseStyle, style, scrollStyle]}>
        <AutoSizer>
          {({ width, height }) => {
            const tableHeight = height - TABLE_BOTTOM_CUSHION;
            return (
              <Table
                scrollTop={resetScrollTop ? 0 : undefined} // it's needed for https://dremio.atlassian.net/browse/DX-7140
                onScroll={this.handleScroll}
                scrollToIndex={scrollToIndex}
                headerHeight={HEADER_HEIGHT}
                rowCount={tableSize}
                rowClassName={({ index }) => this.rowClassName(this.getRow(sortedTableData, index), index)}
                rowHeight={ROW_HEIGHT}
                rowGetter={({ index }) => this.getRow(sortedTableData, index)}
                sortDirection={sortDirection}
                sortBy={sortBy}
                height={tableHeight}
                width={tableColumnsWidth ? tableWidth : width}
                sort={this.sort}
                {...tableProps}
              >
                {tableColumns.map((item) =>
                  <Column
                    key={item.key || item.dataKey}
                    dataKey={item.key}
                    className={item.className || 'column-' + item.key}
                    headerClassName={item.headerClassName}
                    label={item.label}
                    style={item.style}
                    headerRenderer={
                      !resizableColumn ?
                        (options) => this.renderHeader(options, item)
                        :
                        (data) => this.draggableHeaderRenderer(data, item)
                    }
                    width={item.isFixedWidth ? item.width : width || 100}
                    minWidth={item.width || 100}
                    flexGrow={enableHorizontalScroll ? 0 : item.flexGrow}
                    disableSort={item.disableSort || disableSort}
                    cellRenderer={(opts) => {
                      return (
                        <div onClick={() => onClick && onClick(opts)} >
                          {
                            this.renderCell(opts, item.key)
                          }
                        </div>
                      );
                    }
                    }
                  />
                )}
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
    width: '100%',
    overflow: 'hidden'
  }
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
    defer: PropTypes.bool
  };
  static defaultProps = {
    defer: true
  };
  state = {
    initial: this.props.defer
  };
  componentWillMount() {
    if (this.props.defer) this.constructor._deferredRendererSet.add(this);
  }
  componentWillUnmount() {
    this.constructor._deferredRendererSet.delete(this);
  }

  render() {
    if (this.state.initial) return null;
    return <div>{this.props.render()}</div>;
  }
}
