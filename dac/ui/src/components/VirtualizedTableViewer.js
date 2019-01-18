/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
import { AutoSizer, Table, Column } from 'react-virtualized';
import classNames from 'classnames';
import Immutable from 'immutable';

import {humanSorter} from 'utils/sort';

const ROW_HEIGHT = 30;
const HEADER_HEIGHT = 39;

export const SortDirection = {
  ASC: 'ASC',
  DESC: 'DESC'
};

// todo: make this determined by the render time on the current machine
const DEFERRED_SPEED_THRESHOLD = 5;

@Radium
export default class VirtualizedTableViewer extends Component {
  static propTypes = {
    tableData: PropTypes.instanceOf(Immutable.List),
    className: PropTypes.string,
    columns: PropTypes.array,
    defaultSortBy: PropTypes.string,
    defaultSortDirection: PropTypes.string,
    style: PropTypes.object,
    resetScrollTop: PropTypes.bool
    // other props passed into react-virtualized Table
  }

  static defaultProps = {
    tableData: Immutable.List()
  }

  state = {
    sortBy: this.props.defaultSortBy,
    sortDirection: this.props.defaultSortDirection
  }

  lastScrollTop = 0
  lastScrollTime = Date.now()
  lastSpeed = 0

  sort = ({ sortBy, sortDirection }) => {
    this.setState({
      sortBy,
      sortDirection
    });
  }

  rowClassName(rowData, index) {
    return ((rowData && rowData.rowClassName) || '') + ' ' + (index % 2 ? 'odd' : 'even');
  }

  handleScroll = ({scrollTop}) => {
    const speed = Math.abs(scrollTop - this.lastScrollTop) / (Date.now() - this.lastScrollTime);

    if (speed < DEFERRED_SPEED_THRESHOLD) {
      DeferredRenderer._flush(); // going slow enough, can afford to flush as we go
    }
    DeferredRenderer._scheduleFlush(); // ALWAYS schedule, for any renders that happen after the onScroll

    this.lastScrollTop = scrollTop;
    this.lastScrollTime = Date.now();
    this.lastSpeed = speed;
  }

  renderHeader = ({ label, dataKey, sortBy, sortDirection },
    /* column */ { style, infoContent, headerStyle }) => {
    const isSorted = sortBy === dataKey;
    const headerClassName = isSorted ? classNames({
      'sort-asc': sortDirection === SortDirection.ASC,
      'sort-desc': sortDirection === SortDirection.DESC
    }) : '';
    const infoContentStyle = {};
    if (isSorted) {
      // sort icon with - 4px to put infoContent closer to sort icon. See .sort-icon() mixin
      infoContentStyle.marginLeft = 20;
    }
    return (
      <div style={{ display: 'flex', alignItems: 'center', ...style, ...headerStyle }}>
        <div className={headerClassName}>{ label === undefined ? dataKey : label} </div>
        {infoContent && <span style={infoContentStyle}>
          {infoContent}
        </span>}
      </div>
    );
  }

  renderCell({rowData, rowIndex, isScrolling}, column) {
    // NOTE: factoring in this.lastSpeed here is too slow
    return <DeferredRenderer defer={isScrolling} render={() => rowData.data[column].node()}/>;
  }

  render() {
    const { tableData, columns, style, resetScrollTop, ...tableProps } = this.props;
    const { sortBy, sortDirection } = this.state;

    const getSortValue = (item) => {
      const value = item.data[sortBy].value;
      if (typeof value === 'function') {
        return value.call(item.data[sortBy], sortDirection, sortBy);
      }
      return value;
    };

    const sortedTableData = sortBy
      ? tableData
        .sortBy(item => getSortValue(item), humanSorter)
        .update(table =>
          sortDirection === SortDirection.DESC
            ? table.reverse()
            : table
        )
      : tableData;
    const isEmpty = tableData.size === 0;
    const baseStyle = isEmpty ? { height: HEADER_HEIGHT } : { height: '100%' };
    return (
      <div style={[styles.base, baseStyle, style]}>
        <AutoSizer>
          {({width, height}) => {
            const tableHeight = height;
            return (
              <Table
                scrollTop={resetScrollTop ? 0 : undefined} // it's needed for https://dremio.atlassian.net/browse/DX-7140
                onScroll={this.handleScroll}
                headerHeight={HEADER_HEIGHT}
                rowCount={tableData.size}
                rowClassName={({index}) => this.rowClassName(sortedTableData.get(index), index)}
                rowHeight={ROW_HEIGHT}
                rowGetter={({index}) => sortedTableData.get(index)}
                sortDirection={sortDirection}
                sortBy={sortBy}
                height={tableHeight}
                width={width}
                sort={this.sort}
                {...tableProps}
              >
                {columns.map((item) =>
                  <Column
                    key={item.key}
                    dataKey={item.key}
                    className={item.className}
                    headerClassName={item.headerClassName}
                    label={item.label}
                    style={item.style}
                    headerRenderer={(options) => this.renderHeader(options, item)}
                    width={item.width || 100}
                    flexGrow={item.flexGrow}
                    disableSort={item.disableSort}
                    cellRenderer={(opts) => this.renderCell(opts, item.key)}
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
      comp.setState({initial: false});
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
  }
  static defaultProps = {
    defer: true
  };
  state = {
    initial: this.props.defer
  }
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
