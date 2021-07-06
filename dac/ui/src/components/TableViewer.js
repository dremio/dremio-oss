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
import PropTypes from 'prop-types';
import Immutable, { List } from 'immutable';
import { Column, Table, Cell } from 'fixed-data-table-2';
import classNames from 'classnames';
import { AutoSizer } from 'react-virtualized';
import ImmutablePropTypes from 'react-immutable-proptypes';
import { tableViewer as tableViewerCls, tableViewerContainer } from './TableViewer.less';

export const cellAlignment = {
  left: 'left',
  center: 'center',
  right: 'right'
};

const cellType = PropTypes.node;

export default class TableViewer extends Component {

  static propTypes = {
    tableData: PropTypes.oneOfType([
      ImmutablePropTypes.listOf(PropTypes.shape({
        data: PropTypes.oneOfType([PropTypes.objectOf(cellType), PropTypes.arrayOf(cellType)]).isRequired,
        rowClassName: PropTypes.string
      })),
      PropTypes.arrayOf(PropTypes.shape({
        data: PropTypes.oneOfType([PropTypes.objectOf(cellType), PropTypes.arrayOf(cellType)]).isRequired,
        rowClassName: PropTypes.string
      }))
    ]),
    className: PropTypes.string,
    rowHeight: PropTypes.number,
    columns: PropTypes.arrayOf(
      PropTypes.shape({
        key: PropTypes.oneOfType([PropTypes.number, PropTypes.string]),
        label: PropTypes.string.isRequired, // todo what do we allow here????
        renderer: PropTypes.func,
        align: PropTypes.oneOf(Object.values(cellAlignment)),
        flexGrow: PropTypes.number,
        width: PropTypes.number
      })
    ).isRequired
    // other props passed to fixed-data-table-2 Table
  };

  static defaultProps = {
    tableData: Immutable.List(),
    rowHeight: 30
  };

  getColumn = (column, columnIndex) => {
    return this.getColumnByConfig(column, columnIndex);
  };

  getColumnByConfig = ( /* columnConfig */ {
    key,
    label,
    renderer,
    align = cellAlignment.left,
    flexGrow = 0,
    width = 20,
    fixed = false
  }, columnIndex) => {
    return (<Column
      key={key === undefined ? columnIndex : key}
      flexGrow={flexGrow}
      width={width}
      columnKey={key}
      align={align}
      style={{borderRight: '#ccc', background: 'inherit'}}
      header={renderer ? renderer(label) : <Cell>{label}</Cell>}
      cell={this.renderCell(label)}
      fixed={fixed}
    />);
  };

  getRowClassName = (rowIndex) => {
    const {
      tableData
    } = this.props;
    if (List.isList(tableData)) {
      return tableData.get(rowIndex).rowClassName;
    }
    return tableData[rowIndex].rowClassName;
  };

  renderCell = label => (/* cellProps */ {
    rowIndex,
    columnKey,
    ...cellProps  // eslint-disable-line @typescript-eslint/no-unused-vars
  }) => {
    const {
      tableData
    } = this.props;
    const currentTableData = List.isList(tableData) ? tableData.get(rowIndex) : tableData[rowIndex];
    return (<Cell>
      <span data-qa={label}>
        {currentTableData.data[columnKey]}
      </span>
    </Cell>);
  };

  render() {
    return (
      <div className={tableViewerContainer}>
        <AutoSizer>
          {({ height, width }) => {
            const {
              rowHeight,
              tableData,
              className,
              columns,
              ...tableProps
            } = this.props;

            const tableColumns = columns.map(this.getColumn);
            const tableSize = List.isList(tableData) ? tableData.size : tableData.length;

            return (
              <Table
                rowHeight={rowHeight}
                headerHeight={30}
                width={width}
                height={height}
                rowsCount={tableSize}
                className={classNames([tableViewerCls, className])}
                rowClassNameGetter={this.getRowClassName}
                {...tableProps}>
                {tableColumns}
              </Table>
            );
          }}
        </AutoSizer>
      </div>
    );
  }
}
