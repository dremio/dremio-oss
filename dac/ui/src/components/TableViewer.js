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
import PropTypes from 'prop-types';
import Immutable from 'immutable';
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
    tableData: ImmutablePropTypes.listOf(PropTypes.shape({
      data: PropTypes.oneOfType([PropTypes.objectOf(cellType), PropTypes.arrayOf(cellType)]).isRequired,
      rowClassName: PropTypes.string
    })),
    className: PropTypes.string,
    rowHeight: PropTypes.number,
    columns: PropTypes.arrayOf(
      PropTypes.shape({
        key: PropTypes.oneOfType([PropTypes.number, PropTypes.string]),
        label: PropTypes.string.isRequired, // todo what do we allow here????
        align: PropTypes.oneOf(Object.values(cellAlignment)),
        flexGrow: PropTypes.number,
        width: PropTypes.number
      })
    ).isRequired
    // other props passed to fixed-data-table-2 Table
  }

  static defaultProps = {
    tableData: Immutable.List(),
    rowHeight: 30
  }

  getColumn = (column, columnIndex) => {
    return this.getColumnByConfig(column, columnIndex);
  }

  getColumnByConfig = ( /* columnConfig */ {
    key,
    label,
    align = cellAlignment.left,
    flexGrow = 0,
    width = 20
  }, columnIndex) => {
    return (<Column
        key={key === undefined ? columnIndex : key}
        flexGrow={flexGrow}
        width={width}
        columnKey={key}
        align={align}
        header={<Cell>{label}</Cell>}
        cell={this.renderCell(label)}
      />);
  }

  getRowClassName = (rowIndex) => {
    const {
      tableData
    } = this.props;

    return tableData.get(rowIndex).rowClassName;
  }

  renderCell = label => (/* cellProps */ {
    rowIndex,
    columnKey,
    ...cellProps
  }) => {
    const {
      tableData
    } = this.props;

    return (<Cell>
      <span data-qa={label}>
        {tableData.get(rowIndex).data[columnKey]}
      </span>
    </Cell>);
  }

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

            return (
              <Table
                rowHeight={rowHeight}
                headerHeight={30}
                width={width}
                height={height}
                rowsCount={tableData.size}
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
