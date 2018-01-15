/*
 * Copyright (C) 2017 Dremio Corporation
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
import { Cell } from 'fixed-data-table-2';
import Immutable from 'immutable';
import shallowEqual from 'fbjs/lib/shallowEqual';
import { pick } from 'lodash/object';
import { MAP, TEXT, LIST } from 'constants/DataTypes';
import { DATE_TYPES, NUMBER_TYPES } from 'constants/columnTypeGroups';

import dataFormatUtils from 'utils/dataFormatUtils';
import exploreUtils from 'utils/explore/exploreUtils';
import { RED } from 'uiTheme/radium/colors';

import EllipsisIcon from '../EllipsisIcon';

import './ExploreTableCell.less';

const CACHED_ROWS_NUMBER = 50;

export default class ExploreTableCell extends Component {
  static propTypes = {
    columnType: PropTypes.string.isRequired,
    onShowMore: PropTypes.func,
    rowIndex: PropTypes.number,
    columnKey: PropTypes.oneOfType([PropTypes.string, PropTypes.number]),
    data: PropTypes.instanceOf(Immutable.List).isRequired,
    columnName: PropTypes.string,
    columnStatus: PropTypes.string,
    loadNextRows: PropTypes.func,
    isDumbTable: PropTypes.bool,

    selectAll: PropTypes.func,
    tableData: PropTypes.object,
    onCellTextSelect: PropTypes.func,
    selectItemsOfList: PropTypes.func,
    location: PropTypes.object,

    // Cell props
    width: PropTypes.number,
    height: PropTypes.number,
    style: PropTypes.object
  };

  static contextTypes = {
    router: PropTypes.object
  };

  static defaultProps = {
    data: Immutable.List()
  };

  constructor(props) {
    super(props);
    this.onMouseUp = this.onMouseUp.bind(this);
    this.onMouseDown = this.onMouseDown.bind(this);
    this.onMouseEnter = this.onMouseEnter.bind(this);
    this.onEllipsisClick = this.onEllipsisClick.bind(this);
    this.getCellValue = this.getCellValue.bind(this);
    this.state = {
      cellStringBiggerThanCell: false,
      curSelectedCell: '',
      startSelect: false
    };

  }

  componentWillReceiveProps(nextProps) {
    this.tryToLoadNextRows(nextProps.rowIndex, nextProps.data);
  }

  shouldComponentUpdate(nextProps, nextState) {
    const dataPath = [nextProps.rowIndex, 'row', nextProps.columnKey];

    const props = ['rowIndex', 'columnKey', 'columnStatus'];
    return !shallowEqual(pick(nextProps, props), pick(this.props, props))
      || !shallowEqual(nextState, this.state)
      || !Immutable.is(nextProps.data.getIn(dataPath), this.props.data.getIn(dataPath));
  }

  componentDidUpdate(prevProps, prevState) {
    if (this.state.startSelect && !prevState.startSelect) {
      $(document).on('mouseup', this.onMouseUp);
    } else if (!this.state.startSelect && prevState.startSelect) {
      $(document).off('mouseup', this.onMouseUp);
    }
  }

  onMouseDown(e) {
    this.setState({
      curSelectedCell: e.target,
      startSelect: true
    });
  }

  onMouseEnter(e) {
    const elem = $(e.target);
    const fixedTableElement = elem.closest('.public_fixedDataTableCell_main');
    const elemWidth = elem.outerWidth(true);
    const cellStringBiggerThanCell = elemWidth > fixedTableElement.outerWidth();
    if (cellStringBiggerThanCell !== this.state.cellStringBiggerThanCell) {
      this.setState({
        cellStringBiggerThanCell: elemWidth > fixedTableElement.outerWidth()
      });
    }
  }

  onMouseUp() {
    if (this.props.isDumbTable) {
      return null;
    }
    const state = this.props.location.state || {};
    this.setState({ startSelect: false });
    const selection = window.getSelection();
    const selectionData = exploreUtils.getSelectionData(exploreUtils.updateSelectionToSingleCell(selection));
    if (this.prohibitSelection(selectionData)) {
      return null;
    }

    const columnName = selectionData.oRange.startContainer.parentNode.getAttribute('data-columnname');
    const columnText = selectionData.oRange.startContainer.data;
    const columnType = this.props.tableData.get('columns').find(col => col.get('name') === columnName).get('type');
    const cellValue = this.getCellValue();
    if (columnType === LIST) {
      this.props.selectItemsOfList(columnText, columnName, columnType, selectionData);
    } else if (columnType === TEXT) {
      const selectedData = exploreUtils.getSelection(columnText, columnName, selectionData);
      this.context.router.push({
        ...this.props.location,
        state: {
          ...state,
          columnName,
          columnType,
          hasSelection: true,
          selection: selectedData.model
        }}
      );
      if (!state.transformType) {
        this.props.onCellTextSelect({ ...selectedData.position, columnType });
      }
    } else if (columnType !== MAP) {
      const element = selectionData.oRange.startContainer.parentElement;
      this.props.selectAll(element, columnType, columnName, columnText, cellValue);
    }
    window.getSelection().removeAllRanges();
  }

  onEllipsisClick(anchor) {
    const { columnType, columnName } = this.props;
    const value = this.getCellValue();
    const valueUrl = this.getFullValueUrl();
    this.props.onShowMore(value, anchor, columnType, columnName, valueUrl);
  }

  getColumnIndex(target) {
    return $(target).closest('.fixedDataTableCellLayout_main').index();
  }

  getCellValue() {
    const { data, rowIndex, columnKey } = this.props;
    return data.getIn([rowIndex, 'row', columnKey, 'v']);
  }

  getCellType() {
    // columnType can be 'mixed'. The cell type is returned by server.
    const { data, rowIndex, columnKey } = this.props;
    return data.getIn([rowIndex, 'row', columnKey, 't']);
  }

  getFullValueUrl() {
    const { data, rowIndex, columnKey } = this.props;
    return data.getIn([rowIndex, 'row', columnKey, 'u']);
  }

  prohibitSelection(selectionData) {
    const { query } = this.props.location;
    if (this.props.isDumbTable ||
        !selectionData ||
        !selectionData.text ||
        !$(this.state.curSelectedCell).closest('.public_fixedDataTableCell_main')) {
      return true;
    }
    const columnName = selectionData.oRange.startContainer.parentNode.getAttribute('data-columnname');
    const column = this.props.tableData.get('columns').find(col => col.get('name') === columnName);
    if (!column) return true;

    const columnStatus = this.props.tableData.get('columns').find(col => col.get('name') === columnName).get('status');
    return Boolean(query.type && query.type === 'transform' && columnStatus === 'HIGHLIGHTED');
  }

  showEllipsis() {
    const { columnType } = this.props;
    const value = this.getCellValue();
    return value !== null && Boolean(this.state.cellStringBiggerThanCell
                  || this.getFullValueUrl()
                  || columnType === MAP
                  || columnType === LIST);
  }

  tryToLoadNextRows(rowIndex, data) {
    if (rowIndex + CACHED_ROWS_NUMBER >= data.size) {
      this.props.loadNextRows(data.size);
    }
  }

  render() {
    const { rowIndex, data, columnType, style, width, height } = this.props;
    const row = data.get(rowIndex);
    const showEllipsis = this.showEllipsis();
    const cellValue = this.getCellValue();
    const emptyStyle = (cellValue === null || cellValue === '') ? styles.nullCell : {};
    const removedStyle = row && row.deleted ? styles.removedCell : {};

    const isNumericCell = (DATE_TYPES.indexOf(columnType) !== -1 || NUMBER_TYPES.indexOf(columnType) !== -1)
      && cellValue !== null;
    const extraWrapStyle = isNumericCell ? { display: 'flex', justifyContent: 'flex-end' } : {};
    // position is absolute by default. We need set "relative" for align
    const extraCellStyle = isNumericCell ? { position: 'relative' } : {};

    return (
      <Cell
        width={width}
        height={height}
        style={{ ...style, ...extraWrapStyle }}
        className={'explore-cell ' + (showEllipsis ? 'explore-cell-overflow' : '')}
        ref='cellContent'
        onMouseDown={this.onMouseDown}
      >
        {row &&
          <div className='cell-data'>
            <span
              onMouseEnter={this.onMouseEnter}
              style={{...emptyStyle, ...removedStyle, ...extraCellStyle }}
              className='cell-wrap' data-columnname={this.props.columnName}>
              {dataFormatUtils.formatValue(cellValue, this.getCellType() || columnType, row)}
            </span>
          </div>
        }
        {showEllipsis && <EllipsisIcon onClick={this.onEllipsisClick} />}
      </Cell>
    );
  }
}

const styles = {
  removedCell: {
    color: RED,
    textDecoration: 'line-through'
  },
  nullCell: {
    fontStyle: 'italic', textAlign: 'center', width: '95%', color: '#aaa'
  }
};
