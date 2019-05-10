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
import { Cell } from 'fixed-data-table-2';
import Immutable from 'immutable';
import shallowEqual from 'fbjs/lib/shallowEqual';
import { pick } from 'lodash/object';
import { LIST, MAP, TEXT } from 'constants/DataTypes';
import { DATE_TYPES, NUMBER_TYPES } from 'constants/columnTypeGroups';

import dataFormatUtils from 'utils/dataFormatUtils';
import exploreUtils from 'utils/explore/exploreUtils';
import { RED } from 'uiTheme/radium/colors';

import { withLocation } from 'containers/dremioLocation';
import EllipsisIcon from '../EllipsisIcon';

import './ExploreTableCell.less';

const CACHED_ROWS_NUMBER = 50;

export class ExploreTableCellView extends Component {
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
    columns: PropTypes.instanceOf(Immutable.List),
    onCellTextSelect: PropTypes.func,
    selectItemsOfList: PropTypes.func,
    shouldRenderInvisibles: PropTypes.bool, // this is a dangerous/experimental option, it can interfere with other features (e.g. selection dropdown)

    // Cell props
    width: PropTypes.number,
    height: PropTypes.number,
    style: PropTypes.object,

    // context properties
    location: PropTypes.object
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
      innerContentWidth: 0, // will be used to calculate should we display ellipses or not. Zero value is ignored
      curSelectedCell: '',
      startSelect: false
    };

  }

  componentWillReceiveProps(nextProps) {
    this.tryToLoadNextRows(nextProps.rowIndex, nextProps.data);
  }

  shouldComponentUpdate(nextProps, nextState) {
    const dataPath = [nextProps.rowIndex, 'row', nextProps.columnKey];

    const props = ['rowIndex', 'columnKey', 'columnStatus', 'columnType', 'width'];
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

    this.setState({
      innerContentWidth: elem.outerWidth(true)
    });
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

    const columnName = this.getColumnName(selectionData.oRange.startContainer);
    const columnText = selectionData.oRange.startContainer.data;
    const columnType = this.props.columns.find(col => col.get('name') === columnName).get('type');
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
    const columnName = this.getColumnName(selectionData.oRange.startContainer);
    const column = this.props.columns.find(col => col.get('name') === columnName);
    if (!column) return true;

    const columnStatus = this.props.columns.find(col => col.get('name') === columnName).get('status');
    return Boolean(query.type && query.type === 'transform' && columnStatus === 'HIGHLIGHTED');
  }

  // When shouldRenderInvisibles= true, there is additional wrapping above the cell values. We have to find a cell wrapper to determine a cell wrapper.
  // Without that following selenium tests failed.
  // Test / selenium / smoketests.016_extract.Extract from menu
  // Test / selenium / smoketests.018_transformation.transformation DX-3716#Case1 #Case4 #Case5

  getColumnName(cellContentEl) {
    let currentElement = cellContentEl;

    while (currentElement && (!currentElement.className || currentElement.className.indexOf('cell-wrap') < 0)) {
      currentElement = currentElement.parentNode;
    }

    if (currentElement) {
      return currentElement.getAttribute('data-columnname');
    }
    return null;
  }

  showEllipsis() {
    const {
      columnType,
      width
    } = this.props;
    const value = this.getCellValue();
    return value !== null && Boolean(this.state.innerContentWidth > width
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
    const { rowIndex, data, columnType, style, width, height, shouldRenderInvisibles } = this.props;
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

    const className = 'explore-cell ' + (showEllipsis ? 'explore-cell-overflow' : '') +
      (shouldRenderInvisibles ? ' explore-call-show-invisible' : '');

    return (
      <Cell
        width={width}
        height={height}
        style={{ ...style, ...extraWrapStyle }}
        className={className}
        ref='cellContent'
        onMouseDown={this.onMouseDown}
      >
        {row &&
          <div className='cell-data'>
            <span
              onMouseEnter={this.onMouseEnter}
              style={{...emptyStyle, ...removedStyle, ...extraCellStyle }}
              className='cell-wrap' data-columnname={this.props.columnName}>
              {dataFormatUtils.formatValue(cellValue, this.getCellType() || columnType, row, shouldRenderInvisibles)}
            </span>
          </div>
        }
        {showEllipsis && <EllipsisIcon onClick={this.onEllipsisClick} />}
      </Cell>
    );
  }
}

export default withLocation(ExploreTableCellView);

const styles = {
  removedCell: {
    color: RED,
    textDecoration: 'line-through'
  },
  nullCell: {
    fontStyle: 'italic', textAlign: 'center', width: '95%', color: '#aaa'
  }
};
