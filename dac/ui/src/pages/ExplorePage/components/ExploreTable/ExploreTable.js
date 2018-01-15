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
import { PureComponent } from 'react';
import Radium from 'radium';
import PropTypes from 'prop-types';
import ReactDOM from 'react-dom';
import shallowEqual from 'shallowequal';
import $ from 'jquery';
import Immutable from 'immutable';
import { Column, Table } from 'fixed-data-table-2';
import { AutoSizer } from 'react-virtualized';
import { injectIntl } from 'react-intl';

import { DEFAULT_ROW_HEIGHT, MIN_COLUMN_WIDTH } from 'uiTheme/radium/sizes';

import ViewStateWrapper from 'components/ViewStateWrapper';
import ViewCheckContent from 'components/ViewCheckContent';
import ExploreTableCell from './ExploreTableCell';
import ColumnHeader from './ColumnHeader';

import './ExploreTable.less';

const TIME_BEFORE_SPINNER = 1500;
export const RIGHT_TREE_OFFSET = 251;

@injectIntl
@Radium
export default class ExploreTable extends PureComponent {
  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map),
    tableData: PropTypes.instanceOf(Immutable.Map),
    paginationUrl: PropTypes.string,
    transform: PropTypes.instanceOf(Immutable.Map),
    exploreViewState: PropTypes.instanceOf(Immutable.Map),
    cardsViewState: PropTypes.instanceOf(Immutable.Map),
    openDetailsWizard: PropTypes.func.isRequired,
    updateColumnName: PropTypes.func.isRequired,
    dragType: PropTypes.string,
    sqlSize: PropTypes.number,
    sqlState: PropTypes.bool,
    isResizeInProgress: PropTypes.bool,
    pageType: PropTypes.string,
    makeTransform: PropTypes.func.isRequired,
    preconfirmTransform: PropTypes.func.isRequired,
    toggleDropdown: PropTypes.func,
    onCellTextSelect: PropTypes.func,
    onCellShowMore: PropTypes.func,
    selectAll: PropTypes.func,
    loadNextRows: PropTypes.func,
    selectItemsOfList: PropTypes.func,
    isDumbTable: PropTypes.bool,
    getTableHeight: PropTypes.func,
    location: PropTypes.object,
    height: PropTypes.number,
    isGrayed: PropTypes.bool,
    intl: PropTypes.object.isRequired
  };

  static getCellStyle(column) {
    return {width: '100%', display: 'inline-block', backgroundColor: column.color};
  }

  static getCellWidth(column, defaultWidth) {
    return column.width || defaultWidth;
  }

  static tableWidth(width, wrapper, widthScale, rightTreeVisible) {
    return (width || wrapper && (wrapper.offsetWidth - (rightTreeVisible ? RIGHT_TREE_OFFSET : 0))) * widthScale;
  }

  static tableHeight(height, wrapper, nodeOffsetTop) {
    return (height || wrapper && (wrapper.offsetHeight - nodeOffsetTop));
  }

  static getDefaultColumnWidth(widthThatWillBeSet, columns) {
    const size = columns.length || 1;
    const numberOfColumnsWithNonDefaultWidth = columns.filter(col => col.width).length;
    const widthReservedByUserActions = columns.map(col => col.width || 0).reduce((prev, cur) => prev + cur, 0);
    const defaultColumnWidth = (widthThatWillBeSet - widthReservedByUserActions) /
      ((size - numberOfColumnsWithNonDefaultWidth) || 1);
    return defaultColumnWidth > MIN_COLUMN_WIDTH ? defaultColumnWidth : MIN_COLUMN_WIDTH;
  }

  static getTableSize({widthScale = 1, width, height, maxHeight, rightTreeVisible}, wrappers, columns, nodeOffsetTop) {
    if (!wrappers[0] && !wrappers[1] && !width && !height) {
      console.error('Can\'t find wrapper element, size maybe wrong');
    }

    const widthThatWillBeSet = ExploreTable.tableWidth(width, wrappers[0], widthScale, rightTreeVisible);
    const heightThatWillBeSet = ExploreTable.tableHeight(height, wrappers[1], nodeOffsetTop);
    const defaultColumnWidth = ExploreTable.getDefaultColumnWidth(widthThatWillBeSet, columns);

    return Immutable.Map({
      width: widthThatWillBeSet,
      height: heightThatWillBeSet,
      maxHeight,
      defaultColumnWidth
    });
  }

  constructor(props) {
    super(props);
    this.updateSize = this.updateSize.bind(this);
    this.handleColumnResizeEnd = this.handleColumnResizeEnd.bind(this);
    this.loadNextRows = this.loadNextRows.bind(this);
    const columns = props.tableData && props.tableData.get('columns');

    this.state = {
      defaultColumnWidth: 0,
      size: Immutable.Map({ width: 0, height: 0, defaultColumnWidth: 0 }),
      columns: columns || Immutable.List([])
    };
  }

  componentDidMount() {
    this.updateSize();
  }

  componentWillReceiveProps(nextProps) {
    const needUpdateColumns = this.needUpdateColumns(nextProps);
    if (needUpdateColumns) {
      this.updateColumns(nextProps);
    }
  }

  componentDidUpdate(prevProps) {
    if (!shallowEqual(prevProps, this.props)) {
      this.updateSize();
    }
    if (this.props.dataset.get('datasetVersion') !== prevProps.dataset.get('datasetVersion')) {
      this.lastLoaded = 0;
    }

    // https://dremio.atlassian.net/browse/DX-5848
    // https://github.com/facebook/fixed-data-table/issues/401
    // https://github.com/facebook/fixed-data-table/issues/415
    $('.fixedDataTableCellLayout_columnResizerContainer').on('mousedown', this.removeResizerHiddenElem);
  }

  componentWillUnmount() {
    $('.fixedDataTableCellLayout_columnResizerContainer').off('mousedown', this.removeResizerHiddenElem);
  }

  loadNextRows(offset, loadNew) {
    if (this.props.isDumbTable) {
      return;
    }
    if (this.lastLoaded !== offset || loadNew) {
      this.lastLoaded = offset;
      const datasetVersion = this.props.dataset.get('datasetVersion');
      this.props.loadNextRows(datasetVersion, this.props.paginationUrl, offset);
    }
  }

  getScrollToColumn() {
    const columns = this.state.columns;
    const index = columns.findIndex(val => val.get('status') === 'HIGHLIGHTED');

    if (index === -1) {
      return null;
    }

    const defaultColumnWidth = this.state.size.get('defaultColumnWidth');
    const tableWidth = this.state.size.get('width');
    const visibleColumns = tableWidth / defaultColumnWidth;
    const offset = Math.floor(visibleColumns / 2);
    return (index + offset) > columns.size - 1
      ? index + (columns.size - index - 1)
      : index + offset;
  }

  getColumnsToCompare(columns) {
    return columns.map(c => {
      const { hidden, index, name, type, status } = c.toJS();
      return Immutable.Map({ hidden, index, name, type, status });
    });
  }

  shouldShowNoData(viewState) {
    const { tableData, dataset } = this.props;
    const rows = tableData.get('rows');
    return !viewState.get('isInProgress') &&
      !viewState.get('isFailed') &&
      Boolean(dataset.get('datasetVersion')) &&
      !rows.size;
  }

  needUpdateColumns(nextProps) {
    const newColumns = nextProps.tableData && nextProps.tableData.get('columns')
      || Immutable.List();
    if (!newColumns.size) {
      return false;
    }
    if (!this.state.columns.size) {
      return true;
    }
    return !this.getColumnsToCompare(newColumns).equals(this.getColumnsToCompare(this.state.columns));
  }

  updateColumns(props) {
    const columns = props.tableData.get('columns');
    this.setState({ columns });
  }

  updateSize = () => {
    const columns = this.state.columns.toJS().filter(col => !col.hidden);
    const node = ReactDOM.findDOMNode(this);
    const nodeOffsetTop = $(node).offset().top;
    const wrapperForWidth = $(node).parents('.table-parent')[0];
    const wrapperForHeight = document.querySelector('#grid-page');
    const wrappers = [ wrapperForWidth, wrapperForHeight ];
    //this need to fix DX-8244 due to re-render of table because of horizontal scroll clipping
    let height = this.props.height;

    if (this.props.getTableHeight) {
      height = this.props.getTableHeight(node);
    }

    const size = ExploreTable.getTableSize({...this.props, height}, wrappers, columns, nodeOffsetTop);
    this.setState(state => {
      if (!state.size.equals(size)) {
        return {size};
      }
    });
  }

  handleColumnResizeEnd(width, index) {
    const { columns } = this.state;

    // https://dremio.atlassian.net/browse/DX-5848
    // https://github.com/facebook/fixed-data-table/issues/401
    // https://github.com/facebook/fixed-data-table/issues/415
    $('.fixedDataTableColumnResizerLineLayout_main').addClass('fixedDataTableColumnResizerLineLayout_hiddenElem');
    this.setState({
      columns: columns.setIn([index, 'width'], width)
    });
    this.updateSize();
  }

  removeResizerHiddenElem = () => {
    $('.fixedDataTableColumnResizerLineLayout_main').removeClass('fixedDataTableColumnResizerLineLayout_hiddenElem');
  }

  renderColumnHeader(column, width) {
    return (
      <ColumnHeader
        pageType={this.props.pageType}
        columnsCount={this.props.tableData.get('columns').size}
        isResizeInProgress={this.props.isResizeInProgress}
        dragType={this.props.dragType}
        updateColumnName={this.props.updateColumnName}
        column={column}
        width={width}
        defaultColumnWidth={this.state.size.get('defaultColumnWidth')}
        openDetailsWizard={this.props.openDetailsWizard}
        makeTransform={this.props.makeTransform}
        preconfirmTransform={this.props.preconfirmTransform}
        isDumbTable={this.props.isDumbTable}
      />
    );
  }

  renderCell(column) {
    const cellStyle = ExploreTable.getCellStyle(column);
    return (
      <ExploreTableCell
        columnType={column.type}
        columnName={column.name}
        columnStatus={column.status}
        onShowMore={this.props.onCellShowMore}
        loadNextRows={this.loadNextRows}
        style={cellStyle}
        data={this.props.tableData.get('rows')}
        selectAll={this.props.selectAll}
        selectItemsOfList={this.props.selectItemsOfList}
        onCellTextSelect={this.props.onCellTextSelect}
        tableData={this.props.tableData}
        isDumbTable={this.props.isDumbTable}
        location={this.props.location}
      />
    );
  }

  renderColumns() {
    const columns = this.state.columns.toJS();
    const filteredColumns = columns.filter((column) => !column.hidden);
    return filteredColumns.map(column => {
      const cellWidth = column.width || this.state.size.get('defaultColumnWidth');
      return (
        <Column
          key={column.name}
          header={this.renderColumnHeader(column, cellWidth)}
          width={cellWidth}
          isResizable
          allowCellsRecycling
          columnKey={column.index}
          cell={this.renderCell(column)}
        />
      );
    });
  }

  renderTable() {
    const { dataset, tableData } = this.props;
    const scrollToColumn = this.getScrollToColumn();
    if ((!dataset.get('isNewQuery')) && tableData.get('columns').size) {
      return (
        <AutoSizer>
          { ({height, width}) => (
            <Table
              rowHeight={DEFAULT_ROW_HEIGHT}
              ref='table'
              rowsCount={tableData.get('rows').size}
              width={width}
              height={height}
              overflowX='auto'
              overflowY='auto'
              scrollToColumn={scrollToColumn}
              onColumnResizeEndCallback={this.handleColumnResizeEnd}
              isColumnResizing={false}
              headerHeight={DEFAULT_ROW_HEIGHT}>
              {this.renderColumns()}
            </Table>) }
        </AutoSizer>
      );
    }
  }

  render() {
    const columns = this.state.columns;
    const height = this.state.size.get('height');
    const { exploreViewState, cardsViewState, pageType, intl } = this.props;
    const showMessage = pageType === 'default';
    const viewState = pageType === 'default' || !(cardsViewState && cardsViewState.size)
      || exploreViewState.get('isInProgress') ? exploreViewState : cardsViewState;

    return (
      <div className='fixed-data-table' style={{ width: '100%' }}>
        <ViewStateWrapper
          spinnerDelay={columns.size ? TIME_BEFORE_SPINNER : 0}
          viewState={viewState}
          showMessage={showMessage}
          hideChildrenWhenFailed={false}
          >
          {this.props.isGrayed && <div data-qa='table-grayed-out' style={styles.grayed}/>}
          {this.renderTable()}
          <ViewCheckContent
            message={intl.formatMessage({ id: 'Dataset.NoData' })}
            viewState={viewState}
            dataIsNotAvailable={this.shouldShowNoData(viewState)}
            customStyle={{
              bottom: height / 2,
              position: 'absolute',
              height: 0
            }}
            />
        </ViewStateWrapper>
      </div>
    );
  }
}

const styles = {
  grayed: {
    position: 'absolute',
    height: '100%',
    width: '100%',
    backgroundColor: 'rgba(255, 255, 255, 0.4)',
    zIndex: 2,
    pointerEvents: 'none'
  }
};
