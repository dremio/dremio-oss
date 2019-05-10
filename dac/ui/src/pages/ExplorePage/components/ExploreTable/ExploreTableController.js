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
import Immutable from 'immutable';
import { connect } from 'react-redux';
import Radium from 'radium';
import pureRender from 'pure-render-decorator';

import PropTypes from 'prop-types';

import exploreUtils from 'utils/explore/exploreUtils';
import exploreTransforms from 'utils/exploreTransforms';

import { LIST, MAP } from 'constants/DataTypes';

import { getPeekData, getImmutableTable, getPaginationUrl, getExploreState, getColumnFilter } from 'selectors/explore';
import { getViewState } from 'selectors/resources';
import { resetViewState } from 'actions/resources';
import { accessEntity } from 'actions/resources/lru';

import { updateTableColumns } from 'actions/explore/view';
import {
  transformHistoryCheck, performTransform
} from 'actions/explore/dataset/transform';

import { FULL_CELL_VIEW_ID } from 'actions/explore/dataset/data';
import { isSqlChanged } from '@app/sagas/utils';
import { ErrorBoundary } from '@app/components/ErrorBoundary';

import { LOAD_TRANSFORM_CARDS_VIEW_ID } from 'actions/explore/recommended';
import { HIGHLIGHTED_TABLE } from 'uiTheme/radium/colors';

import { constructFullPath } from 'utils/pathUtils';
import { PageTypes } from '@app/pages/ExplorePage/pageTypes';

import ExploreTable from './ExploreTable';
import ExploreCellLargeOverlay from './ExploreCellLargeOverlay';
import DropdownForSelectedText from './DropdownForSelectedText';

@Radium
@pureRender
export class ExploreTableController extends Component {
  static propTypes = {
    pageType: PropTypes.string,
    dataset: PropTypes.instanceOf(Immutable.Map),
    tableData: PropTypes.instanceOf(Immutable.Map).isRequired,
    previewVersion: PropTypes.string,
    paginationUrl: PropTypes.string,
    isDumbTable: PropTypes.bool,
    exploreViewState: PropTypes.instanceOf(Immutable.Map).isRequired,
    cardsViewState: PropTypes.instanceOf(Immutable.Map),
    fullCellViewState: PropTypes.instanceOf(Immutable.Map),
    currentSql: PropTypes.string,
    queryContext: PropTypes.instanceOf(Immutable.List),
    location: PropTypes.object,
    dragType: PropTypes.string,
    width: PropTypes.number,
    height: PropTypes.number,
    widthScale: PropTypes.number,
    rightTreeVisible: PropTypes.bool,
    sqlSize: PropTypes.number,
    sqlState: PropTypes.bool,
    isResizeInProgress: PropTypes.bool,
    children: PropTypes.node,
    getTableHeight: PropTypes.func,
    shouldRenderInvisibles: PropTypes.bool, // this is a dangerous/experimental option, it can interfere with other features (e.g. selection dropdown)
    columnFilter: PropTypes.string,
    // Actions
    resetViewState: PropTypes.func,
    updateTableColumns: PropTypes.func,
    transformHistoryCheck: PropTypes.func,
    performTransform: PropTypes.func,
    confirmTransform: PropTypes.func,
    accessEntity: PropTypes.func.isRequired
  };

  static contextTypes = {
    router: PropTypes.object
  };

  static defaultProps = {
    dataset: Immutable.Map()
  }

  transformPreconfirmed = false; // eslint-disable-line react/sort-comp

  constructor(props) {
    super(props);
    this.openDetailsWizard = this.openDetailsWizard.bind(this);

    this.handleCellTextSelect = this.handleCellTextSelect.bind(this);
    this.handleCellShowMore = this.handleCellShowMore.bind(this);
    this.selectAll = this.selectAll.bind(this);
    this.selectItemsOfList = this.selectItemsOfList.bind(this);

    this.state = {
      activeTextSelect: null,
      openPopover: false,
      activeCell: null
    };
  }

  componentWillReceiveProps(nextProps) {
    const { isGrayed } = this.state;
    const isContextChanged = this.isContextChanged(nextProps);

    const newIsGreyed = nextProps.pageType === PageTypes.default &&  (this.isSqlChanged(nextProps) || isContextChanged);
    if (isGrayed !== newIsGreyed) {
      this.setState({ isGrayed: newIsGreyed });
    }

    const nextVersion = nextProps.tableData && nextProps.tableData.get('version');

    if (nextVersion && nextVersion !== this.props.tableData.get('version')) {
      this.transformPreconfirmed = false;
      this.props.accessEntity('tableData', nextVersion);
    }
  }

  getNextTableAfterTransform({data, tableData}) {
    const hash = {
      DROP: () => exploreTransforms.dropColumn({name: data.columnName, table: tableData}),
      RENAME: () => exploreTransforms.renameColumn({
        name: data.columnName, nextName: data.newColumnName, table: tableData
      }),
      DESC: () => tableData,
      ASC: () => tableData,
      MULTIPLY: () => tableData
    };
    return exploreTransforms.isTransformOptimistic(data.type)
      ? (hash[data.type] && hash[data.type]()) || null
      : null;
  }

  selectAll(elem, columnType, columnName, cellText, cellValue) {
    const { pathname, query, state } = this.props.location;
    const isNull = cellValue === null;
    const text = isNull ? null : cellText;
    const length = isNull ? 0 : text.length;
    const model = {
      cellText: text,
      offset: 0,
      columnName,
      length
    };
    const position = exploreUtils.selectAll(elem);
    this.context.router.push({
      pathname,
      query,
      state: { ...state, columnName, columnType, hasSelection: true, selection: Immutable.fromJS(model) }
    });
    this.handleCellTextSelect({ ...position, columnType });
  }

  selectItemsOfList(columnText, columnName, columnType, selection) {
    const { router } = this.context;
    const { location } = this.props;
    const content = exploreUtils.getSelectionForList(columnText, columnName, selection);
    if (!content) {
      return;
    }

    router.push({
      ...location,
      state: {
        ...location.state,
        columnName,
        columnType,
        listOfItems: content.listOfItems,
        hasSelection: true,
        selection: Immutable.fromJS(content.model)
      }
    });
    this.handleCellTextSelect({ ...content.position, columnType });
  }

  handleCellTextSelect(activeTextSelect) {
    this.setState({
      activeTextSelect,
      openPopover: !this.props.isDumbTable
    });
  }

  handleCellShowMore(cellValue, anchor, columnType, columnName, valueUrl) {
    this.setState({
      activeCell: {
        cellValue,
        anchor,
        columnType,
        columnName,
        // for dumb table do not try to load full cell value, as server does not support this functionality
        // for that case. Lets show truncated values that was loaded.
        isTruncatedValue: Boolean(this.props.isDumbTable && valueUrl),
        valueUrl: this.props.isDumbTable ? null : valueUrl
      }
    });
  }

  preconfirmTransform = () => {
    return new Promise((resolve) => {
      if (!this.transformPreconfirmed) {
        this.props.transformHistoryCheck(this.props.dataset, () => {
          this.transformPreconfirmed = true;
          resolve();
        });
      } else {
        resolve();
      }
    });
  }

  isSqlChanged(nextProps) {
    if (!nextProps.dataset || !this.props.dataset) {
      return false;
    }
    const nextSql = nextProps.dataset.get('sql');
    const { currentSql } = nextProps;
    return isSqlChanged(nextSql, currentSql);
  }

  isContextChanged(nextProps) {
    if (!nextProps.dataset || !this.props.dataset) {
      return false;
    }
    const nextContext = nextProps.dataset.get('context');
    const { queryContext } = nextProps;
    return nextContext && queryContext && constructFullPath(nextContext) !== constructFullPath(queryContext);
  }

  openDetailsWizard({ detailType, columnName, toType = null }) {
    const { dataset, currentSql, queryContext, exploreViewState } = this.props;

    const callback = () => {
      const { router } = this.context;
      const { location } = this.props;
      const column = this.props.tableData.get('columns').find(col => col.get('name') === columnName).toJS();
      const nextLocation = exploreUtils.getLocationToGoToTransformWizard({
        detailType, column, toType, location
      });

      router.push(nextLocation);
    };
    this.props.performTransform({
      dataset,
      currentSql,
      queryContext,
      viewId: exploreViewState.get('viewId'),
      callback
    });
  }

  makeTransform = (data, checkHistory = true) => {
    const { dataset, currentSql, queryContext, exploreViewState } = this.props;
    const doTransform = () => {
      this.props.performTransform({
        dataset,
        currentSql,
        queryContext,
        transformData: exploreUtils.getMappedDataForTransform(data),
        viewId: exploreViewState.get('viewId'),
        nextTable: this.getNextTableAfterTransform({data, tableData: this.props.tableData})
      });
    };
    if (checkHistory) {
      this.props.transformHistoryCheck(dataset, doTransform);
    } else {
      doTransform();
    }
  }

  updateColumnName = (oldColumnName, e) => {
    const newColumnName = e.target.value;
    if (newColumnName !== oldColumnName) {
      this.makeTransform({ type: 'RENAME', columnName: oldColumnName, newColumnName }, false);
    }
  }

  hideCellMore = () => this.setState({activeCell: null})
  hideDrop = () => this.setState({
    activeTextSelect: null,
    openPopover: false
  });

  decorateTable = (tableData) => {
    const { location } = this.props;
    if (location.query.type === 'transform') {
      const transform = exploreUtils.getTransformState(location);
      const columnType = transform.get('columnType');
      const initializeColumnTypeForExtract = columnType === LIST || columnType === MAP
        ? columnType
        : 'default';
      const isDefault = initializeColumnTypeForExtract === 'default';

      if (!exploreUtils.transformHasSelection(transform) && isDefault) {
        const columnName = transform.get('columnName');
        const columns = tableData.get('columns').map(column => {
          if (column.get('name') === columnName && !column.get('status')) {
            return column.set('status', 'TRANSFORM_ON').set('color', HIGHLIGHTED_TABLE);
          }
          return column;
        });

        return tableData.set('columns', columns);
      }
    }

    return tableData;
  }

  renderExploreCellLargeOverlay() {
    return this.state.activeCell && !this.props.location.query.transformType
      ? (
        <ErrorBoundary>
          <ExploreCellLargeOverlay
            {...this.state.activeCell}
            isDumbTable={this.props.isDumbTable}
            fullCellViewState={this.props.fullCellViewState}
            onSelect={this.handleCellTextSelect}
            hide={this.hideCellMore}
            openPopover={this.state.openPopover}
            selectAll={this.selectAll}
          />
        </ErrorBoundary>
      )
      : null;
  }

  render() {
    const tableData = this.decorateTable(this.props.tableData);
    const rows = tableData.get('rows');
    const columns =  exploreUtils.getFilteredColumns(tableData.get('columns'), this.props.columnFilter);

    return (
      <div style={styles.base}>
        <ExploreTable
          pageType={this.props.pageType}
          dataset={this.props.dataset}
          rows={rows}
          columns={columns}
          paginationUrl={this.props.paginationUrl}
          exploreViewState={this.props.exploreViewState}
          cardsViewState={this.props.cardsViewState}
          isResizeInProgress={this.props.isResizeInProgress}
          widthScale={this.props.widthScale}
          openDetailsWizard={this.openDetailsWizard}
          makeTransform={this.makeTransform}
          preconfirmTransform={this.preconfirmTransform}
          width={this.props.width}
          updateColumnName={this.updateColumnName}
          height={this.props.height}
          dragType={this.props.dragType}
          sqlSize={this.props.sqlSize}
          sqlState={this.props.sqlState}
          rightTreeVisible={this.props.rightTreeVisible}
          onCellTextSelect={this.handleCellTextSelect}
          onCellShowMore={this.handleCellShowMore}
          selectAll={this.selectAll}
          selectItemsOfList={this.selectItemsOfList}
          isDumbTable={this.props.isDumbTable}
          getTableHeight={this.props.getTableHeight}
          isGrayed={this.state.isGrayed}
          shouldRenderInvisibles={this.props.shouldRenderInvisibles}
          />
        {this.renderExploreCellLargeOverlay()}
        {this.state.activeTextSelect &&
          <DropdownForSelectedText
            dropPositions={Immutable.fromJS(this.state.activeTextSelect)}
            openPopover={this.state.openPopover}
            hideDrop={this.hideDrop}/>}
        {this.props.children}
      </div>
    );
  }
}

function mapStateToProps(state, ownProps) {
  const location = state.routing.locationBeforeTransitions || {};
  const { dataset } = ownProps;
  const datasetVersion = dataset && dataset.get('datasetVersion');
  const paginationUrl = getPaginationUrl(state, datasetVersion);
  const exploreState = getExploreState(state);
  let explorePageProps = null;
  if (exploreState) {
    explorePageProps = {
      currentSql: exploreState.view.currentSql,
      queryContext: exploreState.view.queryContext,
      isResizeInProgress: exploreState.ui.get('isResizeInProgress')
    };
  }

  let tableData = ownProps.tableData;
  const previewVersion = location.state && location.state.previewVersion;
  if (!ownProps.isDumbTable) {
    if (ownProps.pageType === PageTypes.default
      || !previewVersion
      || ownProps.exploreViewState.get('isAutoPeekFailed')) {
      tableData = getImmutableTable(state, datasetVersion);
    } else {
      tableData = getPeekData(state, previewVersion);
    }
  }

  return {
    tableData: tableData || Immutable.fromJS({rows: [], columns: []}),
    columnFilter: getColumnFilter(state),
    previewVersion,
    paginationUrl,
    location,
    exploreViewState: ownProps.exploreViewState,
    fullCellViewState: ownProps.isDumbTable ? ownProps.exploreViewState : getViewState(state, FULL_CELL_VIEW_ID),
    cardsViewState: ownProps.isDumbTable ? ownProps.exploreViewState
      : getViewState(state, LOAD_TRANSFORM_CARDS_VIEW_ID),
    ...explorePageProps
  };
}

export default connect(mapStateToProps, {
  resetViewState,
  updateTableColumns,
  transformHistoryCheck,
  performTransform,
  accessEntity
})(ExploreTableController);

const styles = {
  base: {
    position: 'relative',
    overflowY: 'visible',
    opacity: 1,
    display: 'flex',
    flexGrow: 1
  },
  buttons: {
    display: 'flex',
    justifyContent: 'space-around',
    flexWrap: 'wrap'
  },
  modalTitle: {
    textAlign: 'center',
    marginTop: 10,
    marginBottom: 15
  }
};
