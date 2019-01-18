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
import uuid from 'uuid';
import { connect } from 'react-redux';
import { getExploreState } from '@app/selectors/explore';

import { ExploreInfoHeader } from '../ExploreInfoHeader';
import InnerJoin from './InnerJoin';

export class InnerJoinController extends Component {
  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map),
    leftColumns: PropTypes.instanceOf(Immutable.List),
    rightColumns: PropTypes.instanceOf(Immutable.List),
    fields: PropTypes.object.isRequired,
    recommendation: PropTypes.object
  };

  static getDefaultDragAreaColumns(props) {
    const { recommendation } = props;
    if (recommendation) {
      const immutableRecommended = Immutable.fromJS(recommendation);
      const cols = immutableRecommended.get('matchingKeys');
      return cols.map((value, key) => {
        const leftColumn = props.leftColumns.find(col => col.get('name') === key) || Immutable.Map({ empty: true });
        const rightColumn = props.rightColumns.find(col => col.get('name') === value) || Immutable.Map({ empty: true });
        const colForDrag = InnerJoinController.getDragAreaColumnModel(leftColumn, rightColumn);
        return colForDrag;
      }).toList();
    }
    return Immutable.List();
  }

  static getDragAreaColumnModel(leftColumn, rightColumn) {
    return Immutable.Map({
      default: leftColumn,
      custom: rightColumn,
      id: uuid.v4()
    });
  }

  constructor(props) {
    super(props);

    this.addEmptyColumnToInnerJoin = this.addEmptyColumnToInnerJoin.bind(this);
    this.addColumnToInnerJoin = this.addColumnToInnerJoin.bind(this);
    this.updateColumns = this.updateColumns.bind(this);

    this.onDragStart = this.onDragStart.bind(this);
    this.onDrop = this.onDrop.bind(this);
    this.removeColumn = this.removeColumn.bind(this);
    this.stopDrag = this.stopDrag.bind(this);
    this.state = {
      isDragInProgress: false,
      columnDragName: '',
      leftColumns: props.leftColumns,
      rightColumns: props.rightColumns,
      columnsInDragArea: InnerJoinController.getDefaultDragAreaColumns(props)
    };
  }

  componentWillReceiveProps(nextProps) {
    const isLeftColumnsChanged = nextProps.leftColumns.size && !this.props.leftColumns.size;
    const isRightColumnsChanged = nextProps.rightColumns.size && !this.props.rightColumns.size;
    if (isLeftColumnsChanged || isRightColumnsChanged) {
      this.setState({
        leftColumns: nextProps.leftColumns,
        rightColumns: nextProps.rightColumns,
        columnsInDragArea: InnerJoinController.getDefaultDragAreaColumns(nextProps)
      });
    }
  }

  onDragStart(type, dragData) {
    this.setState({
      isDragInProgress: true,
      columnDragName: dragData.id,
      type,
      dragColumntableType: type
    });
  }

  onDrop(dropData) {
    const name = this.state.type === 'custom' ? 'rightColumns' : 'leftColumns';
    const columnNameInArea = this.state.type === 'custom' ? 'custom' : 'default';
    const columnName = dropData.id;
    const columnIndexThatWillBeAdded = !this.state.columnsInDragArea
                                                   .find(col => col.getIn([columnNameInArea, 'name']) === columnName)
                                        && this.state[name].findIndex(column => column.get('name') === columnName);
    const column = this.state[name].get(columnIndexThatWillBeAdded);

    if (dropData.id && columnIndexThatWillBeAdded || columnIndexThatWillBeAdded === 0) {
      const mappedColumn = this.state.type === 'custom'
        ? InnerJoinController.getDragAreaColumnModel(Immutable.Map({ empty: true }), column)
        : InnerJoinController.getDragAreaColumnModel(column, Immutable.Map({ empty: true }));
      this.setState({
        isDragInProgress: false,
        type: '',
        columnDragName: ''
      });
      const newColumnsInDragArea = this.state.columnsInDragArea.push(mappedColumn);
      this.setState({ columnsInDragArea : newColumnsInDragArea }, this.updateColumns);
    }
  }

  stopDrag() {
    this.setState({
      isDragInProgress: false,
      type: '',
      columnDragName: ''
    });
  }

  removeColumn(id) {
    const newColumnsInDragArea = this.state.columnsInDragArea.filter(item => item.get('id') !== id);
    this.setState({ columnsInDragArea: newColumnsInDragArea }, this.updateColumns);
  }

  addEmptyColumnToInnerJoin() {
    const empty = InnerJoinController.getDragAreaColumnModel(
      Immutable.Map({ empty: true }),
      Immutable.Map({ empty: true })
    );
    const newColumnsInDragArea = this.state.columnsInDragArea.push(empty);
    this.setState({ columnsInDragArea : newColumnsInDragArea }, this.updateColumns);
  }

  addColumnToInnerJoin({columnName, dragColumnId, dragColumnType}) {
    const { columnsInDragArea } = this.state;
    const index = columnsInDragArea.findIndex(item => dragColumnId === item.get('id'));
    const column = columnsInDragArea.get(index);
    const newId = dragColumnType === 'custom'
      ? column.get('default').get('name') + columnName
      : columnName + column.get('custom').get('name');
    const newColumn = dragColumnType === 'custom'
      ? this.state.rightColumns.find(item => item.get('name') === columnName)
      : this.state.leftColumns.find(item => item.get('name') === columnName);
    const columns = columnsInDragArea.setIn([index, dragColumnId], newId).setIn([index, dragColumnType], newColumn);
    this.setState({
      columnsInDragArea: columns
    }, this.updateColumns);
  }

  updateColumns() {
    const { columnsInDragArea } = this.state;
    const columns = columnsInDragArea.toJS().map(column => {
      return {
        joinedColumn: column.default.name,
        joinedTableKeyColumnName: column.custom.name
      };
    });
    this.props.fields.columns.onChange(columns);
  }

  render() {
    const activeDataset = this.props.fields.activeDataset;
    const dpathArray = (activeDataset.value || activeDataset.initialValue || []);
    const customNameForDisplay = dpathArray && dpathArray[dpathArray.length - 1];
    const defaultNameForDisplay = ExploreInfoHeader.getNameForDisplay(this.props.dataset);

    return (
      <InnerJoin
        dragColumntableType={this.state.dragColumntableType}
        defaultNameForDisplay={defaultNameForDisplay}
        customNameForDisplay={customNameForDisplay}
        fields={this.props.fields}
        stopDrag={this.stopDrag}
        onDragStart={this.onDragStart}
        isDragInProgress={this.state.isDragInProgress}
        handleDrop={this.onDrop}
        leftColumns={this.state.leftColumns}
        rightColumns={this.state.rightColumns}
        columnDragName={this.state.columnDragName}
        type={this.state.type}
        removeColumn={this.removeColumn}
        addColumnToInnerJoin={this.addColumnToInnerJoin}
        addEmptyColumnToInnerJoin={this.addEmptyColumnToInnerJoin}
        columnsInDragArea={this.state.columnsInDragArea}
        dragType='groupBy'
      />
    );
  }
}

function mapStateToProps(state, ownProps) {
  return {
    recommendation: getExploreState(state).join.getIn(['custom', 'recommendation'])
  };
}

export default connect(mapStateToProps, null)(InnerJoinController);
