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
import Immutable from 'immutable';

import ExploreDragArea from 'pages/ExplorePage/components/ExploreDragArea';
import { isAlreadySelected } from 'utils/explore/aggregateUtils';
import ColumnDragItem from 'utils/ColumnDragItem';
import { rowMargin } from '@app/uiTheme/less/forms.less';

import DragAreaColumn from '../../DragComponents/DragAreaColumn';

// todo: loc (needs build fix)
const DEFAULT_DRAG_AREA_TEXT = ('Drag and drop a field here or click “Add a Dimension”.');

@Radium
class ColumnDragArea extends Component {
  static propTypes = {
    dragItem: PropTypes.instanceOf(ColumnDragItem),
    dragOrigin: PropTypes.string,
    columnsField: PropTypes.array,
    allColumns: PropTypes.instanceOf(Immutable.List),
    disabledColumnNames: PropTypes.instanceOf(Immutable.Set),
    onDrop: PropTypes.func,
    removeColumn: PropTypes.func,
    moveColumn: PropTypes.func,
    dragType: PropTypes.string,
    isDragInProgress: PropTypes.bool,
    addColumn: PropTypes.func,
    dragAreaText: PropTypes.string,
    handleDragStart: PropTypes.func,
    canUseFieldAsBothDimensionAndMeasure: PropTypes.bool,
    onDragEnd: PropTypes.func,
    className: PropTypes.string,
    dragContentCls: PropTypes.string
  };

  static defaultProps = {
    dragAreaText: DEFAULT_DRAG_AREA_TEXT,
    dragOrigin: 'dimensions',
    canUseFieldAsBothDimensionAndMeasure: true
  }

  handleDrop = (data) => {
    if (this.canDropColumn()) {
      this.props.onDrop(this.props.dragOrigin, data);
    }
  }

  handleRemoveColumn = (index) => {
    this.props.columnsField.removeField(index);
  }

  canSelectColumn = (columnName) => {
    const { dragItem, dragOrigin, canUseFieldAsBothDimensionAndMeasure } = this.props;
    const isFromDifferentArea = dragItem.dragOrigin !== dragOrigin;
    if (!canUseFieldAsBothDimensionAndMeasure) {
      const column = this.props.allColumns.find(col => col.get('name') === columnName);
      if (column) {
        return !column.get('disabled');
      }
    }
    return isFromDifferentArea && !isAlreadySelected(this.props.columnsField, columnName);
  }

  canDropColumn = () => {
    return this.props.isDragInProgress && this.canSelectColumn(this.props.dragItem.id);
  }

  renderColumnsForDragArea() {
    return this.props.columnsField.map( (columnField, i) => (
      <DragAreaColumn
        className={rowMargin}
        onDragStart={this.props.handleDragStart}
        onDragEnd={this.props.onDragEnd}
        canSelectColumn={this.canSelectColumn}
        field={columnField.column}
        isDragInProgress={this.props.isDragInProgress}
        dragItem={this.props.dragItem}
        allColumns={this.props.allColumns}
        disabledColumnNames={this.props.disabledColumnNames}
        key={i}
        index={i}
        dragOrigin={this.props.dragOrigin}
        moveColumn={this.props.moveColumn}
        onRemoveColumn={this.handleRemoveColumn}
        dragType={this.props.dragType}
      />
    ));
  }

  render() {
    const isEmpty = !this.props.columnsField.length;
    const isDragged = this.canDropColumn();
    const {
      className,
      dragContentCls
    } = this.props;

    return (
      <ExploreDragArea
        className={className}
        dragContentCls={dragContentCls}
        dataQa={this.props.dragOrigin}
        dragType={this.props.dragType}
        onDrop={this.handleDrop}
        isDragged={isDragged}
        emptyDragAreaText={this.props.dragAreaText}
        dragContentStyle={!isEmpty ? style.dragContent.base : {}}
      >
        {this.renderColumnsForDragArea()}
      </ExploreDragArea>
    );
  }
}

const style = {
  dragContent: {
    base: {
      borderLeftWidth: '1px',
      borderRightWidth: '0',
      borderTopWidth: '0',
      borderBottomWidth: '0'
    }
  }
};

export default ColumnDragArea;
