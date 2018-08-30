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
import DragSortColumn from './DragSortColumn';

const DEFAULT_DRAG_AREA_TEXT = 'Drag and drop field here or click “Add a Sort Field”';

@Radium
class SortDragArea extends Component {
  static propTypes = {
    columnsField: PropTypes.array,
    allColumns: PropTypes.instanceOf(Immutable.List),
    onDrop: PropTypes.func,
    removeColumn: PropTypes.func,
    moveColumn: PropTypes.func,
    dragType: PropTypes.string,
    isDragInProgress: PropTypes.bool,
    addColumn: PropTypes.func
  };

  handleDrop = (data) => {
    this.props.onDrop('sort', data);
  }

  handleRemoveColumn = (index) => {
    this.props.columnsField.removeField(index);
  }

  renderColumnsForDragArea() {
    const { allColumns, columnsField } = this.props;

    if (allColumns && allColumns.size) {
      return columnsField.map((columnField, i) => (
        <DragSortColumn
          field={columnField}
          isDragInProgress={this.props.isDragInProgress}
          allColumns={this.props.allColumns}
          key={i}
          index={i}
          type='sort'
          moveColumn={this.props.moveColumn}
          dragType={this.props.dragType}
          onRemoveColumn={this.handleRemoveColumn}
        />
      ));
    }

    return null;
  }

  render() {
    return (
      <ExploreDragArea
        dragType={this.props.dragType}
        onDrop={this.handleDrop}
        isDragged={this.props.isDragInProgress}
        emptyDragAreaText={DEFAULT_DRAG_AREA_TEXT}
      >
        {this.renderColumnsForDragArea()}
      </ExploreDragArea>
    );
  }
}

export default SortDragArea;
