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
import { Component } from "react";
import PropTypes from "prop-types";
import Immutable from "immutable";
import classNames from "clsx";

import { dragContentBase } from "@app/uiTheme/less/commonStyles.less";
import ExploreDragArea from "pages/ExplorePage/components/ExploreDragArea";
import ColumnDragItem from "utils/ColumnDragItem";

import DragMeasureColumn from "./DragMeasureColumn";

// todo: loc (needs build fix)
export const MEASURE_DRAG_AREA_TEXT =
  "Drag and drop a field here or click “Add a Measure”.";

const DRAG_AREA_TYPE = "measures";

class MeasureDragArea extends Component {
  static propTypes = {
    dragItem: PropTypes.instanceOf(ColumnDragItem),
    columnsField: PropTypes.array,
    allColumns: PropTypes.instanceOf(Immutable.List),
    disabledColumnNames: PropTypes.instanceOf(Immutable.Set),
    onDrop: PropTypes.func,
    moveColumn: PropTypes.func,
    dragType: PropTypes.string,
    isDragInProgress: PropTypes.bool,
    handleDragStart: PropTypes.func,
    onDragEnd: PropTypes.func,
    className: PropTypes.string,
    dragContentCls: PropTypes.string,
  };

  handleRemoveColumn = (index) => {
    this.props.columnsField.removeField(index);
  };

  handleDrop = (data) => {
    if (this.canDropColumn()) {
      this.props.onDrop(DRAG_AREA_TYPE, data);
    }
  };

  canDropColumn() {
    const isFromDifferentArea =
      this.props.dragItem.dragOrigin !== DRAG_AREA_TYPE;
    return this.props.isDragInProgress && isFromDifferentArea;
  }

  renderColumnsForDragArea() {
    const { columnsField } = this.props;

    return columnsField.map((columnField, i) => (
      <DragMeasureColumn
        field={columnField}
        onDragStart={this.props.handleDragStart}
        onDragEnd={this.props.onDragEnd}
        isDragInProgress={this.props.isDragInProgress}
        dragItem={this.props.dragItem}
        allColumns={this.props.allColumns}
        disabledColumnNames={this.props.disabledColumnNames}
        key={i}
        index={i}
        moveColumn={this.props.moveColumn}
        dragType={this.props.dragType}
        onRemoveColumn={this.handleRemoveColumn}
      />
    ));
  }

  render() {
    const isEmpty = !this.props.columnsField.length;
    const isDragged = this.canDropColumn();
    const { className, dragContentCls } = this.props;

    return (
      <ExploreDragArea
        className={classNames(className, !isEmpty ? dragContentBase : null)}
        dragContentCls={dragContentCls}
        dataQa={DRAG_AREA_TYPE}
        dragType={this.props.dragType}
        onDrop={this.handleDrop}
        isDragged={isDragged}
        emptyDragAreaText={MEASURE_DRAG_AREA_TEXT}
      >
        {this.renderColumnsForDragArea()}
      </ExploreDragArea>
    );
  }
}
export default MeasureDragArea;
