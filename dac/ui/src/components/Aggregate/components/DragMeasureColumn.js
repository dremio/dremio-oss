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
import classNames from 'classnames';

import {
  getColumnByName, getMeasuresForColumnType, isMeasureValidForColumnType
} from 'utils/explore/aggregateUtils';
import ColumnDragItem from 'utils/ColumnDragItem';
import { base, columnElement, select as selectCls } from '@app/pages/ExplorePage/components/MultiplySort/components/DragSortColumn.less';

import Select from '../../Fields/Select';
import DragAreaColumn from '../../DragComponents/DragAreaColumn';

const COUNT_STAR = 'Count_Star';

//todo reuse DragSortColumn for this component
@Radium
class DragMeasureColumn extends Component {
  static propTypes = {
    dragItem: PropTypes.instanceOf(ColumnDragItem),
    field: PropTypes.object,
    isDragInProgress: PropTypes.bool,
    allColumns: PropTypes.instanceOf(Immutable.List),
    dragType: PropTypes.string.isRequired,
    moveColumn: PropTypes.func,
    index: PropTypes.number,
    onDragStart: PropTypes.func,
    onDragEnd: PropTypes.func,
    onRemoveColumn: PropTypes.func
  };

  canSelectColumn = (columnName) => {
    const { field, allColumns } = this.props;
    const column = getColumnByName(allColumns, columnName);
    return isMeasureValidForColumnType(field.measure.value, column && column.get('type'));
  }

  render() {
    const { field, allColumns, index } = this.props;
    const selectedColumn = getColumnByName(allColumns, field.column.value);
    const measureItems = getMeasuresForColumnType(selectedColumn && selectedColumn.get('type'));

    return (
      <div className={classNames(['drag-measure-column', base])}>
        <Select
          {...field.measure}
          className={classNames(['agg-func', selectCls])} // agg-func is used in automated testing
          items={measureItems}
          iconStyle={styles.iconStyle}
          customLabelStyle={styles.customLabelStyle}
        />
        <div className={columnElement}>
          <DragAreaColumn
            onDragStart={this.props.onDragStart}
            onDragEnd={this.props.onDragEnd}
            field={field.column}
            disabled={field.measure.value === COUNT_STAR} // count star doesn't need a column
            canSelectColumn={this.canSelectColumn}
            isDragInProgress={this.props.isDragInProgress}
            dragItem={this.props.dragItem}
            allColumns={this.props.allColumns}
            index={index}
            dragOrigin='measures'
            moveColumn={this.props.moveColumn}
            dragType={this.props.dragType}
            onRemoveColumn={this.props.onRemoveColumn}
          />
        </div>
      </div>
    );
  }
}

const styles = {
  iconStyle: {
    top: 0
  },
  customLabelStyle: {
    top: 13
  }
};

export default DragMeasureColumn;
