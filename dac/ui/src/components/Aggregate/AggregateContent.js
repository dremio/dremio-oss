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

import DragColumnMenu from 'components/DragComponents/DragColumnMenu';
import ColumnDragItem from 'utils/ColumnDragItem';

import ColumnDragArea from './components/ColumnDragArea';
import MeasureDragArea, { MEASURE_DRAG_AREA_TEXT } from './components/MeasureDragArea';
import { base, inner, leftBorder, fullHeight, contentPadding } from './AggregateContent.less';

export const NOT_SUPPORTED_TYPES = new Set(['MAP', 'LIST', 'STRUCT']);

@Radium
class AggregateContent extends Component {
  static propTypes = {
    fields: PropTypes.object,
    canSelectMeasure: PropTypes.bool,
    canUseFieldAsBothDimensionAndMeasure: PropTypes.bool,
    allColumns: PropTypes.instanceOf(Immutable.List),
    handleDragStart: PropTypes.func,
    onDragEnd: PropTypes.func,
    onDrop: PropTypes.func,
    handleMeasureChange: PropTypes.func,
    dragType: PropTypes.string,
    path: PropTypes.string,
    isDragInProgress: PropTypes.bool,
    style: PropTypes.object,
    dragItem: PropTypes.instanceOf(ColumnDragItem),
    className: PropTypes.string
  };

  static defaultProps = {
    allColumns: Immutable.List(),
    canSelectMeasure: true,
    canUseFieldAsBothDimensionAndMeasure: true
  };

  disabledColumnNames = undefined;

  constructor(props) {
    super(props);
    this.receiveProps(props, {});
  }

  componentWillReceiveProps(nextProps) {
    this.receiveProps(nextProps, this.props);
  }

  receiveProps(nextProps, oldProps) {
    // disabledColumnNames is wholly derived from these props, so only recalculate it when one of them has changed
    const propKeys = ['allColumns', 'fields', 'canSelectMeasure', 'canUseFieldAsBothDimensionAndMeasure'];
    if (propKeys.some((key) => nextProps[key] !== oldProps[key])) {
      this.disabledColumnNames = this.getDisabledColumnNames(nextProps);
    }
  }

  getDisabledColumnNames(props) {
    const {
      allColumns, fields, canSelectMeasure, canUseFieldAsBothDimensionAndMeasure
    } = props;
    const dimensionColumnNames = Immutable.Set(fields.columnsDimensions.map(col => col.column.value));
    const measuresColumnNames = Immutable.Set(fields.columnsMeasures.map(col => col.column.value));
    const columnsInEither = dimensionColumnNames.concat(measuresColumnNames);
    const columnsInBoth = dimensionColumnNames.intersect(measuresColumnNames);

    const disabledColumns = allColumns.filter(
      (column) =>
        NOT_SUPPORTED_TYPES.has(column.get('type')) ||
        (!canSelectMeasure && columnsInBoth.has(column.get('name'))) ||
        (!canUseFieldAsBothDimensionAndMeasure && columnsInEither.has(column.get('name')))
      );
    return Immutable.Set(disabledColumns.map((column) => column.get('name')));
  }

  render() {
    const {
      allColumns, onDrop, fields, dragType, isDragInProgress, dragItem,
      handleDragStart, onDragEnd, canUseFieldAsBothDimensionAndMeasure,
      className
    } = this.props;
    const commonDragAreaProps = {
      allColumns,
      disabledColumnNames: this.disabledColumnNames,
      handleDragStart,
      onDragEnd,
      onDrop,
      dragType,
      isDragInProgress,
      dragItem,
      canUseFieldAsBothDimensionAndMeasure
    };
    const measurementCls = classNames(['aggregate-measurement', fullHeight]);

    return (
      <div className={classNames(['aggregate-content', base, className])} style={this.props.style}>
        <div className={inner}>
          <DragColumnMenu
            items={allColumns}
            className={fullHeight}
            disabledColumnNames={this.disabledColumnNames}
            columnType='column'
            handleDragStart={handleDragStart}
            onDragEnd={onDragEnd}
            dragType={dragType}
            name={`${this.props.path} (${la('Current')})`}
          />
        </div>
        <div className={leftBorder}>
          <ColumnDragArea
            className={classNames(['aggregate-dimension', fullHeight])}
            dragContentCls={contentPadding}
            {...commonDragAreaProps}
            columnsField={fields.columnsDimensions}/>
        </div>
        <div className={leftBorder}>
          {
            this.props.canSelectMeasure ?
              <MeasureDragArea
                dragContentCls={contentPadding}
                className={measurementCls}
                {...commonDragAreaProps}
                columnsField={fields.columnsMeasures}/> :
              <ColumnDragArea
                dragContentCls={contentPadding}
                className={measurementCls}
                {...commonDragAreaProps}
                dragOrigin='measures'
                dragAreaText={MEASURE_DRAG_AREA_TEXT}
                columnsField={fields.columnsMeasures}/>
          }
        </div>

      </div>
    );
  }
}

export default AggregateContent;
