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

import React from 'react';
import PropTypes from 'prop-types';
import clsx from 'clsx';

import './flexTable.scss';

const FlexTable = (props) => {
  const {
    data,
    header,
    classes,
    emptyMessage,
    showEmptyMessage
  } = props;

  const renderCell = (cell, cellClass, key) => {
    const {
      value,
      flexGrow,
      className
    } = cell;
    const cellClassName = clsx(
      'flexTable__cell',
      {
        [classes.cell]: classes.cell,
        [cellClass]: cellClass,
        [className]: className
      }
    );

    return (
      <div className={cellClassName} key={key} style={flexGrow ? { flexGrow } : {}}>
        {value}
      </div>
    );
  };

  const renderEmptyTableContent = () => {
    return showEmptyMessage && <div className='flexTable__row --empty'>
      {emptyMessage}
    </div>;
  };

  const renderHeader = () => {
    const className = clsx(
      'flexTable__row --header',
      { [classes.headerRow]: classes.headerRow }
    );
    return (
      <div className={className}>
        {
          header.map((headerCell, index) => renderCell(headerCell, classes.headerCell, `header_cell_${index}`))
        }
      </div>
    );
  };

  const renderData = () => {
    if (data.length === 0) {
      return renderEmptyTableContent();
    }
    const className = clsx(
      'flexTable__row --data',
      { [classes.dataRow]: classes.dataRow }
    );
    return data.map((dataRow, rowIndex) => (
      <div className={className} key={`data_row_${rowIndex}`}>
        {
          dataRow.map((dataCell, index) => {
            const { flexGrow } = header[index];
            const dataCellWithBreakPoints = {
              ...dataCell,
              flexGrow
            };
            return renderCell(dataCellWithBreakPoints, classes.dataCell, `data_cell_${rowIndex}_${index}`);
          })
        }
      </div>
    ));
  };

  const root = clsx(
    'flexTable',
    { [classes.root]: classes.root }
  );

  return (
    <div className={root}>
      <div className='flexTable__headerContainer'>
        {renderHeader()}
      </div>
      <div className='flexTable__dataContainer'>
        {renderData()}
      </div>
    </div>
  );
};

const dataCellPropType = PropTypes.shape({
  value: PropTypes.oneOfType([
    PropTypes.string,
    PropTypes.node
  ]).isRequired,
  className: PropTypes.string
});

const headerCellPropType = PropTypes.shape({
  value: PropTypes.oneOfType([
    PropTypes.string,
    PropTypes.node
  ]).isRequired,
  flexGrow: PropTypes.number,
  className: PropTypes.string
});

FlexTable.propTypes = {
  header: PropTypes.arrayOf(headerCellPropType).isRequired,
  data: (...args) => {
    const arrayValidationError = PropTypes.arrayOf(PropTypes.arrayOf(dataCellPropType)).isRequired(...args);
    if (arrayValidationError) {
      return arrayValidationError;
    }
    const [props, propName, componentName] = args;
    const invalidLengthCellIndex = props[propName].findIndex(row => row.length !== props.header.length);
    if (invalidLengthCellIndex !== -1) {
      return new Error(
        `Invalid prop '${propName}' supplied to '${componentName}'. Please check the length of row ${invalidLengthCellIndex + 1}`
      );
    }
  },
  classes: PropTypes.shape({
    root: PropTypes.string,
    headerRow: PropTypes.string,
    dataRow: PropTypes.string,
    headerCell: PropTypes.string,
    dataCell: PropTypes.string,
    cell: PropTypes.string
  }),
  emptyMessage: PropTypes.string,
  showEmptyMessage: PropTypes.bool
};

FlexTable.defaultProps = {
  classes: {},
  emptyMessage: 'No Items',
  showEmptyMessage: true
};

export default FlexTable;
