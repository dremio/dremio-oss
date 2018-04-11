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
import pureRender from 'pure-render-decorator';

import PropTypes from 'prop-types';

import { BINARY, TEXT, INTEGER, FLOAT, DATE, TIME, DATETIME } from 'constants/DataTypes';
import { TO_BINARY_TYPES, TO_INTEGER_TYPES,
         TO_DATE_TYPES, TO_FLOAT_TYPES, ALL_TYPES, TO_TIME_TYPES } from 'constants/columnTypeGroups';

import MenuItem from './../MenuItem';
import ColumnMenuItem from './../ColumnMenus/ColumnMenuItem';

// todo: loc

@Radium
@pureRender
export default class TypeGroup extends Component {
  static propTypes = {
    makeTransform: PropTypes.func.isRequired,
    columnType: PropTypes.string,
    NoParamToText: PropTypes.array,
    NoParamToBinary: PropTypes.array,
    NoParamToInt: PropTypes.array,
    NoParamToFloat: PropTypes.array,
    NoParamToDateTimeTimestamp: PropTypes.array
  }

  constructor(props) {
    super(props);
    this.setVisibility = this.setVisibility.bind(this);
  }

  componentDidMount() {
    if (!this.refs.root || !this.refs.root.children) {
      return null;
    }
    const divs = [...this.refs.root.children].filter(child => child.nodeName === 'DIV');
    if (divs.length === 1) {
      this.setVisibility(false);
    } else {
      this.setVisibility(true);
    }
  }

  setVisibility(visibility) {
    this.setState({
      visibility
    });
  }

  render() {
    const {
      columnType, NoParamToText, NoParamToBinary, NoParamToInt, NoParamToFloat, NoParamToDateTimeTimestamp
    } = this.props;

    const commonProps = {
      columnType,
      onClick: this.props.makeTransform
    };
    return (
      <div ref='root'>
        <MenuItem disabled>TYPE</MenuItem>
        <ColumnMenuItem
          {...commonProps}
          actionType={TEXT}
          title={NoParamToText.indexOf(columnType) !== -1 ? 'Text' : 'Text…'}
          availableTypes={ALL_TYPES.filter(a => a !== TEXT && a !== BINARY)} // BINARY to TEXT is disabled due to BE bug DX-4110
        />
        <ColumnMenuItem
          {...commonProps}
          actionType={BINARY}
          title={NoParamToBinary.indexOf(columnType) !== -1 ? 'Binary' : 'Binary…'}
          availableTypes={TO_BINARY_TYPES}
        />
        <ColumnMenuItem
          {...commonProps}
          actionType={INTEGER}
          title={NoParamToInt.indexOf(columnType) !== -1 ? 'Integer' : 'Integer…'}
          availableTypes={TO_INTEGER_TYPES}
        />
        <ColumnMenuItem
          {...commonProps}
          actionType={FLOAT}
          title={NoParamToFloat.indexOf(columnType) !== -1 ? 'Float' : 'Float…'}
          availableTypes={TO_FLOAT_TYPES}
        />
        <ColumnMenuItem
          {...commonProps}
          actionType={DATE}
          title={NoParamToDateTimeTimestamp.indexOf(columnType) !== -1 ? 'Date' : 'Date…'}
          availableTypes={TO_DATE_TYPES.filter(a => a !== DATE)}
        />
        <ColumnMenuItem
          {...commonProps}
          actionType={TIME}
          title={NoParamToDateTimeTimestamp.indexOf(columnType) !== -1 ? 'Time' : 'Time…'}
          availableTypes={TO_TIME_TYPES}
        />
        <ColumnMenuItem
          {...commonProps}
          actionType={DATETIME}
          title={NoParamToDateTimeTimestamp.indexOf(columnType) !== -1 ? 'Date & Time' : 'Date & Time…'}
          availableTypes={TO_DATE_TYPES.filter(a => a !== DATETIME)}
        />
      </div>
    );
  }
}
