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

import {BINARY, TEXT, INTEGER, FLOAT, DECIMAL, MAP, BOOLEAN, DATE, TIME, DATETIME} from 'constants/DataTypes';

import Menu from './Menu';

import TypeGroup from './TypeGroups/TypeGroup';
import AutoGroup from './TypeGroups/AutoGroup';

export const NoParamToText = [BINARY, INTEGER, FLOAT, DECIMAL, MAP, BOOLEAN];
export const NoParamToInt = [BOOLEAN];
export const NoParamToFloat = [INTEGER];
export const NoParamToBinary = [TEXT];
export const NoParamToDateTimeTimestamp = [DATE, TIME, DATETIME];
export const NoParamToJSON = [TEXT, BINARY];

@Radium
@pureRender
export default class ColumnTypeMenu extends Component {
  static propTypes = {
    columnType: PropTypes.string.isRequired,
    hideDropdown: PropTypes.func,
    columnName: PropTypes.string,
    openDetailsWizard: PropTypes.func,
    makeTransform: PropTypes.func
  };

  constructor(props) {
    super(props);
    this.makeTransform = this.makeTransform.bind(this);
  }

  runTableTransform(newType) {
    const { columnType, columnName } = this.props;
    const config = {
      type: 'CONVERT_DATA_TYPE',
      newFieldName: columnName,
      dropSourceField: true,
      columnName,
      columnType,
      toType: newType
    };
    this.props.makeTransform(config);
  }

  makeTransform(toType) {
    const { columnName, columnType } = this.props;
    const hash = {
      TEXT: NoParamToText,
      INTEGER: NoParamToInt,
      FLOAT: NoParamToFloat,
      BINARY: NoParamToBinary,
      TIME: NoParamToDateTimeTimestamp,
      DATE: NoParamToDateTimeTimestamp,
      DATETIME: NoParamToDateTimeTimestamp,
      JSON: NoParamToJSON
    };
    if (hash[toType] && hash[toType].indexOf(columnType) !== -1) {
      this.runTableTransform(toType);
    } else {
      this.props.openDetailsWizard({detailType: 'CONVERT_DATA_TYPE', columnName, toType});
    }
    this.props.hideDropdown();
  }

  render() {
    const {columnType} = this.props;

    return (
      <Menu>
        <TypeGroup
          makeTransform={this.makeTransform}
          columnType={columnType}
          NoParamToText={NoParamToText}
          NoParamToInt={NoParamToInt}
          NoParamToFloat={NoParamToFloat}
          NoParamToBinary={NoParamToBinary}
          NoParamToDateTimeTimestamp={NoParamToDateTimeTimestamp}/>
        <AutoGroup makeTransform={this.makeTransform} columnType={columnType}/>
      </Menu>
    );
  }
}
