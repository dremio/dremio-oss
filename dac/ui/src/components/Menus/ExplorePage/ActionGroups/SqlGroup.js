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

import { LIST, MAP, TEXT } from 'constants/DataTypes';
import { ALL_TYPES } from 'constants/columnTypeGroups';

import ColumnMenuItem from './../ColumnMenus/ColumnMenuItem';

@Radium
@pureRender
export default class SqlGroup extends Component {
  static propTypes = {
    makeTransform: PropTypes.func.isRequired,
    columnType: PropTypes.string
  }
  static renderMenuItems(columnType, onClick) {
    return [
      <ColumnMenuItem key='UNNEST'
        columnType={columnType}
        actionType='UNNEST'
        title={la('Unnest')}
        availableTypes={[LIST]}
        onClick={onClick}/>,
      <ColumnMenuItem key='EXTRACT_ELEMENTS'
        columnType={columnType}
        actionType='EXTRACT_ELEMENTS'
        title={la('Extract Element(s)…')}
        availableTypes={[LIST]}
        onClick={onClick}/>,
      <ColumnMenuItem key='EXTRACT_ELEMENT'
        columnType={columnType}
        actionType='EXTRACT_ELEMENT'
        title={la('Extract Element…')}
        availableTypes={[MAP]}
        onClick={onClick}/>,
      <ColumnMenuItem key='CONVERT_CASE'
        columnType={columnType}
        actionType='CONVERT_CASE'
        title={la('Convert Case…')}
        availableTypes={[TEXT]}
        onClick={onClick}/>,
      <ColumnMenuItem key='TRIM_WHITE_SPACES'
        columnType={columnType}
        actionType='TRIM_WHITE_SPACES'
        title={la('Trim Whitespace…')}
        availableTypes={[TEXT]}
        onClick={onClick}/>,
      <ColumnMenuItem key='CALCULATED_FIELD'
        columnType={columnType}
        actionType='CALCULATED_FIELD'
        title={la('Calculated Field…')}
        availableTypes={ALL_TYPES}
        onClick={onClick}
      />
    ];
  }
  render() {
    const { columnType, makeTransform } = this.props;
    const menuItems = SqlGroup.renderMenuItems(columnType, makeTransform);
    return (
      <div>
        {menuItems}
      </div>
    );
  }
}
