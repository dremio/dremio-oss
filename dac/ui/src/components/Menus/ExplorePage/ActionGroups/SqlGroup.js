/*
 * Copyright (C) 2017 Dremio Corporation
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
import { Component, PropTypes } from 'react';
import Radium from 'radium';
import pureRender from 'pure-render-decorator';
import Divider from 'material-ui/Divider';

import { LIST, MAP, TEXT, DATE, TIME, DATETIME } from 'constants/DataTypes';
import { BIN_TYPES, NUMBER_TYPES, ALL_TYPES } from 'constants/columnTypeGroups';

import ColumnMenuItem from './../ColumnMenus/ColumnMenuItem';

@Radium
@pureRender
export default class SqlGroup extends Component {
  static propTypes = {
    makeTransform: PropTypes.func.isRequired,
    isAvailable: PropTypes.func.isRequired,
    columnType: PropTypes.string
  }
  renderMenuItems(columnType) {
    return [
      <ColumnMenuItem
        columnType={columnType}
        actionType='UNNEST'
        title={la('Unnest')}
        availableTypes={[LIST]}
        onClick={this.props.makeTransform}/>,
      <ColumnMenuItem
        columnType={columnType}
        actionType='EXTRACT_ELEMENTS'
        title={la('Extract Element(s)...')}
        availableTypes={[LIST]}
        onClick={this.props.makeTransform}/>,
      <ColumnMenuItem
        columnType={columnType}
        actionType='EXTRACT_ELEMENT'
        title={la('Extract Element...')}
        availableTypes={[MAP]}
        onClick={this.props.makeTransform}/>,
      false && <ColumnMenuItem
        disabled
        columnType={columnType}
        actionType='SPLIT_TO_MULTIPLY_FIELDS'
        title={la('Split to Multiple Fields...')}
        availableTypes={[LIST]}
        onClick={this.props.makeTransform}/>,
      <ColumnMenuItem
        columnType={columnType}
        actionType='CONVERT_CASE'
        title={la('Convert Case...')}
        availableTypes={[TEXT]}
        onClick={this.props.makeTransform}/>,
      false && <ColumnMenuItem
        columnType={columnType}
        actionType='EXTRACT_DATE_PART'
        title={la('Extract Date Part...')}
        availableTypes={[DATE]}
        onClick={this.props.makeTransform}/>,
      false && <ColumnMenuItem
        columnType={columnType}
        actionType='EXTRACT_TIME_PART'
        title={la('Extract Time Part...')}
        availableTypes={[TIME]}
        onClick={this.props.makeTransform}/>,
      false && <ColumnMenuItem
        columnType={columnType}
        actionType='EXTRACT_PART'
        title={la('Extract Part...')}
        availableTypes={[DATETIME]}
        onClick={this.props.makeTransform}/>,
      false && <ColumnMenuItem
        columnType={columnType}
        actionType='TRUNCATE_DATE'
        title={la('Truncate Date...')}
        availableTypes={[DATE]}
        onClick={this.props.makeTransform}/>,
      false && <ColumnMenuItem
        columnType={columnType}
        actionType='TRUNCATE_TIME'
        title={la('Truncate Time...')}
        availableTypes={[TIME]}
        onClick={this.props.makeTransform}/>,
      false && <ColumnMenuItem
        columnType={columnType}
        actionType='TRUNCATE'
        title={la('Truncate...')}
        availableTypes={[DATETIME]}
        onClick={this.props.makeTransform}/>,
      false && <ColumnMenuItem
        columnType={columnType}
        actionType='ROUND'
        title={la('Round...')}
        availableTypes={NUMBER_TYPES}
        onClick={this.props.makeTransform}/>,
      false && <ColumnMenuItem
        columnType={columnType}
        actionType='BIN'
        title={la('Bin...')}
        availableTypes={BIN_TYPES}
        onClick={this.props.makeTransform}/>,
      <ColumnMenuItem
        columnType={columnType}
        actionType='TRIM_WHITE_SPACES'
        title={la('Trim Whitespace...')}
        availableTypes={[TEXT]}
        onClick={this.props.makeTransform}/>,
      <ColumnMenuItem
        columnType={columnType}
        actionType='CALCULATED_FIELD'
        title={la('Calculated Field...')}
        availableTypes={ALL_TYPES.filter(type => type !== LIST)}
        onClick={this.props.makeTransform}
      />
    ];
  }
  render() {
    const { columnType, isAvailable } = this.props;
    const menuItems = this.renderMenuItems(columnType);
    const shouldShowDivider = isAvailable(menuItems, columnType);
    return (
      <div>
        {menuItems}
        {shouldShowDivider && <Divider style={{marginTop: 5, marginBottom: 5}}/>}
      </div>
    );
  }
}
