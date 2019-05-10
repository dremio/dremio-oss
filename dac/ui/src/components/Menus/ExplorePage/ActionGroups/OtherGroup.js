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
import { KEEP_ONLY_TYPES } from '@app/constants/columnTypeGroups';
import ColumnMenuItem from '@app/components/Menus/ExplorePage/ColumnMenus/ColumnMenuItem';


@Radium
@pureRender
export default class OtherGroup extends Component {
  static propTypes = {
    makeTransform: PropTypes.func.isRequired,
    columnType: PropTypes.string
  }
  static renderMenuItems(columnType, onClick) {
    return [
      <ColumnMenuItem key='KEEP_ONLY'
        columnType={columnType}
        actionType='KEEP_ONLY'
        title={la('Keep Only…')}
        availableTypes={KEEP_ONLY_TYPES}
        onClick={onClick}
      />,
      <ColumnMenuItem key='EXCLUDE'
        columnType={columnType}
        actionType='EXCLUDE'
        title={la('Exclude…')}
        availableTypes={KEEP_ONLY_TYPES}
        onClick={onClick}
      />
    ];
  }
  render() {
    const { columnType, makeTransform } = this.props;
    const menuItems = OtherGroup.renderMenuItems(columnType, makeTransform);
    return (
      <div>
        {menuItems}
      </div>
    );
  }
}
