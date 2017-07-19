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

import Menu from './Menu';

import SortGroup from './ActionGroups/SortGroup';
import MainActionGroup from './ActionGroups/MainActionGroup';
import SqlGroup from './ActionGroups/SqlGroup';
import ReplaceGroup from './ActionGroups/ReplaceGroup';
import OtherGroup from './ActionGroups/OtherGroup';

@Radium
@pureRender
export default class ColumnActionMenu extends Component {
  static propTypes = {
    columnType: PropTypes.string,
    columnName: PropTypes.string,
    columnsNumber: PropTypes.number,
    makeTransform: PropTypes.func,
    openDetailsWizard: PropTypes.func,
    hideDropdown: PropTypes.func
  }

  makeTransform = (actionType) => {
    const { columnName } = this.props;
    const oneStepItems = ['ASC', 'DESC', 'DROP', 'UNNEST'];
    if (oneStepItems.indexOf(actionType) !== -1) {
      this.props.makeTransform({type: actionType, columnName});
    } else if (actionType === 'RENAME') {
      $(`.cell#cell${columnName}`, '.fixed-data-table').focus();
    } else {
      this.props.openDetailsWizard({ detailType: actionType, columnName });
    }
    this.props.hideDropdown();
  }

  isAvailable(menuItems, columnType) {
    return menuItems.some((menuItem) => menuItem && menuItem.props.availableTypes.includes(columnType));
  }

  render() {
    const { columnType, columnsNumber } = this.props;
    return (
      <Menu>
        <SortGroup
          isAvailable={this.isAvailable}
          makeTransform={this.makeTransform}
          columnType={columnType}
        />
        <MainActionGroup
          makeTransform={this.makeTransform}
          columnType={columnType}
          columnsNumber={columnsNumber}
        />
        <SqlGroup
          isAvailable={this.isAvailable}
          makeTransform={this.makeTransform}
          columnType={columnType}
        />
        <ReplaceGroup
          isAvailable={this.isAvailable}
          makeTransform={this.makeTransform}
          columnType={columnType}
        />
        <OtherGroup
          makeTransform={this.makeTransform}
          columnType={columnType}
        />
      </Menu>
    );
  }
}
