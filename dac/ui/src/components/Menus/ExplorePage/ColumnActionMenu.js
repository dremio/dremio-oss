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
import Divider from '@material-ui/core/Divider';

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
    columnsCount: PropTypes.number,
    makeTransform: PropTypes.func,
    openDetailsWizard: PropTypes.func,
    hideDropdown: PropTypes.func,
    onRename: PropTypes.func.isRequired
  }

  makeTransform = (actionType) => {
    const { columnName } = this.props;
    const oneStepItems = ['ASC', 'DESC', 'DROP', 'UNNEST'];
    if (oneStepItems.indexOf(actionType) !== -1) {
      this.props.makeTransform({type: actionType, columnName});
    } else if (actionType === 'RENAME') {
      this.props.onRename();
    } else {
      this.props.openDetailsWizard({ detailType: actionType, columnName });
    }
    this.props.hideDropdown();
  }

  isAvailable(menuItems, columnType) {
    return menuItems.some((menuItem) => menuItem && menuItem.props.availableTypes.includes(columnType));
  }
  shouldShowDivider(menuGroup) {
    const { columnType } = this.props;
    const menuItems = menuGroup.renderMenuItems(columnType);
    return this.isAvailable(menuItems, columnType);
  }
  renderDivider(menuGroups) {
    const shouldShowDivider = menuGroups.every((menuGroup) => this.shouldShowDivider(menuGroup));
    return shouldShowDivider && <Divider style={styles.dividerStyle} />;
  }

  render() {
    const { columnType, columnsCount } = this.props;
    return (
      <Menu>
        <SortGroup
          makeTransform={this.makeTransform}
          columnType={columnType}
        />
        {this.renderDivider([SortGroup])}
        <MainActionGroup
          makeTransform={this.makeTransform}
          columnType={columnType}
          columnsCount={columnsCount}
        />
        <Divider style={styles.dividerStyle}/>
        <SqlGroup
          makeTransform={this.makeTransform}
          columnType={columnType}
        />
        {this.renderDivider([SqlGroup, ReplaceGroup, OtherGroup])}
        <ReplaceGroup
          makeTransform={this.makeTransform}
          columnType={columnType}
        />
        {this.renderDivider([ReplaceGroup, OtherGroup])}
        <OtherGroup
          makeTransform={this.makeTransform}
          columnType={columnType}
        />
      </Menu>
    );
  }
}

const styles = {
  dividerStyle: {
    marginTop: 5,
    marginBottom: 5
  }
};
