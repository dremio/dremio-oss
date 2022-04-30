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
import { PureComponent } from 'react';
import Radium from 'radium';
import { injectIntl } from 'react-intl';


import PropTypes from 'prop-types';

import { Divider } from '@material-ui/core';
import MenuItem from './MenuItem';
import Menu from './Menu';

import './SaveMenu.less';

export const DOWNLOAD_TYPES = {
  json: 'JSON',
  csv: 'CSV',
  parquet: 'PARQUET'
};

@injectIntl
@Radium
export default class SaveMenu extends PureComponent {
  static propTypes = {
    action: PropTypes.func,
    closeMenu: PropTypes.func,
    mustSaveAs: PropTypes.bool,
    disableBoth: PropTypes.bool,
    isSqlEditorTab: PropTypes.bool,
    intl: PropTypes.object
  };

  saveAction = (saveType) => {
    this.props.closeMenu();
    this.props.action(saveType);
  }

  renderMenuItems = () => {
    const { mustSaveAs, disableBoth, isSqlEditorTab, intl } = this.props;

    const saveMenuItems = [
      { label: 'NewQuery.SaveScript', handleSave: () => this.saveAction('saveScript'), disabled: false, class: 'save-menu-item' },
      { label: 'NewQuery.SaveScriptAs', handleSave: () => this.saveAction('saveScriptAs'), disabled: false, class: 'save-as-menu-item' },
      { label: 'NewQuery.SaveView', handleSave: () => this.saveAction('saveView'), disabled: mustSaveAs || disableBoth, class: 'save-menu-item' },
      { label: 'NewQuery.SaveViewAs', handleSave: () => this.saveAction('saveViewAs'), disabled: disableBoth, class: 'save-as-menu-item' }
    ];

    const renderItems = isSqlEditorTab ?
      [saveMenuItems[0], saveMenuItems[1], null, saveMenuItems[3]] :
      [saveMenuItems[2], saveMenuItems[3], null,  saveMenuItems[1]];

    return renderItems.map((item, i) => {
      return item && item.label ? (
        <MenuItem
          key={item.label}
          className={item.class}
          disabled={item.disabled}
          onClick={item.handleSave}
        >
          {intl.formatMessage({ id: item.label})}
        </MenuItem>
      ) : (
        <Divider key={`${i}`} className='custom-menu-divider' />
      );
    });
  }

  render() {
    return (
      <Menu>
        {this.renderMenuItems()}
      </Menu>
    );
  }
}
