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
import PropTypes from 'prop-types';
import { withRouter } from 'react-router';
import Immutable from 'immutable';

import DividerHr from 'components/Menus/DividerHr';

import Menu from './Menu';
import MenuItem from './MenuItem';
import MenuLabel from './MenuLabel';
import ExportMenu from './ExportMenu';
import BiToolsMenu from './BiToolsMenu';

export class CombinedActionMenu extends PureComponent {
  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map),
    datasetColumns: PropTypes.array,
    action: PropTypes.func,
    closeMenu: PropTypes.func.isRequired,
    isSettingsDisabled: PropTypes.bool,
    isActionDisabled: PropTypes.bool,
    router: PropTypes.object,
    location: PropTypes.object
  };

  handleSettingsClick = () => {
    const { isSettingsDisabled, dataset, router, location, closeMenu } = this.props;
    closeMenu();
    if (isSettingsDisabled) return;

    router.push({
      ...location,
      state: {
        modal: 'DatasetSettingsModal',
        datasetUrl: dataset.getIn(['apiLinks', 'namespaceEntity']),
        datasetType: dataset.get('datasetType'),
        query: { then: 'query' },
        isHomePage: false
      }
    });
  };

  render() {
    const { isSettingsDisabled, isActionDisabled, action, closeMenu, datasetColumns } = this.props;
    return (
      <Menu>
        <MenuLabel>{la('Dataset')}</MenuLabel>
        <MenuItem key='settings'
          className={'ellipsis-settings-item'}
          onClick={this.handleSettingsClick}
          disabled={isSettingsDisabled}>
          {la('Settings')}
        </MenuItem>
        <DividerHr />
        <MenuLabel>{la('Download')}</MenuLabel>
        <ExportMenu action={action} closeMenu={closeMenu} datasetColumns={datasetColumns}/>
        <DividerHr />
        <BiToolsMenu action={action} closeMenu={closeMenu} disabled={isActionDisabled}/>
      </Menu>
    );
  }
}

export default withRouter(CombinedActionMenu);
