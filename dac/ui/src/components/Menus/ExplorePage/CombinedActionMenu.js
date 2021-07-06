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
import { connect } from 'react-redux';
import Immutable from 'immutable';
import { injectIntl } from 'react-intl';

import { CombinedActionMenuMixin } from 'dyn-load/components/Menus/ExplorePage/CombinedActionMenuMixin.js';
import HoverHelp from '@app/components/HoverHelp';
import MenuItem from '@app/components/Menus/MenuItem';
import { getJobProgress } from '@app/selectors/explore';

import Menu from './Menu';
import MenuLabel from './MenuLabel';

@injectIntl
@CombinedActionMenuMixin
export class CombinedActionMenu extends PureComponent {
  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map),
    datasetColumns: PropTypes.array,
    action: PropTypes.func,
    closeMenu: PropTypes.func.isRequired,
    isSettingsDisabled: PropTypes.bool,
    intl: PropTypes.object.isRequired,
    //connected
    jobProgress: PropTypes.object,
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

  renderDownloadSectionHeader = () => {
    const {intl, jobProgress} = this.props;
    const isRun = jobProgress && jobProgress.isRun;
    const headerText = isRun ? la('Download (limited)') : la('Download (sample)');
    const helpContent = isRun ?
      intl.formatMessage({ id: 'Explore.run.warning' }) :
      intl.formatMessage({ id: 'Explore.preview.warning' });
    return (
      <MenuLabel>
        {headerText}<HoverHelp content={helpContent} placement='bottom-start' />
      </MenuLabel>
    );
  };

  render() {
    const { isSettingsDisabled } = this.props;
    return (
      <Menu>
        <MenuLabel>{la('Dataset')}</MenuLabel>
        <MenuItem key='settings'
          className={'ellipsis-settings-item'}
          onClick={this.handleSettingsClick}
          disabled={isSettingsDisabled}>
          {la('Settings')}
        </MenuItem>
        {this.checkToRenderDownloadSection()}
      </Menu>
    );
  }
}

function mapStateToProps(state) {
  return {
    jobProgress: getJobProgress(state)
  };
}

export default withRouter(connect(mapStateToProps)(CombinedActionMenu));
