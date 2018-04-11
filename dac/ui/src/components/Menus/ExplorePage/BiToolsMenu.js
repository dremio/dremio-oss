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
import { FormattedMessage } from 'react-intl';

import { NEXT_ACTIONS } from 'actions/explore/nextAction';
import AnalyzeMenuItems from 'components/Menus/AnalyzeMenuItems';

import Menu from './Menu';
import MenuLabel from './MenuLabel';


@Radium
@pureRender
export default class BiToolsMenu extends Component {
  static propTypes = {
    action: PropTypes.func
  };

  handleTableauClick = () => this.props.action({name: NEXT_ACTIONS.openTableau, label: 'Tableau'})
  handleQlikClick = () => this.props.action({name: NEXT_ACTIONS.openQlik, label: 'Qlik Sense'})
  handlePowerBIClick = () => this.props.action({name: NEXT_ACTIONS.openPowerBI, label: 'Power BI'})

  render() {
    return (
      <Menu>
        <MenuLabel><FormattedMessage id='Dataset.AnalyzeWith'/></MenuLabel>
        <AnalyzeMenuItems
          openTableau={this.handleTableauClick}
          openQlikSense={this.handleQlikClick}
          openPowerBI={this.handlePowerBIClick}
        />
      </Menu>
    );
  }
}
