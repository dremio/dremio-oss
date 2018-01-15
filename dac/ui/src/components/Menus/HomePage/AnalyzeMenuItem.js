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
import { Component } from 'react';

import pureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import Immutable from 'immutable';

import { connect } from 'react-redux';

import MenuItem from 'components/Menus/MenuItem';
import SubMenu from 'components/Menus/SubMenu';
import FontIcon from 'components/Icon/FontIcon';
import AnalyzeMenuItems from 'components/Menus/AnalyzeMenuItems';

import { openTableau, openQlikSense, openPowerBI } from 'actions/explore/download';

@pureRender
export class AnalyzeMenuItem extends Component {

  static propTypes = {
    entity: PropTypes.instanceOf(Immutable.Map),
    openTableau: PropTypes.func,
    openQlikSense: PropTypes.func,
    openPowerBI: PropTypes.func,
    closeMenu: PropTypes.func
  };

  handleTableauClick = () => {
    this.props.openTableau(this.props.entity);
    this.props.closeMenu();
  }

  handleQlikClick = () => {
    this.props.openQlikSense(this.props.entity);
    this.props.closeMenu();
  }

  handlePowerBIClick = () => {
    this.props.openPowerBI(this.props.entity);
    this.props.closeMenu();
  }

  render() {
    return <MenuItem
      rightIcon={<FontIcon type='TriangleRight' theme={styles.rightIcon}/>}
      menuItems={[
        <SubMenu key='analyze-with'>
          <AnalyzeMenuItems
            openTableau={this.handleTableauClick}
            openPowerBI={this.handlePowerBIClick}
            openQlikSense={this.handleQlikClick}
          />
        </SubMenu>
      ]}>{la('Analyze With')}</MenuItem>;
  }
}

export default connect(null, {
  openTableau,
  openQlikSense,
  openPowerBI
})(AnalyzeMenuItem);

const styles = {
  rightIcon: {
    'Icon': {
      width: 7,
      height: 10,
      backgroundSize: '24px 24px',
      backgroundPosition: '-10px 50%'
    }
  }
};
