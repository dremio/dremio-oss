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

import PropTypes from 'prop-types';

import { bodyWhite } from 'uiTheme/radium/typography';

import './MainHeaderItem.less';

@Radium
export default class MainHeaderItem extends Component {
  static propTypes = {
    children: PropTypes.node,
    customStyle: PropTypes.object
  };

  constructor(props) {
    super(props);
  }

  render() {
    const {customStyle} = this.props;
    return (
      <div className='explore-header-item' style={[styles.exploreHeaderItem, bodyWhite, customStyle || {}]}>
        {this.props.children}
      </div>
    );
  }
}


const styles = {
  exploreHeaderItem: {
    display: 'flex',
    whiteSpace: 'nowrap'
  }
};
