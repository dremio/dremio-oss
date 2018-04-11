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

import PropTypes from 'prop-types';

import './SettingItem.less';

export default class SettingItem extends Component {
  static propTypes = {
    onClick: PropTypes.func
  }

  constructor(props) {
    super(props);
  }

  render() {
    return (
      <div className='setting-item' onClick={this.props.onClick}>
        <i className='fa fa-cog'></i>
        <i className='fa fa-angle-down'></i>
      </div>
    );
  }
}
