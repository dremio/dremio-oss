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

import MenuItem from './MenuItem';
import Menu from './Menu';

@Radium
@pureRender
export default class SaveMenu extends Component {
  static propTypes = {
    action: PropTypes.func
  };

  constructor(props) {
    super(props);

    this.saveAs = props.action.bind(null, { name: 'saveAs', label: la('Save As…') });
    this.save = props.action.bind(null, { name: 'save', label: la('Save') });
  }

  render() {
    return (
      <Menu>
        <MenuItem className='save-menu-item' onClick={this.save}>
          {la('Save')}
        </MenuItem>
        <MenuItem
          className='save-as-menu-item'
          onClick={this.saveAs}>
          {la('Save As…')}
        </MenuItem>
      </Menu>
    );
  }
}
