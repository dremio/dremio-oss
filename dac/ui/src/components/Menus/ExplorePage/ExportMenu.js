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

import { EXPORT_TYPES, UNSUPPORTED_TYPE_COLUMNS } from 'constants/explorePage/exportTypes';
import MenuItem from './MenuItem';
import Menu from './Menu';

@Radium
@pureRender
export default class ExportMenu extends Component {
  static propTypes = {
    type: PropTypes.string,
    action: PropTypes.func,
    datasetColumns: PropTypes.array
  };

  static defaultProps = {
    exportOptions: []
  }

  static defaultMenuItem = {
    label: 'JSON',
    name: 'JSON'
  }

  renderMenuItems() {
    const datasetColumns = new Set(this.props.datasetColumns);
    const isTypesIntersected = (types) => !![...types].filter(type => datasetColumns.has(type)).length;

    return EXPORT_TYPES.map(name => {
      const types = UNSUPPORTED_TYPE_COLUMNS[name];
      const disabled = types && isTypesIntersected(types);
      const onClick = disabled ? () => {} : this.props.action.bind(this, {name, label: name});

      return (
        <MenuItem key={name} onClick={onClick} disabled={disabled}>
          {name}
        </MenuItem>
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
