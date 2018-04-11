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
import { PureComponent } from 'react';
import PropTypes from 'prop-types';

import { MAP, LIST, MIXED } from 'constants/DataTypes';

const UNSUPPORTED_TYPE_COLUMNS = {
  'CSV': new Set([MAP, LIST, MIXED])
};

import MenuItem from './MenuItem';
import Menu from './Menu';

const TYPES = [
  { label: 'JSON', name: 'JSON' },
  { label: 'CSV', name: 'CSV' },
  { label: 'Parquet', name: 'PARQUET' }
];

export default class ExportMenu extends PureComponent {
  static propTypes = {
    action: PropTypes.func,
    datasetColumns: PropTypes.array
  };

  static defaultMenuItem = TYPES[0]

  renderMenuItems() {
    const datasetColumns = new Set(this.props.datasetColumns);
    const isTypesIntersected = (types) => !![...types].filter(type => datasetColumns.has(type)).length;

    return TYPES.map(type => {
      const types = UNSUPPORTED_TYPE_COLUMNS[type.name];
      const disabled = types && isTypesIntersected(types);
      const onClick = disabled ? () => {} : () => this.props.action(type);

      return <MenuItem key={type.name} onClick={onClick} disabled={disabled}>{type.label}</MenuItem>;
    });
  }

  render() {
    return <Menu>{this.renderMenuItems()}</Menu>;
  }
}
