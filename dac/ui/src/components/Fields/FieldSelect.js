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
import { Component } from 'react';
import PropTypes from 'prop-types';
import { injectIntl } from 'react-intl';
import { typeToIconType } from '@app/constants/DataTypes';
import Art from 'components/Art';
import Select from './Select';

@injectIntl
export default class FieldSelect extends Component {
  static propTypes = {
    formField: PropTypes.object,
    items: PropTypes.array,
    style: PropTypes.object,
    intl: PropTypes.object.isRequired
  }
  mapFieldsToOptions(items) {
    return items.map(item => ({
      label: (
        <div style={styles.wrap}>
          <Art
            src={`types/${typeToIconType[item.type]}.svg`}
            alt={this.props.intl.formatMessage({id: 'Common.FieldType'})}
            style={styles.icon}
          />
          <span style={styles.name}>{item.name}</span>
        </div>
      ),
      option: item.name
    }));
  }
  render() {
    const { items, formField, style } = this.props;
    return <Select {...formField} style={style} items={this.mapFieldsToOptions(items)} />;
  }
}

const styles = {
  wrap: {
    display: 'flex',
    justifyContent: 'flex-start',
    alignItems: 'center'
  },
  icon: {
    width: 24,
    height: 20
  },
  name: {
    marginLeft: 5
  }
};
