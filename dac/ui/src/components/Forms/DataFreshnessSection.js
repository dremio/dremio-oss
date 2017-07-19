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
import { section, label } from 'uiTheme/radium/forms';
import { TextField, Select } from 'components/Fields';
import HoverHelp from 'components/HoverHelp';

@Radium
export default class DataFreshnessSection extends Component {
  static propTypes = {
    fields: PropTypes.object,
    entityType: PropTypes.string
  }

  static defaultFormValue() {
    return  {
      unit: 'DAYS',
      duration: 1
    };
  }

  static getFields() {
    return ['accelerationTTL.unit', 'accelerationTTL.duration'];
  }

  periodOptions = [ // todo: proper (localized) pluralization
    { label: la('Minute(s)'), option: 'MINUTES' },
    { label: la('Hour(s)'), option: 'HOURS' },
    { label: la('Day(s)'), option: 'DAYS' },
    { label: la('Week(s)'), option: 'WEEKS' }
  ]

  render() {
    const { fields: { accelerationTTL }, entityType } = this.props;
    const helpContent = la(`Maximum time to live for Reflections using data from this ${entityType}.`);
    return (
      <div style={section}>
        <span style={styles.label}>
          {la('Data Freshness')}
          <HoverHelp content={helpContent} />
        </span>
        <div style={{ display: 'flex' }}>
          <TextField {...accelerationTTL.duration} type='number' style={{ width: 32 }} step={1} min={1} />
          <Select
            {...accelerationTTL.unit}
            items={this.periodOptions}
            buttonStyle={{ textAlign: 'left' }}
            style={styles.select}
          />
        </div>
      </div>
    );
  }
}

const styles = {
  select: {
    width: 164
  },
  label: {
    ...label,
    display: 'flex',
    alignItems: 'center'
  }
};
