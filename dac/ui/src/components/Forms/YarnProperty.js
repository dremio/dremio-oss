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
import {Component} from 'react';

import PropTypes from 'prop-types';

import FieldWithError from 'components/Fields/FieldWithError';
import TextField from 'components/Fields/TextField';
import Select from 'components/Fields/Select';

import {applyValidators, isRequired} from 'utils/validation';

export default class YarnProperty extends Component {
  static getFields() {
    return [
      'id', 'name', 'value', 'type'
    ];
  }

  static propTypes = {
    fields: PropTypes.object,
    style: PropTypes.object
  }

  static validate(values) {
    return applyValidators(values, [
      isRequired('name'),
      isRequired('value'),
      isRequired('type')
    ]);
  }

  options = [
    { label: 'Java', option: 'JAVA_PROP'},
    { label: 'System', option: 'SYSTEM_PROP'},
    { label: 'Environment', option: 'ENV_VAR'}
  ]

  render() {
    const {fields: {name, value, type}} = this.props;

    return (
      <div style={{display: 'flex'}}>
        <FieldWithError label='Type' {...type} style={{display: 'inline-block', marginRight: 10}}>
          <Select
            {...type}
            items={this.options}
            style={{width: 120}}/>
        </FieldWithError>
        <FieldWithError label='Name' {...name} style={{display: 'inline-block'}}>
          <TextField {...name} />
        </FieldWithError>
        <FieldWithError label='Value' {...value} style={{display: 'inline-block'}}>
          <TextField {...value} />
        </FieldWithError>
      </div>
    );
  }
}
