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

import PropTypes from 'prop-types';

import FieldWithError from 'components/Fields/FieldWithError';
import TextField from 'components/Fields/TextField';

import { applyValidators, isRequired } from 'utils/validation';

export default class Property extends Component {
  static getFields() {
    return [
      'id', 'name', 'value'
    ];
  }

  static propTypes = {
    fields: PropTypes.object,
    style: PropTypes.object
  }

  static validate(values) {
    return applyValidators(values, [
      isRequired('name'),
      isRequired('value')
    ]);
  }

  render() {
    const {style, fields: {name, value}} = this.props;

    return (
      <div style={style}>
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
