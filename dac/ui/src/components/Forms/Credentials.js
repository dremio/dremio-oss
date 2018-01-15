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
import Radio from 'components/Fields/Radio';

import { formLabel } from 'uiTheme/radium/typography';
import { section, sectionTitle, formRow } from 'uiTheme/radium/forms';

export default class Credentials extends Component {
  static getFields() {
    return ['config.authenticationType', 'config.username', 'config.password'];
  }

  static propTypes = {
    fields: PropTypes.object.isRequired
  };

  static validate(values) {
    const errors = {config: {}};
    if (values.config.authenticationType === 'MASTER' && !values.config.username) {
      errors.config.username = 'Username is required unless you choose no authentication or user credentials';
    }
    if (values.config.authenticationType === 'MASTER' && !values.config.password) {
      errors.config.password = 'Password is required unless you choose no authentication or user credentials';
    }
    return errors;
  }

  constructor(props) {
    super(props);
  }

  render() {
    const {fields} = this.props;
    const {config: {authenticationType, username, password}} = fields;
    const inlineBlock = {display: 'inline-block'};
    return (
      <div className='credentials' style={section}>
        <h2 style={sectionTitle}>{la('Authentication')}</h2>
        <div style={formRow}>
          <Radio
            radioValue='ANONYMOUS'
            label='No Authentication'
            {...authenticationType}
            style={{...styles.radio, marginLeft: -5}}/>
          <Radio
            radioValue='MASTER'
            label='Master Credentials'
            {...authenticationType}
            style={[styles.radio, {marginLeft: 5}]}/>
        </div>
        <div style={formRow}>
          <FieldWithError errorPlacement='bottom' label='Username' {...username} style={inlineBlock}>
            <TextField className='name id' disabled={authenticationType.value !== 'MASTER'} {...username}/>
          </FieldWithError>
          <FieldWithError errorPlacement='bottom' label='Password' {...password} style={inlineBlock}>
            <TextField className='name key' type='password'
              disabled={authenticationType.value !== 'MASTER'} {...password}/>
          </FieldWithError>
        </div>
      </div>
    );
  }
}

const styles = {
  radio: {
    ...formLabel
  }
};
