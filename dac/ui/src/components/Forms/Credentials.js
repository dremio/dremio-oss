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
import classNames from 'classnames';

import FormUtils from 'utils/FormUtils/FormUtils';

import { FieldWithError, TextField, Radio } from 'components/Fields';

import { rowOfInputsSpacing, rowOfRadio } from '@app/uiTheme/less/forms.less';
import { flexContainer, flexElementAuto } from '@app/uiTheme/less/layout.less';

const DEFAULT_TEXT_FIELDS = [
  {propName: 'username', label: 'Username', errMsg: 'Username is required unless you choose no authentication.'},
  {propName: 'password', label: 'Password', errMsg: 'Password is required unless you choose no authentication.', secure: true}
];

const DEFAULT_RADIO_OPTIONS = [
  { label: 'No Authentication', option: 'ANONYMOUS' },
  { label: 'Master Credentials', option: 'MASTER' }
];

function validate(values, elementConfig) {
  let errors = {config: {}};
  const textFields = (elementConfig && elementConfig.textFields) ? elementConfig.textFields : DEFAULT_TEXT_FIELDS;

  errors = textFields.reduce((accumulator, textField) => {
    if (values.config.authenticationType === 'MASTER' && !values.config[textField.propName]) {
      accumulator.config[textField.propName] = textField.errMsg || `${textField.label} is required unless you choose no authentication`;
    }
    return accumulator;
  }, errors);
  return errors;
}

export default class Credentials extends Component {

  static getFields() {
    // credentials is not configurable
    return ['config.authenticationType', 'config.username', 'config.password'];
  }

  static getValidators(elementConfig) {
    return function(values) {
      return validate(values, elementConfig);
    };
  }

  static propTypes = {
    fields: PropTypes.object.isRequired,
    elementConfig: PropTypes.object
  };

  constructor(props) {
    super(props);
  }

  render() {
    const {fields, elementConfig} = this.props;
    const {config: {authenticationType}} = fields;
    const textFields = (elementConfig && elementConfig.textFields) ? elementConfig.textFields : DEFAULT_TEXT_FIELDS;
    const radioOptions = (elementConfig && elementConfig.radioOptions) ? elementConfig.radioOptions : DEFAULT_RADIO_OPTIONS;
    return (
      <div>
        {radioOptions &&
          <div className={classNames(rowOfInputsSpacing, rowOfRadio)}>
            {radioOptions.map((option, index) => {
              return (
                <Radio radioValue={option.option}
                       key={index}
                       label={option.label}
                       {...authenticationType}/>
              );
            })}
          </div>
        }
        <div className={classNames(rowOfInputsSpacing, flexContainer)}>
          {
            textFields.map((textField, index) => {
              const field = FormUtils.getFieldByComplexPropName(fields, `config.${textField.propName}`);
              const type = (textField.secure) ? {type: 'password'} : {};
              return (
                <FieldWithError errorPlacement='bottom' label={textField.label} key={index} {...field} className={flexElementAuto}>
                  <TextField style={{width: '100%'}} {...type} {...field} key={index}
                             disabled={authenticationType.value !== 'MASTER'} {...field}/>
                </FieldWithError>
              );
            })
          }
        </div>
      </div>
    );
  }
}
