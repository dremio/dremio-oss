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
import classNames from 'classnames';
import { get } from 'lodash/object';

import FormUtils from 'utils/FormUtils/FormUtils';

import { FieldWithError, Radio, TextField } from 'components/Fields';

import { rowOfInputsSpacing, rowOfRadio } from '@app/uiTheme/less/forms.less';
import { flexContainer, flexElementAuto } from '@app/uiTheme/less/layout.less';

const { CONFIG_PROP_NAME, addFormPrefixToPropName } = FormUtils;

const DEFAULT_TEXT_FIELDS = [
  {propName: 'username', label: 'Username', errMsg: 'Username is required unless you choose no authentication.'},
  {propName: 'password', label: 'Password', errMsg: 'Password is required unless you choose no authentication.', secure: true}
];

const DEFAULT_RADIO_OPTIONS = [
  { label: 'No Authentication', option: 'ANONYMOUS' },
  { label: 'Master Credentials', option: 'MASTER' }
];
export const AUTHENTICATION_TYPE_FIELD = addFormPrefixToPropName('authenticationType');
export const USER_NAME_FIELD = addFormPrefixToPropName('username');
export const PASSWORD_FIELD = addFormPrefixToPropName('password');
export const SECRET_RESOURCE_URL_FIELD = addFormPrefixToPropName('secretResourceUrl');

function validate(values, elementConfig) {
  let errors = { [CONFIG_PROP_NAME]: {}};
  const textFields = (elementConfig && elementConfig.textFields) ? elementConfig.textFields : DEFAULT_TEXT_FIELDS;
  const isMasterAuth = get(values, AUTHENTICATION_TYPE_FIELD) === 'MASTER';

  const hasSecretResourceUrl = !!get(values, SECRET_RESOURCE_URL_FIELD);

  errors = textFields.reduce((accumulator, textField) => {
    if (isMasterAuth && !values[CONFIG_PROP_NAME][textField.propName]) {
      // password is not required if secretResourceUrl is set
      if (textField.propName === 'password' && hasSecretResourceUrl) {
        return;
      }

      accumulator[CONFIG_PROP_NAME][textField.propName] = textField.errMsg || `${textField.label} is required unless you choose no authentication`;
    }
    return accumulator;
  }, errors);
  return errors;
}

// credentials is not configurable
const FIELDS = [AUTHENTICATION_TYPE_FIELD, USER_NAME_FIELD, PASSWORD_FIELD];

export default class Credentials extends Component {

  static getFields() {
    return FIELDS;
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
    const {fields} = this.props;
    const authenticationTypeField = get(fields, AUTHENTICATION_TYPE_FIELD);
    const textFields = this.getTextFields();
    const radioOptions = this.getRadioOptions();
    return (
      <div>
        {radioOptions &&
        <div className={classNames(rowOfInputsSpacing, rowOfRadio)}>
          {radioOptions.map((option, index) => {
            return (
              <Radio radioValue={option.option}
                key={index}
                label={option.label}
                {...authenticationTypeField}/>
            );
          })}
        </div>
        }
        <div className={classNames(rowOfInputsSpacing, flexContainer)}>
          {
            textFields.map((textField, index) => {
              const field = FormUtils.getFieldByComplexPropName(fields, addFormPrefixToPropName(textField.propName));
              const type = (textField.secure) ? {type: 'password'} : {};
              return (
                <FieldWithError errorPlacement='bottom' label={textField.label} key={index} {...field} className={flexElementAuto}>
                  <TextField style={{width: '100%'}} {...type} {...field} key={index}
                    disabled={authenticationTypeField.value === 'ANONYMOUS'} {...field}/>
                </FieldWithError>
              );
            })
          }
        </div>
      </div>
    );
  }

  getTextFields() {
    const {elementConfig} = this.props;

    return (elementConfig && elementConfig.textFields) ? elementConfig.textFields : DEFAULT_TEXT_FIELDS;
  }

  getRadioOptions() {
    const {elementConfig} = this.props;

    return (elementConfig && elementConfig.radioOptions) ? elementConfig.radioOptions : DEFAULT_RADIO_OPTIONS;
  }
}
