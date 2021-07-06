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

const SECRET_URL_FIELD_NAME = 'secretResourceUrl';
const KERBEROS_FIELD_NAME = 'useKerberos';
const AWS_PROFILE_FIELD_NAME = 'awsProfile';
const DB_USER_FIELD_NAME = 'dbUser';
const DEFAULT_TEXT_FIELDS = [
  {propName: 'username', label: 'Username', errMsg: 'Username is required unless you choose no authentication.'},
  {propName: 'password', label: 'Password', errMsg: 'Password is required unless you choose no authentication.', secure: true},
  {propName: SECRET_URL_FIELD_NAME, label: 'Secret Resource Url', errMsg: 'Secret Resource Url is required unless you choose no authentication.'},
  {propName: AWS_PROFILE_FIELD_NAME, label: 'AWS Profile', errMsg: 'Some Error Message'},
  {propName: DB_USER_FIELD_NAME, label: 'DbUser', errMsg: 'Some Error Message'}
];

const AUTH_TYPE = {anonymous: 'ANONYMOUS', master: 'MASTER', secret: 'SECRET', kerberos: 'KERBEROS', awsProfile: 'AWS_PROFILE'};
const DEFAULT_RADIO_OPTIONS = [
  { label: 'No Authentication', option: AUTH_TYPE.anonymous },
  { label: 'Master Credentials', option: AUTH_TYPE.master }
];
const SECRET_URL_OPTION = { label: 'Secret Resource Url', option: AUTH_TYPE.secret };
const KERBEROS_OPTION = { label: 'Kerberos', option: AUTH_TYPE.kerberos };
const AWS_PROFILE_OPTION = { label: 'AWS Profile', option: AUTH_TYPE.awsProfile };
export const AUTHENTICATION_TYPE_FIELD = addFormPrefixToPropName('authenticationType');
export const USER_NAME_FIELD = addFormPrefixToPropName('username');
export const PASSWORD_FIELD = addFormPrefixToPropName('password');
export const SECRET_RESOURCE_URL_FIELD = addFormPrefixToPropName(SECRET_URL_FIELD_NAME);
export const KERBEROS_FIELD = addFormPrefixToPropName(KERBEROS_FIELD_NAME);
export const PROFILE_NAME_FIELD = addFormPrefixToPropName(AWS_PROFILE_FIELD_NAME);
export const DB_USER_FIELD = addFormPrefixToPropName(DB_USER_FIELD_NAME);

function validate(values, elementConfig) {
  let errors = { [CONFIG_PROP_NAME]: {}};
  const textFields = (elementConfig && elementConfig.textFields) ? elementConfig.textFields : DEFAULT_TEXT_FIELDS;
  const authTypeValue = get(values, AUTHENTICATION_TYPE_FIELD);

  if (authTypeValue === AUTH_TYPE.anonymous
      || authTypeValue === AUTH_TYPE.kerberos) {
    return errors;
  }

  const isMasterAuth = authTypeValue === AUTH_TYPE.master;
  const isSecretAuth = authTypeValue === AUTH_TYPE.secret;
  const isProfileAuth = authTypeValue === AUTH_TYPE.awsProfile;
  errors = textFields.reduce((accumulator, textField) => {
    if (!values[CONFIG_PROP_NAME][textField.propName]) {
      if (textField.propName === 'username' && !isProfileAuth
        || textField.propName === 'password' && isMasterAuth
        || textField.propName === SECRET_URL_FIELD_NAME && isSecretAuth
        || textField.propName === AWS_PROFILE_FIELD_NAME && isProfileAuth
        || textField.propName === DB_USER_FIELD_NAME && isProfileAuth) {
        accumulator[CONFIG_PROP_NAME][textField.propName] = textField.errMsg || `${textField.label} is required unless you choose no authentication`;
      }
    }
    return accumulator;
  }, errors);
  return errors;
}

// credentials is not configurable via container_selection
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

  typeRadioTouched = false;

  onRadioTouch = () => {
    this.typeRadioTouched = true;
  };

  render() {
    const {fields} = this.props;
    const authenticationTypeField = get(fields, AUTHENTICATION_TYPE_FIELD);
    const textFields = this.getTextFields();
    const radioOptions = this.getRadioOptions();
    if (!this.typeRadioTouched) {
      this.setPriorOptionIfProvided(authenticationTypeField);
    }
    this.setUseKerberos(AUTH_TYPE.kerberos === authenticationTypeField.value);

    return (
      <div>
        {radioOptions &&
        <div className={classNames(rowOfInputsSpacing, rowOfRadio)} onClick={this.onRadioTouch}>
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
        {authenticationTypeField.value !== AUTH_TYPE.anonymous && authenticationTypeField.value !== AUTH_TYPE.kerberos &&
        <div className={classNames(rowOfInputsSpacing, flexContainer)}>
          {
            textFields.map((textField, index) => {
              const field = FormUtils.getFieldByComplexPropName(fields, addFormPrefixToPropName(textField.propName));
              const type = (textField.secure) ? {type: 'password'} : {};
              if (
                (authenticationTypeField.value === AUTH_TYPE.master && (textField.propName === 'username' || textField.propName === 'password')) ||
                (authenticationTypeField.value === AUTH_TYPE.secret && (textField.propName === 'username' || textField.propName === SECRET_URL_FIELD_NAME)) ||
                (authenticationTypeField.value === AUTH_TYPE.awsProfile && (textField.propName === AWS_PROFILE_FIELD_NAME || textField.propName === DB_USER_FIELD_NAME))) {
                return (
                  <FieldWithError errorPlacement='bottom' label={textField.label} key={index}
                    {...field} className={flexElementAuto}>
                    <TextField style={{width: '100%'}} {...type} {...field} key={index} {...field}/>
                  </FieldWithError>);
              } else {
                return null;
              }
            })
          }
        </div>
        }
      </div>
    );
  }

  getDefaultTextFields() {
    const secretField = this.getSecretField();
    const awsProfileField = this.getAWSProfileField() && this.getDbUserField();
    let textFields = (secretField ? DEFAULT_TEXT_FIELDS : DEFAULT_TEXT_FIELDS.filter(field => field.propName !== SECRET_URL_FIELD_NAME));
    textFields = (awsProfileField ? textFields : textFields.filter(field => field.propName !== AWS_PROFILE_FIELD_NAME || field.propName !== DB_USER_FIELD_NAME));
    return textFields;
  }

  getTextFields() {
    const {elementConfig} = this.props;
    return (elementConfig && elementConfig.textFields) ? elementConfig.textFields : this.getDefaultTextFields();
  }

  getRadioOptions() {
    const {elementConfig} = this.props;
    let options = (elementConfig && elementConfig.radioOptions) ? elementConfig.radioOptions : DEFAULT_RADIO_OPTIONS;
    options = (this.getSecretField()) ? [...options, SECRET_URL_OPTION] : options;
    options = (this.getAWSProfileField() && this.getDbUserField()) ? [...options, AWS_PROFILE_OPTION] : options;
    return (this.getKerberosField()) ? [...options, KERBEROS_OPTION] : options;
  }

  getSecretField = () => {
    const {fields} = this.props;
    return get(fields, SECRET_RESOURCE_URL_FIELD);
  };

  getAWSProfileField = () => {
    const {fields} = this.props;
    return get(fields, PROFILE_NAME_FIELD);
  };

  getDbUserField = () => {
    const {fields} = this.props;
    return get(fields, DB_USER_FIELD);
  };

  getKerberosField = () => {
    const {fields} = this.props;
    return get(fields, KERBEROS_FIELD);
  };

  setUseKerberos = (useKerberos) => {
    const kerberosField = this.getKerberosField();
    if (kerberosField) {
      kerberosField.checked = useKerberos;
    }
  }

  setPriorOptionIfProvided = (authTypeField) => {
    const secretField = this.getSecretField();
    if (secretField && secretField.value) {
      authTypeField.value = AUTH_TYPE.secret;
    }
    const kerberosField = this.getKerberosField();
    if (kerberosField && kerberosField.checked) {
      authTypeField.value = AUTH_TYPE.kerberos;
    }
  };

}
