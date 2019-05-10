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
import Radium from 'radium';

import PropTypes from 'prop-types';

import { FormBody } from 'components/Forms';
import { FieldWithError, TextField, PasswordField } from 'components/Fields';
import { applyValidators, isRequired, confirmPassword, isEmail } from 'utils/validation';
import { formRow } from 'uiTheme/radium/forms';

const FIELDS = ['firstName', 'lastName', 'userName', 'email', 'password', 'passwordVerify', 'version'];

@Radium
export default class UserForm extends Component { // todo: rename, make proper "Section", since this is not a full "Form"
  static propTypes = {
    fields: PropTypes.object.isRequired,
    style: PropTypes.object,
    passwordHolderStyles: PropTypes.object,
    isReadMode: PropTypes.bool
  };

  static defaultProps = {
    style: {},
    passwordHolderStyles: {},
    isReadMode: false
  }

  getIsEdit() {
    const version = this.props.fields.version.value;
    // fields.version.value holds empty string even when it should hold undefined.
    // this might be from this https://github.com/erikras/redux-form/issues/621
    return version !== undefined && version !== '';
  }

  render() {
    const { fields, style, passwordHolderStyles, isReadMode } = this.props;

    return (
      <FormBody style={style}>
        <div style={styles.formRow}>
          <FieldWithError
            label={la('First Name')} errorPlacement='top' labelStyle={styles.label} {...fields.firstName}
            style={styles.inlineBlock}>
            <TextField initialFocus {...fields.firstName} disabled={isReadMode}/>
          </FieldWithError>
          <FieldWithError label={la('Last Name')} errorPlacement='top' labelStyle={styles.label} {...fields.lastName}
            style={styles.inlineBlock}>
            <TextField {...fields.lastName} disabled={isReadMode}/>
          </FieldWithError>
        </div>
        <div style={styles.formRow}>
          <FieldWithError label={la('Username')} errorPlacement='top' labelStyle={styles.label} {...fields.userName}
            style={styles.inlineBlock}>
            <TextField {...fields.userName} disabled={this.getIsEdit() || isReadMode}/>
          </FieldWithError>
          <FieldWithError label={la('Email')} errorPlacement='top' labelStyle={styles.label} {...fields.email}>
            <TextField {...fields.email} disabled={isReadMode}/>
          </FieldWithError>
        </div>
        {!isReadMode &&
          <div style={passwordHolderStyles}>
            <div style={styles.formRow}>
              <div className='field-item' style={styles.formItem}>
                <FieldWithError
                  label={la('Password')} errorPlacement='right' labelStyle={styles.label} {...fields.password}>
                  <PasswordField {...fields.password}/>
                </FieldWithError>
              </div>
            </div>
            <div style={styles.formRow}>
              <div className='field-item' style={styles.formItem}>
                <FieldWithError
                  label={la('Confirm Password')}
                  errorPlacement='right'
                  labelStyle={styles.label}
                  {...fields.passwordVerify}>
                  <PasswordField {...fields.passwordVerify}/>
                </FieldWithError>
              </div>
            </div>
          </div>
        }
      </FormBody>
    );
  }
}

const styles = {
  formRow: {
    ...formRow,
    display: 'flex'
  },
  inlineBlock: {
    display: 'inline-block',
    marginRight: 20
  },
  label: {
    margin: '0 0 4px'
  },
  selectStyle: {
    display: 'flex',
    width: 75,
    margin: '0 10px 0 0'
  }
};

export const userFormValidate = (values) => { // todo: loc
  const validators = [
    isRequired('firstName', 'First Name'), isRequired('lastName', 'Last Name'),
    isRequired('userName', 'Username'), isEmail('email'), isRequired('email')
  ];
  if (values.version === undefined) { // only require password for a new user
    validators.push(isRequired('password'), isRequired('passwordVerify', la('Confirm Password')));
  }
  validators.push(confirmPassword('password', 'passwordVerify'));
  return applyValidators(values, validators);
};

export const userFormFields = FIELDS;
