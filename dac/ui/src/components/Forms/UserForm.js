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
import Radium from 'radium';

import PropTypes from 'prop-types';

import { FieldWithError, TextField, PasswordField } from 'components/Fields';
import { applyValidators, isRequired, confirmPassword, isEmail } from 'utils/validation';
import { formRow } from 'uiTheme/radium/forms';
import {EDITION} from 'dyn-load/constants/serverStatus';
import localStorageUtils from 'utils/storageUtils/localStorageUtils';
import * as VersionUtils from '@app/utils/versionUtils';

//export for testing only
export const FIELDS = ['firstName', 'lastName', 'userName', 'email', 'password', 'passwordVerify', 'tag', 'extra'];

@Radium
export default class UserForm extends Component { // todo: rename, make proper "Section", since this is not a full "Form"
  static propTypes = {
    fields: PropTypes.object.isRequired,
    className: PropTypes.string,
    style: PropTypes.object,
    passwordHolderStyles: PropTypes.object,
    isReadMode: PropTypes.bool,
    noExtras: PropTypes.bool
  };

  static defaultProps = {
    style: {},
    passwordHolderStyles: {},
    isReadMode: false
  };

  //#region connectComplexForm.Sections region

  static getFields = () => FIELDS;
  static validate = (values) => { // todo: loc
    const validators = [
      isRequired('firstName', 'First Name'), isRequired('lastName', 'Last Name'),
      isRequired('userName', 'Username'), isEmail('email'), isRequired('email')
    ];
    if (values.tag === undefined) { // only require password for a new user
      validators.push(isRequired('password'), isRequired('passwordVerify', la('Confirm Password')));
    }
    validators.push(confirmPassword('password', 'passwordVerify'));
    return applyValidators(values, validators);
  };

  //#endregion

  getIsEdit() {
    const version = this.props.fields.tag.value;
    // fields.version.value holds empty string even when it should hold undefined.
    // this might be from this https://github.com/erikras/redux-form/issues/621
    return version !== undefined && version !== '';
  }

  render() {
    const { fields, style, passwordHolderStyles, isReadMode, className, noExtras } = this.props;
    const edition = VersionUtils.getEditionFromConfig();
    const isME = edition === EDITION.ME;
    const isAuthed = localStorageUtils.getInstanceId();

    return (
      <div style={style} className={className}>
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
        {!noExtras && !isAuthed && isME &&
          <div style={styles.formRow}>
            <FieldWithError label={la('Instance-id for Authentication')} errorPlacement='top' labelStyle={styles.label} {...fields.extra}
              style={styles.inlineBlock}>
              <TextField {...fields.extra} disabled={isReadMode}/>
            </FieldWithError>
          </div>
        }
      </div>
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
