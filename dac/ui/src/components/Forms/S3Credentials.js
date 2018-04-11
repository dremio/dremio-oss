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

import { description, section, sectionTitle, formRow } from 'uiTheme/radium/forms';

import { FieldWithError, TextField } from 'components/Fields';

export default class S3Credentials extends Component {
  static getFields() {
    return ['config.accessKey', 'config.accessSecret'];
  }

  static mutateSubmitValues(values) {
    // fix for DX-8437 Clicking in the access key box breaks anonymous s3 addition
    if (!values.config.accessKey) delete values.config.accessKey;
    if (!values.config.accessSecret) delete values.config.accessSecret;
  }

  static propTypes = {
    fields: PropTypes.object
  };

  static validate(values) {
    const errors = {config: {}};
    if (values.config.accessKey || values.config.accessSecret) {
      const error = la('Both access secret and key are required for private S3 buckets.');
      if (!values.config.accessKey) {
        errors.config.accessKey = error;
      }
      if (!values.config.accessSecret) {
        errors.config.accessSecret = error;
      }
    }
    return errors;
  }

  render() {
    const {fields: {config: {accessKey, accessSecret}}} = this.props;

    return (
      <div className='credentials' style={section}>
        <h2 style={sectionTitle}>{la('Credentials')}</h2>
        <div style={{display: 'inline-flex'}}>
          <FieldWithError errorPlacement='top' label={la('AWS Access Key')} {...accessKey} style={formRow}>
            <TextField {...accessKey}/>
          </FieldWithError>
          <FieldWithError errorPlacement='top' label={la('AWS Access Secret')} {...accessSecret} style={formRow}>
            <TextField type='password' {...accessSecret}/>
          </FieldWithError>
        </div>
        <div style={description}>
          {la('Note: AWS credentials are not required for accessing public S3 buckets.')}
        </div>
      </div>
    );
  }
}
