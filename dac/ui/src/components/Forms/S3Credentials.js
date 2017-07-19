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

import { description, section, sectionTitle, formRow } from 'uiTheme/radium/forms';

import { FieldWithError, TextField } from 'components/Fields';

export default class S3Credentials extends Component {
  static getFields() {
    return ['config.accessKey', 'config.accessSecret'];
  }

  static propTypes = {
    fields: PropTypes.object
  };

  static validate(values) {
    const errors = {config: {}};
    if (!values.config.externalBucketList.length) {
      if (!values.config.accessSecret) {
        errors.config.accessSecret = 'Access secret is required';
      }

      if (!values.config.accessKey) {
        errors.config.accessKey = 'Access key is required';
      }
    }
    return errors;
  }

  constructor(props) {
    super(props);
  }

  render() {
    const {fields: {config: {accessKey, accessSecret}}} = this.props;

    return (
      <div className='credentials' style={section}>
        <h3 style={sectionTitle}>{la('Credentials')}</h3>
        <div style={{display: 'inline-flex'}}>
          <FieldWithError label='AWS Access Key' {...accessKey} style={formRow}>
            <TextField {...accessKey}/>
          </FieldWithError>
          <FieldWithError label='AWS Access Secret' {...accessSecret} style={formRow}>
            <TextField type='password' {...accessSecret}/>
          </FieldWithError>
        </div>
        <div style={description}>
          Note: AWS credentials are not required for accessing public S3 buckets.
        </div>
      </div>
    );
  }
}
