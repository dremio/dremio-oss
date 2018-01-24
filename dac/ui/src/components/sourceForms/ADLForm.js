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

import General from 'components/Forms/General';
import MetadataRefresh from 'components/Forms/MetadataRefresh';
import { ModalForm, FormBody, modalFormProps } from 'components/Forms';
import { connectComplexForm } from 'components/Forms/connectComplexForm';
import { Select } from 'components/Fields';
import { section, sectionTitle, formRow } from 'uiTheme/radium/forms';
import AdvancedOptionsExpandable from 'components/Forms/AdvancedOptionsExpandable';
import { FieldWithError, TextField } from 'components/Fields';
import SourceProperties from 'components/Forms/SourceProperties';

import { getCreatedSource } from 'selectors/resources';

import { ADL } from 'dyn-load/constants/sourceTypes';

const SECTIONS = [General, SourceProperties, MetadataRefresh];

export class ADLForm extends Component {

  static sourceType = ADL;

  static propTypes = {
    onFormSubmit: PropTypes.func.isRequired,
    onCancel: PropTypes.func.isRequired,
    handleSubmit: PropTypes.func.isRequired,
    editing: PropTypes.bool,
    fields: PropTypes.object,
    formBodyStyle: PropTypes.object
  };

  authModeOptions = [
    { label: la('Refresh Token'), option: 'REFRESH_TOKEN' },
    { label: la('Client Key'), option: 'CLIENT_KEY' }
  ];

  render() {
    const {fields, editing, handleSubmit, onFormSubmit, formBodyStyle} = this.props;

    // TODO: For now we only allow Client Key authentication
    return (
      <ModalForm {...modalFormProps(this.props)} onSubmit={handleSubmit(onFormSubmit)}>
        <FormBody style={formBodyStyle}>
          <General fields={fields} editing={editing}>
            <div style={section}>
              <h2 style={sectionTitle}>{la('Authentication')}</h2>
              { false && <div style={{...formRow, marginTop: 10}}>
                <FieldWithError {...fields.config.mode} label={la('Authentication mode')} errorPlacement='right'>
                  <div>
                    <Select
                      {...fields.config.mode}
                      items={this.authModeOptions}
                      buttonStyle={{ textAlign: 'left' }}
                    />
                  </div>
                </FieldWithError>
              </div>}
              <div style={{...formRow, marginTop: 10}}>
                <FieldWithError label={la('Resource Name')} {...fields.config.accountName}>
                  <TextField {...fields.config.accountName}/>
                </FieldWithError>
              </div>
              <div style={formRow}>
                <FieldWithError label={la('Application Id')} {...fields.config.clientId}>
                  <TextField {...fields.config.clientId}/>
                </FieldWithError>
              </div>
              { fields.config.mode.value === 'REFRESH_TOKEN' && <div style={formRow}>
                <FieldWithError label={la('Token Secret')} {...fields.config.refreshTokenSecret}>
                  <TextField {...fields.config.refreshTokenSecret}/>
                </FieldWithError>
              </div>
              }
              { fields.config.mode.value === 'CLIENT_KEY' && <div>
                <div style={formRow}>
                  <FieldWithError label={la('OAuth 2.0 Token Endpoint')} {...fields.config.clientKeyRefreshUrl}>
                    <TextField {...fields.config.clientKeyRefreshUrl}/>
                  </FieldWithError>
                </div>
                <div style={formRow}>
                  <FieldWithError label={la('Password')} {...fields.config.clientKeyPassword}>
                    <TextField {...fields.config.clientKeyPassword}/>
                  </FieldWithError>
                </div>
              </div>
              }
            </div>
            <SourceProperties fields={fields} />
            <div style={section}>
              <h2 style={sectionTitle}>{la('Advanced Options')}</h2>
              <AdvancedOptionsExpandable>
                <MetadataRefresh fields={fields}/>
              </AdvancedOptionsExpandable>
            </div>
          </General>
        </FormBody>
      </ModalForm>
    );
  }
}

function mapStateToProps(state, props) {
  const createdSource = getCreatedSource(state);
  const initialValues = {
    ...props.initialValues,
    config: {
      mode: 'CLIENT_KEY',
      clientId: '',
      accountName: '',
      clientKeyRefreshUrl: '',
      clientKeyPassword: '',
      ...props.initialValues.config
    }
  };

  if (createdSource && createdSource.size > 1 && props.editing) {
    const propertyList = createdSource.getIn(['config', 'propertyList'])
      && createdSource.getIn(['config', 'propertyList']).toJS() || [];
    initialValues.config.propertyList = propertyList;
  }

  return {
    initialValues
  };
}

export default connectComplexForm({
  form: 'source',
  fields: ['config.mode', 'config.accountName', 'config.clientId', 'config.refreshTokenSecret', 'config.clientKeyRefreshUrl', 'config.clientKeyPassword']
}, SECTIONS, mapStateToProps, null)(ADLForm);

