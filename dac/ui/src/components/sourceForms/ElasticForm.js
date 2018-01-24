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
import HostList from 'components/Forms/HostList';
import Credentials from 'components/Forms/Credentials';
import MetadataRefresh from 'components/Forms/MetadataRefresh';

import { Checkbox } from 'components/Fields';
import { ModalForm, FormBody, modalFormProps } from 'components/Forms';
import { connectComplexForm } from 'components/Forms/connectComplexForm';
import { section, sectionTitle, formRow } from 'uiTheme/radium/forms';
import { getCreatedSource } from 'selectors/resources';
import { FieldWithError, TextField } from 'components/Fields';
import AdvancedOptionsExpandable from 'components/Forms/AdvancedOptionsExpandable';
import { applyValidators, isNumber } from 'utils/validation';

import { ELASTIC } from 'dyn-load/constants/sourceTypes';

const SECTIONS = [General, HostList, Credentials, MetadataRefresh];
const DEFAULT_PORT = 9200;

export class ElasticForm extends Component {

  static sourceType = ELASTIC;

  static propTypes = {
    onFormSubmit: PropTypes.func.isRequired,
    onCancel: PropTypes.func.isRequired,
    handleSubmit: PropTypes.func.isRequired,
    editing: PropTypes.bool,
    fields: PropTypes.object.isRequired,
    formBodyStyle: PropTypes.object
  };

  static validate(values) {
    return {
      ...applyValidators(values, [
        isNumber('config.readTimeoutSeconds'),
        isNumber('config.scrollTimeoutSeconds')
      ])
    };
  }

  onSubmit = (values) => {
    const {onFormSubmit} = this.props;
    return onFormSubmit(this.mapFormatValues(values));
  }

  mapFormatValues(values) {
    const ret = {...values};
    // translate seconds back to milliseconds for server
    ret.config.readTimeoutMillis = Math.floor(ret.config.readTimeoutSeconds * 1000);
    ret.config.scrollTimeoutMillis = Math.floor(ret.config.scrollTimeoutSeconds * 1000);
    delete ret.config.readTimeoutSeconds;
    delete ret.config.scrollTimeoutSeconds;
    return ret;
  }

  render() {
    const {fields, editing, handleSubmit, formBodyStyle} = this.props;
    return (
      <ModalForm {...modalFormProps(this.props)} onSubmit={handleSubmit(this.onSubmit)}>
        <FormBody style={formBodyStyle}>
          <General fields={fields} editing={editing}>
            <HostList defaultPort={DEFAULT_PORT} fields={fields}/>
            <Credentials fields={fields}/>
            <div style={{...section, marginTop: 10}}>
              <h2 style={sectionTitle}>{la('Pushdown Options')}</h2>
              <div style={formRow}>
                <Checkbox {...fields.config.scriptsEnabled} label={la('Scripts enabled in Elasticsearch cluster')}/>
              </div>
            </div>
            <div style={{...section, marginTop: 10}}>
              <h2 style={sectionTitle}>{la('Advanced Options')}</h2>
              <AdvancedOptionsExpandable>
                <div style={formRow}>
                  <Checkbox {...fields.config.showHiddenIndices}
                    label={la('Show hidden indices that start with a dot (.)')}/>
                </div>
                <div style={formRow}>
                  <Checkbox {...fields.config.usePainless}
                    label={la('Use the Painless scripting language when connecting to Elasticsearch 5.0+ ' +
                        '(experimental)')}/>
                </div>
                <div style={formRow}>
                  <Checkbox {...fields.config.useWhitelist} label={la('Query whitelisted hosts only')}/>
                </div>
                <div style={formRow}>
                  <Checkbox {...fields.config.showIdColumn} label={la('Show _id column in elastic tables')}/>
                </div>
                <div style={formRow}>
                  <Checkbox {...fields.config.allowGroupByOnNormalizedFields} label={la('Use index/doc fields when pushing down aggregates and filters on analyzed and normalized fields (may produce unexpected results)')}/>
                </div>
                {/* Note that the timeouts are handled on the server in Milliseconds, but they are
                    presented to users in seconds (as the default values are quite large) */}
                <div style={formRow}>
                  <FieldWithError
                    label={la('Read Timeout (Seconds)')}
                    {...fields.config.readTimeoutSeconds}>
                    <TextField {...fields.config.readTimeoutSeconds}/>
                  </FieldWithError>
                </div>
                <div style={formRow}>
                  <FieldWithError
                    label={la('Scroll Timeout (Seconds)')}
                    {...fields.config.scrollTimeoutSeconds}>
                    <TextField {...fields.config.scrollTimeoutSeconds}/>
                  </FieldWithError>
                </div>
                <div style={formRow}>
                  <FieldWithError
                    label={la('Scroll Size')}
                    hoverHelpText={la('Configure scroll size for Elasticsearch requests Dremio makes. This setting must be less than or equal to your Elasticsearch\'s setting for index.max_result_window setting (typically defaults to 10,000).')}
                    {...fields.config.scrollSize}>
                    <TextField {...fields.config.scrollSize}/>
                  </FieldWithError>
                </div>
                <MetadataRefresh fields={fields}/>
              </AdvancedOptionsExpandable>
            </div>
            <div style={{...section, marginTop: 10}}>
              <h2 style={sectionTitle}>{la('Connection Options')}</h2>
              <div style={formRow}>
                <Checkbox {...fields.config.sslEnabled} label={la('SSL enabled connecting to Elasticsearch cluster')}/>
              </div>
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
      authenticationType: 'MASTER',
      hostList: [{port: DEFAULT_PORT}],
      scriptsEnabled: true,
      sslEnabled: false,
      showHiddenIndices: false,
      showIdColumn: false,
      usePainless: true,
      useWhitelist: false,
      // Note: from the server these are given as milliseconds and translated back and forth
      // when loading existing properties or uploading a new or changed config
      scrollTimeoutSeconds : 300,
      readTimeoutSeconds : 60,
      scrollSize: 4000,
      ...props.initialValues.config
    }
  };
  if (createdSource && createdSource.size > 1 && props.editing) {
    const hostList = createdSource.getIn(['config', 'hostList'])
      && createdSource.getIn(['config', 'hostList']).toJS();
    initialValues.config.hostList = hostList;
    initialValues.config.scrollTimeoutSeconds = createdSource.getIn(['config', 'scrollTimeoutMillis']) / 1000 || 300;
    initialValues.config.readTimeoutSeconds = createdSource.getIn(['config', 'readTimeoutMillis']) / 1000 || 60;
  }
  return {
    initialValues
  };
}

export default connectComplexForm({
  form: 'source',
  fields: ['config.scriptsEnabled', 'config.showHiddenIndices', 'config.sslEnabled', 'config.usePainless',
    'config.useWhitelist', 'config.showIdColumn', 'config.scrollTimeoutSeconds', 'config.readTimeoutSeconds',
    'config.scrollSize', 'config.allowGroupByOnNormalizedFields'],
  validate: ElasticForm.validate
}, SECTIONS, mapStateToProps, null)(ElasticForm);
