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

import General from 'components/Forms/General';
import SingleHost from 'components/Forms/SingleHost';
import SourceProperties from 'components/Forms/SourceProperties';
import MetadataRefresh from 'components/Forms/MetadataRefresh';

import { Checkbox } from 'components/Fields';
import FieldWithError from 'components/Fields/FieldWithError';
import TextField from 'components/Fields/TextField';
import { ModalForm, FormBody, modalFormProps } from 'components/Forms';
import { connectComplexForm } from 'components/Forms/connectComplexForm';
import { getCreatedSource } from 'selectors/resources';
import { section, sectionTitle, formRow } from 'uiTheme/radium/forms';
import AdvancedOptionsExpandable from 'components/Forms/AdvancedOptionsExpandable';

const SECTIONS = [General, SingleHost, SourceProperties, MetadataRefresh];
const DEFAULT_PORT = 9083;

export class Hive extends Component {

  static propTypes = {
    onFormSubmit: PropTypes.func.isRequired,
    onCancel: PropTypes.func.isRequired,
    handleSubmit: PropTypes.func.isRequired,
    editing: PropTypes.bool,
    fields: PropTypes.object,
    formBodyStyle: PropTypes.object
  };

  render() {
    const {fields, editing, handleSubmit, onFormSubmit, formBodyStyle} = this.props;
    const description = `These options will be added to your Hive connection string. Please see the Dremio documentation
                         for a list of commonly used connection string options.`;
    return (
      <ModalForm {...modalFormProps(this.props)} onSubmit={handleSubmit(onFormSubmit)}>
        <FormBody style={formBodyStyle}>
          <General fields={fields} editing={editing}>
            <SingleHost fields={fields} title={la('Hive Metastore')}/>
            <div style={{...section, marginTop: 10}}>
              <h3 style={sectionTitle}>{la('Security')}</h3>
              <div style={formRow}>
                <Checkbox {...fields.config.enableSasl} label={la('Enable SASL')}/>
                <FieldWithError label={la('Kerberos Principal')}
                  {...fields.config.kerberosPrincipal} style={{display: 'inline-block', marginTop: 10}}>
                  <TextField {...fields.config.kerberosPrincipal}/>
                </FieldWithError>
              </div>
            </div>
            <SourceProperties fields={fields} title={la('Connection String Options')} description={description}/>
            <div style={section}>
              <h3 style={sectionTitle}>{la('Advanced Options')}</h3>
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
      port: DEFAULT_PORT,
      enableSasl: false,
      ...props.initialValues.config
    }
  };
  if (createdSource && createdSource.size > 1 && props.editing) {
    const propertyList = createdSource.get('config').get('propertyList')
      && createdSource.get('config').get('propertyList').toJS() || [];
    initialValues.config.propertyList = propertyList;
  }
  return {
    initialValues
  };
}

export default connectComplexForm({
  form: 'source',
  fields: ['config.enableSasl', 'config.kerberosPrincipal']
}, SECTIONS, mapStateToProps, null)(Hive);
