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
import SingleHost from 'components/Forms/SingleHost';
import Credentials from 'components/Forms/Credentials';
import JDBCOptions from 'components/Forms/JDBCOptions';
import MetadataRefresh from 'components/Forms/MetadataRefresh';

import { ModalForm, FormBody, modalFormProps } from 'components/Forms';
import { connectComplexForm } from 'components/Forms/connectComplexForm';
import { FieldWithError, TextField } from 'components/Fields';
import { section, sectionTitle } from 'uiTheme/radium/forms';
import { applyValidators, isRequired } from 'utils/validation';
import AdvancedOptionsExpandable from 'components/Forms/AdvancedOptionsExpandable';

import { POSTGRESQL } from 'dyn-load/constants/sourceTypes';

class DatabaseSection {
  static validate(values) {
    return applyValidators(values, [isRequired('config.databaseName', 'Database Name')]);
  }
}

const SECTIONS = [General, SingleHost, DatabaseSection, Credentials, JDBCOptions, MetadataRefresh];
const DEFAULT_PORT = 5432;

export class PostgreSQLForm extends Component {

  static sourceType = POSTGRESQL;

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
    return (
      <ModalForm {...modalFormProps(this.props)} onSubmit={handleSubmit(onFormSubmit)}>
        <FormBody style={formBodyStyle}>
          <General fields={fields} editing={editing}>
            <SingleHost fields={fields} title='Connection'/>
            <div style={{...section, marginTop: -20}}>
              <FieldWithError label='Database' {...fields.config.databaseName}>
                <TextField {...fields.config.databaseName}/>
              </FieldWithError>
            </div>
            <Credentials fields={fields}/>
            <div style={section}>
              <h2 style={sectionTitle}>{la('Advanced Options')}</h2>
              <AdvancedOptionsExpandable>
                <JDBCOptions fields={fields}/>
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
  const initialValues = {
    ...props.initialValues,
    config: {
      port: DEFAULT_PORT,
      authenticationType: 'MASTER',
      ...props.initialValues.config
    }
  };
  return {
    initialValues
  };
}

export default connectComplexForm({
  form: 'source',
  fields: ['config.databaseName']
}, SECTIONS, mapStateToProps, null)(PostgreSQLForm);
