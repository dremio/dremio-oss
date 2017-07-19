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
import Credentials from 'components/Forms/Credentials';
import MetadataRefresh from 'components/Forms/MetadataRefresh';


import { FieldWithError, TextField } from 'components/Fields';

import { ModalForm, FormBody, modalFormProps } from 'components/Forms';
import { connectComplexForm } from 'components/Forms/connectComplexForm';
import { section, sectionTitle } from 'uiTheme/radium/forms';
import { applyValidators, isRequired } from 'utils/validation';
import AdvancedOptionsExpandable from 'components/Forms/AdvancedOptionsExpandable';

class InstanceSection {
  static validate(values) {
    return applyValidators(values, [isRequired('config.instance', 'instance')]);
  }
}

const SECTIONS = [General, SingleHost, InstanceSection, Credentials, MetadataRefresh];
const DEFAULT_PORT = 1521;

export class Oracle extends Component {

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
            <SingleHost fields={fields} title='Host' style={{marginBottom: 15}}/>
            <div style={section}>
              <FieldWithError label='SID' {...fields.config.instance}>
                <TextField {...fields.config.instance}/>
              </FieldWithError>
            </div>
            <Credentials fields={fields}/>
            <div style={{...section}}>
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
  fields: ['config.instance']
}, SECTIONS, mapStateToProps, null)(Oracle);
