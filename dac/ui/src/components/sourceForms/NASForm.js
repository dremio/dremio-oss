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

import { FieldWithError, TextField } from 'components/Fields';

import { ModalForm, FormBody, modalFormProps } from 'components/Forms';
import { connectComplexForm } from 'components/Forms/connectComplexForm';
import { section, sectionTitle, formRow, description } from 'uiTheme/radium/forms';
import { applyValidators, isRequired } from 'utils/validation';
import AdvancedOptionsExpandable from 'components/Forms/AdvancedOptionsExpandable';

import { NAS } from 'dyn-load/constants/sourceTypes';

const SECTIONS = [General, MetadataRefresh];

function validate(values) {
  return applyValidators(values, [isRequired('config.path', 'Path')]);
}

export class NASForm extends Component {

  static sourceType = NAS;

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
            <div style={section}>
              <h2 style={sectionTitle}>{la('Mount')}</h2>
              <div style={formRow}>
                <FieldWithError errorPlacement='top' label={la('Path')} {...fields.config.path}>
                  <TextField {...fields.config.path}/>
                </FieldWithError>
              </div>
              <div style={description}>
                {la('Note: This path must exist on all nodes in the Dremio cluster.')}
              </div>
            </div>
            <div style={section}>
              <h2 style={sectionTitle}>{la('Advanced Options')}</h2>
              <AdvancedOptionsExpandable>
                <MetadataRefresh fields={fields} hideObjectNames/>
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
    ...props.initialValues
  };
  return {
    initialValues
  };
}

export default connectComplexForm({
  form: 'source',
  fields: ['config.path'],
  validate
}, SECTIONS, mapStateToProps, null)(NASForm);
