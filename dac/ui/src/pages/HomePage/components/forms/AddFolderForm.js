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

import { connectComplexForm } from 'components/Forms/connectComplexForm';
import { ModalForm, FormBody, modalFormProps } from 'components/Forms';
import { FieldWithError, TextField } from 'components/Fields';
import { applyValidators, isRequired } from 'utils/validation';
import { sectionTitle, formRow, description } from 'uiTheme/radium/forms';

const FIELDS = ['name'];

function validate(values) {
  return applyValidators(values, [isRequired('name', la('Folder Name'))]);
}

export class AddFolderForm extends Component {

  static propTypes = {
    onFormSubmit: PropTypes.func.isRequired,
    onCancel: PropTypes.func.isRequired,
    fields: PropTypes.object,
    handleSubmit: PropTypes.func.isRequired
  };

  constructor(props) {
    super(props);
  }

  render() {
    const {fields, handleSubmit, onFormSubmit} = this.props;
    const sectionDescription = la('Dremio folders work just like your desktop ones;' +
      ' they add another layer of organization within a Space.');
    const secTitle = la('Add a Folder to the Space');
    return (
      <ModalForm {...modalFormProps(this.props)} onSubmit={handleSubmit(onFormSubmit)}>
        <FormBody>
          <h3 style={sectionTitle}>{secTitle}</h3>
          <div style={description}>{sectionDescription}</div>
          <div style={formRow}>
            <FieldWithError label={la('Folder Name')} errorPlacement='right' {...fields.name}>
              <TextField initialFocus {...fields.name}/>
            </FieldWithError>
          </div>
        </FormBody>
      </ModalForm>
    );
  }
}

export default connectComplexForm({
  form: 'addFolder',
  fields: FIELDS,
  validate
})(AddFolderForm);
