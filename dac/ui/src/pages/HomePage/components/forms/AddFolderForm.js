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
import { injectIntl } from 'react-intl';

import { connectComplexForm } from 'components/Forms/connectComplexForm';
import { ModalForm, FormBody, modalFormProps } from 'components/Forms';

import { FieldWithError, TextField } from 'components/Fields';
import { applyValidators, isRequired } from 'utils/validation';
import { sectionTitle, formRow, description } from 'uiTheme/radium/forms';

const FIELDS = ['name'];

function validate(values, props) {
  const { intl } = props;
  return applyValidators(values, [isRequired('name', intl.formatMessage({ id: 'Folder.Name' }))]);
}

export class AddFolderForm extends Component {

  static propTypes = {
    onFormSubmit: PropTypes.func.isRequired,
    onCancel: PropTypes.func.isRequired,
    fields: PropTypes.object,
    handleSubmit: PropTypes.func.isRequired,
    intl: PropTypes.object.isRequired
  };

  render() {
    const {fields, handleSubmit, onFormSubmit, intl} = this.props;
    const sectionDescription = intl.formatMessage({ id: 'Folder.AddFileModalDescription' });
    const secTitle = intl.formatMessage({ id: 'Folder.AddFolderToSpace' });
    return (
      <ModalForm {...modalFormProps(this.props)} onSubmit={handleSubmit(onFormSubmit)}>
        <FormBody>
          <h2 style={sectionTitle}>{secTitle}</h2>
          <div style={description}>{sectionDescription}</div>
          <div style={formRow}>
            <FieldWithError label={intl.formatMessage({ id: 'Folder.Name' })} errorPlacement='right' {...fields.name}>
              <TextField initialFocus {...fields.name}/>
            </FieldWithError>
          </div>
        </FormBody>
      </ModalForm>
    );
  }
}

export default injectIntl(connectComplexForm({
  form: 'addFolder',
  fields: FIELDS,
  validate
})(AddFolderForm));
