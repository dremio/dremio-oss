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
import path from 'path';
import { Component } from 'react';

import PropTypes from 'prop-types';

import { connectComplexForm } from 'components/Forms/connectComplexForm';
import { ModalForm, FormBody, modalFormProps } from 'components/Forms';
import { FieldWithError, TextField, FileField } from 'components/Fields';
import { applyValidators, isRequired } from 'utils/validation';
import { injectIntl } from 'react-intl';

const FIELDS = ['file', 'name', 'extension'];

import { formRow } from 'uiTheme/radium/forms';

function validate(values) {
  return applyValidators(values, [isRequired('file'), isRequired('name')]);
}

@injectIntl
export class AddFileFormPage1 extends Component {
  static propTypes = {
    onFormSubmit: PropTypes.func.isRequired,
    onCancel: PropTypes.func.isRequired,
    fields: PropTypes.object,
    handleSubmit: PropTypes.func.isRequired,
    intl: PropTypes.object.isRequired
  };

  state = {
    isUploading: false
  };

  onFileChange = (file, isUploading) => {
    const { fields } = this.props;

    const filename = file.name;
    const extName = path.extname(filename);
    fields.name.onChange(path.basename(filename, extName));
    fields.extension.onChange(extName.slice(1));
    fields.file.onChange(file);
    this.setState({
      isUploading
    });
  };

  render() {
    const { fields, handleSubmit, onFormSubmit, intl } = this.props;
    const confirmText = this.state.isUploading
      ? intl.formatMessage({ id: 'File.Uploading' })
      : intl.formatMessage({ id: 'Common.Next' });

    return (
      <ModalForm
        {...modalFormProps(this.props)}
        canSubmit={!this.state.isUploading}
        confirmText={confirmText}
        onSubmit={handleSubmit(onFormSubmit)}
      >
        <FormBody style={{height: '100%'}}>
          <FileField accept='multipart/form-data' {...fields.file} onChange={this.onFileChange}/>
          <FieldWithError
            label={intl.formatMessage({ id: 'Common.Name' })}
            {...fields.name} style={[formRow]}
            errorPlacement='top'
          >
            <TextField accept='multipart/form-data' {...fields.name}/>
          </FieldWithError>
        </FormBody>
      </ModalForm>
    );
  }
}

export default connectComplexForm({
  form: 'addFile',
  fields: FIELDS,
  validate,
  destroyOnUnmount: false
})(AddFileFormPage1);
