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
import path from 'path';
import { Component, PropTypes } from 'react';
import Immutable from 'immutable';

import { connectComplexForm } from 'components/Forms/connectComplexForm';
import { ModalForm, FormBody, modalFormProps } from 'components/Forms';
import { FieldWithError, TextField, FileField } from 'components/Fields';
import { applyValidators, isRequired } from 'utils/validation';

import ViewStateWrapper from 'components/ViewStateWrapper';

const FIELDS = ['file', 'name', 'extension'];

import { formRow } from 'uiTheme/radium/forms';
import { h5 } from 'uiTheme/radium/typography';

function validate(values) {
  return applyValidators(values, [isRequired('file'), isRequired('name')]);
}

export class AddFileFormPage1 extends Component {
  static propTypes = {
    onFormSubmit: PropTypes.func.isRequired,
    onCancel: PropTypes.func.isRequired,
    fields: PropTypes.object,
    handleSubmit: PropTypes.func.isRequired,
    viewState: PropTypes.instanceOf(Immutable.Map)
  };

  constructor(props) {
    super(props);
    this.onFileChange = this.onFileChange.bind(this);
  }

  onFileChange(file) {
    const { fields } = this.props;

    const filename = file.name;
    const extName = path.extname(filename);
    fields.name.onChange(path.basename(filename, extName));
    fields.extension.onChange(extName.slice(1));
    fields.file.onChange(file);
  }

  render() {
    const {fields, handleSubmit, onFormSubmit, viewState} = this.props;

    return (
      <ModalForm {...modalFormProps(this.props)} confirmText='Next' onSubmit={handleSubmit(onFormSubmit)}>
        <ViewStateWrapper viewState={viewState} hideChildrenWhenInProgress hideChildrenWhenFailed={false}>
          <FormBody>
            <FileField accept='multipart/form-data' {...fields.file} onChange={this.onFileChange}/>
            <FieldWithError label='Name' {...fields.name} style={[formRow, h5]}>
              <TextField accept='multipart/form-data' {...fields.name}/>
            </FieldWithError>
          </FormBody>
        </ViewStateWrapper>
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
