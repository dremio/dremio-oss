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

import { ModalForm, FormBody, modalFormProps } from 'components/Forms';
import General from 'components/Forms/General';
import { connectComplexForm } from 'components/Forms/connectComplexForm';

const SECTIONS = [General];

export class SpaceForm extends Component {

  static propTypes = {
    onFormSubmit: PropTypes.func.isRequired,
    onCancel: PropTypes.func.isRequired,
    handleSubmit: PropTypes.func.isRequired,
    editing: PropTypes.bool,
    fields: PropTypes.object,
    dirty: PropTypes.bool,
    updateFormDirtyState: PropTypes.func,
    values: PropTypes.object
  };

  render() {
    const {fields, handleSubmit, onFormSubmit, editing} = this.props;
    const description = la('Spaces are how you organize datasets in Dremio.' +
      ' They can contain physical datasets, virtual datasets, files, and folders.' +
      ' Datasets and files can belong to more than one Space.');
    return (
      <ModalForm {...modalFormProps(this.props)} onSubmit={handleSubmit(onFormSubmit)}>
        <FormBody>
          <General
            showAccelerationSection={false}
            fields={fields}
            editing={editing}
            sectionDescription={la(description)}/>
        </FormBody>
      </ModalForm>
    );
  }
}

export default connectComplexForm({
  form: 'space',
  fields: ['version']
}, SECTIONS, null, {})(SpaceForm);
