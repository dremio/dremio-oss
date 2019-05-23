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
    values: PropTypes.object,
    intl: PropTypes.object.isRequired
  };

  render() {
    const {fields, handleSubmit, onFormSubmit, editing, intl} = this.props;
    const description = intl.formatMessage({ id: 'Space.AddSpaceModalDescription' });
    return (
      <ModalForm {...modalFormProps(this.props)} onSubmit={handleSubmit(onFormSubmit)}>
        <FormBody style={{padding: '20px 15px'}}>
          <General
            showAccelerationSection={false}
            fields={fields}
            editing={editing}
            sectionDescription={description}
          />
        </FormBody>
      </ModalForm>
    );
  }
}

export default injectIntl(connectComplexForm({
  form: 'space',
  fields: ['version']
}, SECTIONS, null, {})(SpaceForm));
