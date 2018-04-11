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
import Radium from 'radium';

import PropTypes from 'prop-types';

import { connectComplexForm } from 'components/Forms/connectComplexForm';
import { ModalForm, modalFormProps } from 'components/Forms';

import UserForm, { userFormFields, userFormValidate } from 'components/Forms/UserForm';

@Radium
export class AddUserForm extends Component {

  static propTypes = {
    onCancel: PropTypes.func.isRequired,
    onFormSubmit: PropTypes.func.isRequired,
    handleSubmit: PropTypes.func.isRequired,
    updateFormDirtyState: PropTypes.func,
    fields: PropTypes.object
  };

  render() {
    const { fields, handleSubmit, onFormSubmit } = this.props;
    return (
      <ModalForm {...modalFormProps(this.props)} onSubmit={handleSubmit(onFormSubmit)}>
        <UserForm
          fields={fields}
          passwordHolderStyles={styles.passwordHolder}
        />
      </ModalForm>
    );
  }
}

export default connectComplexForm({
  form: 'addUser',
  fields: userFormFields,
  validate: userFormValidate
})(AddUserForm);

const styles = {
  passwordHolder: {
    margin: '40px 0 0'
  }
};
