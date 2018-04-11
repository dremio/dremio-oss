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
import { connect } from 'react-redux';

import Modal from 'components/Modals/Modal';
import { createNewUser } from 'actions/admin';
import ApiUtils from 'utils/apiUtils/apiUtils';
import FormUnsavedWarningHOC from 'components/Modals/FormUnsavedWarningHOC';

import AddUserForm from '../forms/AddUserForm';

import './Modal.less';

export class AddUserModal extends Component {
  static propTypes = {
    isOpen: PropTypes.bool,
    hide: PropTypes.func,
    createNewUser: PropTypes.func,
    pathname: PropTypes.string,
    updateFormDirtyState: PropTypes.func,
    query: PropTypes.object
  }

  submit = (values) => {
    const mappedValues = {
      'userName' : values.userName,
      'firstName' : values.firstName,
      'lastName' : values.lastName,
      'email' : values.email,
      'createdAt' : new Date().getTime(),
      'password': values.password
    };
    return ApiUtils.attachFormSubmitHandlers(
      this.props.createNewUser(mappedValues)
    ).then(() => this.props.hide(null, true));
  }

  render() {
    const { isOpen, hide, updateFormDirtyState } = this.props;
    return (
      <Modal
        title={la('Add User')}
        size='small'
        isOpen={isOpen}
        classQa='add-user-modal'
        hide={hide}>
        <AddUserForm
          updateFormDirtyState={updateFormDirtyState}
          onFormSubmit={this.submit}
          onCancel={hide} />
      </Modal>
    );
  }
}

export default connect(null, {
  createNewUser
})(FormUnsavedWarningHOC(AddUserModal));
