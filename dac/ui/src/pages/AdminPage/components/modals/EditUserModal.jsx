/*
 * Copyright (C) 2017-2019 Dremio Corporation
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
import { Component } from "react";
import PropTypes from "prop-types";

import Modal from "components/Modals/Modal";
import ApiUtils from "utils/apiUtils/apiUtils";
import FormUnsavedWarningHOC from "components/Modals/FormUnsavedWarningHOC";

import EditUserForm from "../forms/EditUserForm";
import "./Modal.less";

export class EditUserModal extends Component {
  static propTypes = {
    userId: PropTypes.string,
    isOpen: PropTypes.bool,
    hide: PropTypes.func,
    loadUser: PropTypes.func,
    editUser: PropTypes.func,
    query: PropTypes.object,
    updateFormDirtyState: PropTypes.func,
  };

  submit = (submitPromise) => {
    return ApiUtils.attachFormSubmitHandlers(submitPromise).then(() => {
      this.onHide(null, true);
      return null;
    });
  };

  onHide = (...args) => {
    const { hide } = this.props;
    hide(...args);
  };

  render() {
    const { userId, isOpen, updateFormDirtyState } = this.props;
    return (
      <Modal
        title={userId ? laDeprecated("Edit User") : laDeprecated("Add User")}
        size="small"
        isOpen={isOpen}
        classQa="add-user-modal"
        hide={this.onHide}
      >
        <EditUserForm
          userId={userId}
          updateFormDirtyState={updateFormDirtyState}
          onFormSubmit={this.submit}
          onCancel={this.onHide}
          passwordHasPadding
          isModal
        />
      </Modal>
    );
  }
}

export default FormUnsavedWarningHOC(EditUserModal);
