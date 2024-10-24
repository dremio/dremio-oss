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
import { connect } from "react-redux";

import Modal from "components/Modals/Modal";

import ApiUtils from "utils/apiUtils/apiUtils";
import { addNewFolderForSpace } from "actions/resources/spaceDetails";
import { intl } from "#oss/utils/intl";

import AddFolderForm from "../forms/AddFolderForm";

import "./Modal.less";

export class AddFolderModal extends Component {
  static propTypes = {
    isOpen: PropTypes.bool,
    hide: PropTypes.func,
    rootPath: PropTypes.string,
    rootName: PropTypes.string,

    //connected
    addNewFolderForSpace: PropTypes.func,
    disabled: PropTypes.bool,
  };

  submit = (values) => {
    return ApiUtils.attachFormSubmitHandlers(
      this.props.addNewFolderForSpace(
        values.name,
        this.props.rootPath,
        this.props.rootName,
      ),
    ).then(() => this.props.hide());
  };

  render() {
    const { isOpen, hide, disabled } = this.props;
    return (
      <Modal
        size="small"
        title={intl.formatMessage({ id: "Folder.AddFolder" })}
        isOpen={isOpen}
        hide={hide}
        style={{ height: 250, top: "30%" }}
        className="--newModalStyles"
      >
        <AddFolderForm
          onFormSubmit={this.submit}
          onCancel={hide}
          disabled={disabled}
        />
      </Modal>
    );
  }
}

export default connect(
  (state) => {
    return {
      disabled: !state.form.addFolder?.name?.value,
    };
  },
  { addNewFolderForSpace },
)(AddFolderModal);
