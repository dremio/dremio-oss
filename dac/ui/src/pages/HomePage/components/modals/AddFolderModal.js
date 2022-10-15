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
import { compose } from "redux";
import { withRouter } from "react-router";
import PropTypes from "prop-types";
import { connect } from "react-redux";
import Immutable from "immutable";

import Modal from "components/Modals/Modal";

import ApiUtils from "utils/apiUtils/apiUtils";
import { addNewFolderForSpace } from "actions/resources/spaceDetails";
import { getRootEntityType } from "@app/utils/pathUtils";
import { intl } from "@app/utils/intl";

import AddFolderForm from "../forms/AddFolderForm";

import "./Modal.less";

export class AddFolderModal extends Component {
  static propTypes = {
    isOpen: PropTypes.bool,
    hide: PropTypes.func,

    //connected
    parentEntity: PropTypes.instanceOf(Immutable.Map),
    parentType: PropTypes.string,
    addNewFolderForSpace: PropTypes.func,
  };

  static contextTypes = {
    username: PropTypes.string,
  };

  constructor(props) {
    super(props);
    this.submit = this.submit.bind(this);
  }

  submit(values) {
    return ApiUtils.attachFormSubmitHandlers(
      this.props.addNewFolderForSpace(values.name)
    ).then(() => this.props.hide());
  }

  render() {
    const { isOpen, hide, parentType } = this.props;
    return (
      <Modal
        size="small"
        title={intl.formatMessage({ id: "Folder.AddFolder" })}
        isOpen={isOpen}
        hide={hide}
      >
        <AddFolderForm
          onFormSubmit={this.submit}
          onCancel={hide}
          parentType={parentType}
        />
      </Modal>
    );
  }
}

function mapStateToProps(state, props) {
  const parentType = getRootEntityType(props.location.pathname);
  return {
    parentType,
  };
}

export default compose(
  withRouter,
  connect(mapStateToProps, { addNewFolderForSpace })
)(AddFolderModal);
