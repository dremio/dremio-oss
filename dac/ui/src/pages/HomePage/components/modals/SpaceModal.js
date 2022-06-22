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
import { injectIntl } from "react-intl";

import { getViewState } from "selectors/resources";
import { getSpaceVersion, getSpaceName, getSpace } from "@app/selectors/home";
import Modal from "components/Modals/Modal";
import FormUnsavedWarningHOC from "components/Modals/FormUnsavedWarningHOC";

import ApiUtils from "utils/apiUtils/apiUtils";
import {
  createNewSpace,
  updateSpace,
  updateSpacePrivileges,
} from "actions/resources/spaces";

import {
  getSpaceUpdated,
  getAdminStatus,
} from "dyn-load/pages/HomePage/components/modals/SpaceModalMixin";
import SpaceForm from "../forms/SpaceForm";
import "./Modal.less";

export const VIEW_ID = "SpaceModal";

const mapStateToProps = (state, { entityId }) => {
  return {
    spaceName: getSpaceName(state, entityId),
    spaceVersion: getSpaceVersion(state, entityId),
    viewState: getViewState(state, VIEW_ID),
    space: getSpace(state, entityId),
    isAdmin: getAdminStatus(state),
  };
};

@injectIntl
export class SpaceModal extends Component {
  static propTypes = {
    isOpen: PropTypes.bool,
    hide: PropTypes.func,
    entityId: PropTypes.string,

    //connected
    space: PropTypes.object,
    spaceName: PropTypes.string,
    spaceVersion: PropTypes.string,
    createNewSpace: PropTypes.func,
    updateSpace: PropTypes.func,
    updateSpacePrivileges: PropTypes.func,
    initialFormValues: PropTypes.object,
    updateFormDirtyState: PropTypes.func,
    intl: PropTypes.object.isRequired,
  };

  static contextTypes = {
    username: PropTypes.string,
  };

  hide = () => {
    this.props.hide();
  };

  submit = (values) => {
    return ApiUtils.attachFormSubmitHandlers(
      getSpaceUpdated(values, this.props)
    ).then(() => this.props.hide(null, true));
  };

  renderForm() {
    const {
      entityId,
      spaceName,
      spaceVersion,
      initialFormValues,
      updateFormDirtyState,
    } = this.props;

    return (
      <SpaceForm
        initialValues={
          entityId
            ? {
                name: spaceName,
                version: spaceVersion,
                id: entityId,
                ...initialFormValues,
              }
            : null
        }
        updateFormDirtyState={updateFormDirtyState}
        editing={entityId !== undefined}
        onFormSubmit={this.submit}
        onCancel={this.hide}
      />
    );
  }

  render() {
    const { isOpen, entityId, intl } = this.props;
    return (
      <Modal
        size="large"
        title={
          entityId
            ? intl.formatMessage({ id: "Space.EditSpace" })
            : intl.formatMessage({ id: "Space.AddSpace" })
        }
        isOpen={isOpen}
        hide={this.hide}
      >
        {this.renderForm()}
      </Modal>
    );
  }
}

export default connect(mapStateToProps, {
  createNewSpace,
  updateSpace,
  updateSpacePrivileges,
})(FormUnsavedWarningHOC(SpaceModal));
