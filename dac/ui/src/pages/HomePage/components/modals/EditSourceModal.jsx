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
import { PureComponent } from "react";
import PropTypes from "prop-types";
import { injectIntl } from "react-intl";
import FormUnsavedWarningHOC from "components/Modals/FormUnsavedWarningHOC";
import EnginePreRequisiteHOC from "@inject/pages/HomePage/components/modals/PreviewEngineCheckHOC.tsx";
import { getEditSourceModalTitle } from "@inject/utils/sourceUtils";
import Modal from "components/Modals/Modal";
import EditSourceView from "./EditSourceView";

@injectIntl
export class EditSourceModal extends PureComponent {
  static contextTypes = {
    router: PropTypes.object.isRequired,
  };

  static propTypes = {
    isOpen: PropTypes.bool,
    hide: PropTypes.func,
    updateFormDirtyState: PropTypes.func,
    query: PropTypes.object,
    intl: PropTypes.object.isRequired,
  };

  hide = () => {
    this.props.hide();
  };

  render() {
    const { isOpen, query, hide, updateFormDirtyState } = this.props;

    //HOC Props
    const {
      iconDisabled,
      hideCancel,
      confirmText,
      canSubmit,
      hasError,
      isSubmitting,
      confirmButtonStyle,
      handleSubmit,
      setPreviewEngine,
      onDismissError,
    } = this.props;

    const addProps = {
      hideCancel,
      confirmText,
      canSubmit,
      hasError,
      isSubmitting,
      confirmButtonStyle,
      handleSubmit,
      setPreviewEngine,
      onDismissError,
    };

    const modalTitle = getEditSourceModalTitle(query.type, query.name);

    return (
      <Modal
        size="large"
        title={modalTitle}
        isOpen={isOpen}
        hide={!iconDisabled && hide}
        iconDisabled={iconDisabled}
      >
        {query.name && (
          <EditSourceView
            updateFormDirtyState={updateFormDirtyState}
            hide={!iconDisabled && this.hide}
            sourceName={query.name}
            sourceType={query.type}
            {...addProps}
          />
        )}
      </Modal>
    );
  }
}

export default EnginePreRequisiteHOC(FormUnsavedWarningHOC(EditSourceModal));
