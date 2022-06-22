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

import React from "react";
import ReactDOM from "react-dom";
import DialogContent from "../Dialog/DialogContent";
import ModalForm from "../ModalForm";
import ModalFormAction from "../ModalForm/ModalFormAction";
import ModalFormActionContainer from "../ModalForm/ModalFormActionContainer";

import "./confirmationModal.scss";

const unMountComponent = () => {
  const container = document.querySelector(".conifrmation-container");
  if (container != null) {
    ReactDOM.unmountComponentAtNode(container);
  }
};

type ConfirmationModalProps = {
  element: Node;
  onClose: Function;
  submitFn: Function;
  modalType: string;
  title: string;
  hideCancelButton?: boolean;
  primaryButtonText?: string;
  cancelButtonText?: string;
};

export const ConfirmationModal = (props: ConfirmationModalProps) => {
  const {
    element,
    onClose,
    submitFn,
    modalType,
    title,
    hideCancelButton,
    cancelButtonText,
    primaryButtonText,
  } = props;

  const dialogContentClass = { root: "confirmationModal__content" };

  const handleClose = () => {
    onClose && onClose();
    unMountComponent();
  };

  const handleSubmit = () => {
    unMountComponent();
    submitFn();
  };

  return (
    <ModalForm
      open
      size="sm"
      type={modalType}
      onClose={handleClose}
      title={title}
      onSubmit={handleSubmit}
      disableUnsavedWarning
      classes={{
        paper: "confirmationModal",
        root: "confirmationModal__root",
        container: "confirmationModal__container",
      }}
    >
      {() => {
        return (
          <React.Fragment>
            <DialogContent classes={dialogContentClass}>
              {element}
            </DialogContent>
            <ModalFormActionContainer>
              <ModalFormAction
                direction="right"
                onClick={handleClose}
                color={hideCancelButton ? "primary" : "default"}
              >
                {cancelButtonText}
              </ModalFormAction>
              {!hideCancelButton && (
                <ModalFormAction color="primary" onClick={handleSubmit}>
                  {primaryButtonText}
                </ModalFormAction>
              )}
            </ModalFormActionContainer>
          </React.Fragment>
        );
      }}
    </ModalForm>
  );
};

ConfirmationModal.defaultProps = {
  cancelButtonText: "Cancel",
  primaryButtonText: "Ok",
  hideCancelButton: false,
};

export default function openConfirmationModal(
  renderProps: ConfirmationModalProps
): void {
  const confirmationModal = <ConfirmationModal {...renderProps} />;
  ReactDOM.render(
    confirmationModal,
    document.querySelector(".conifrmation-container")
  );
}
