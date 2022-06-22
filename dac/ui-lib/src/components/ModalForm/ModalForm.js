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
import PropTypes from "prop-types";

import { Formik, Form } from "formik";
import Dialog from "../Dialog";
import FormikUnsavedWarningHOC from "../FormikUnsavedWarningHOC";

const DialogWithUnsavedWarning = FormikUnsavedWarningHOC(Dialog);

const ModalForm = (props) => {
  const {
    // formik Props
    formikComponent,
    enableReinitialize,
    initialErrors,
    initialStatus,
    initialTouched,
    initialValues,
    isInitialValid,
    onReset,
    onSubmit,
    validate,
    validateOnBlur,
    validateOnChange,
    validateOnMount,
    validationSchema,

    // Dialog Props
    classes,
    onClose,
    onEnter,
    onEntered,
    onExit,
    onExited,
    onExiting,
    open,
    size,
    type,
    title,
    children: childComponent,
    endChildren,
    disableUnsavedWarning,
  } = props;

  const handleClose = ({ resetForm }) => {
    resetForm();
    onClose();
  };

  const DialogComp = disableUnsavedWarning ? Dialog : DialogWithUnsavedWarning;

  return (
    <Formik
      component={formikComponent}
      enableReinitialize={enableReinitialize}
      initialErrors={initialErrors}
      initialStatus={initialStatus}
      initialTouched={initialTouched}
      initialValues={initialValues}
      isInitialValid={isInitialValid}
      onReset={onReset}
      onSubmit={onSubmit}
      validate={validate}
      validateOnBlur={validateOnBlur}
      validateOnChange={validateOnChange}
      validateOnMount={validateOnMount}
      validationSchema={validationSchema}
    >
      {(formikProps) => (
        <Form>
          <DialogComp
            classes={classes}
            fullWidth
            onClose={() => handleClose(formikProps)}
            onEnter={onEnter}
            onEntered={onEntered}
            onExit={onExit}
            onExited={onExited}
            onExiting={onExiting}
            open={open}
            size={size}
            title={title}
            endChildren={endChildren}
            type={type}
          >
            {childComponent(formikProps)}
          </DialogComp>
        </Form>
      )}
    </Formik>
  );
};

ModalForm.propTypes = {
  formikComponent: PropTypes.any,
  enableReinitialize: PropTypes.bool,
  initialErrors: PropTypes.object,
  initialStatus: PropTypes.any,
  initialTouched: PropTypes.object,
  initialValues: PropTypes.object,
  isInitialValid: PropTypes.bool,
  onReset: PropTypes.func,
  onSubmit: PropTypes.func,
  validate: PropTypes.func,
  validateOnBlur: PropTypes.bool,
  validateOnChange: PropTypes.bool,
  validateOnMount: PropTypes.bool,
  validationSchema: PropTypes.object,

  // Dialog Props
  size: PropTypes.oneOf(["sm", "md", "lg", "xl"]),
  onClose: PropTypes.func,
  onEnter: PropTypes.func,
  onEntered: PropTypes.func,
  onExit: PropTypes.func,
  onExited: PropTypes.func,
  onExiting: PropTypes.func,
  open: PropTypes.bool.isRequired,
  children: PropTypes.oneOfType([PropTypes.node, PropTypes.func]),
  title: PropTypes.string,
  classes: PropTypes.object,

  disableUnsavedWarning: PropTypes.bool,
  endChildren: PropTypes.node,
  type: PropTypes.oneOf(["default", "info", "warning", "error"]),
};

ModalForm.defaultProps = {
  disableUnsavedWarning: false,
  size: "sm",
  classes: {},
  type: "default",
};

export default ModalForm;
