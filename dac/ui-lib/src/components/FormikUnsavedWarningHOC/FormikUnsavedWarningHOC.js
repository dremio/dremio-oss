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
import React, { Fragment, useState } from "react";
import PropTypes from "prop-types";

import { useFormikContext } from "formik";

import { FormattedMessage } from "react-intl";

import Dialog from "../Dialog";
import DialogTitle from "../Dialog/DialogTitle";
import DialogContent from "../Dialog/DialogContent";
import ModalFormActionContainer from "../ModalForm/ModalFormActionContainer";
import ModalFormAction, {
  MODAL_FORM_ACTION_DIRECTION,
} from "../ModalForm/ModalFormAction";

import * as ButtonTypes from "../Button/ButtonTypes";

const FormikUnsavedWarningHOC = (WrappedComponent) => (props) => {
  const {
    onClose: parentOnClose, // eslint-disable-line react/prop-types
    ...otherProps
  } = props;

  const { dirty = false } = useFormikContext() || {};
  const [showDialog, setShowDialog] = useState(dirty);

  const onClose = (...args) => {
    if (dirty) {
      setShowDialog(true);
    } else {
      parentOnClose(...args);
    }
  };

  const handleStay = () => {
    setShowDialog(false);
  };

  const handleLeave = () => {
    setShowDialog(false);
    parentOnClose();
  };

  return (
    <Fragment>
      <Dialog onClose={handleStay} open={showDialog} size="sm" isCentered>
        <DialogTitle onClose={handleStay}>
          <FormattedMessage
            id="common.unsavedChangesTitle"
            defaultMessage="Unsaved Changes"
          />
        </DialogTitle>
        <DialogContent centerContent>
          <FormattedMessage
            id="common.unsavedChangesMessage"
            defaultMessage="You have unsaved changes. Are you sure you want to leave?"
          />
        </DialogContent>
        <ModalFormActionContainer>
          <ModalFormAction
            onClick={handleStay}
            color={ButtonTypes.DEFAULT}
            direction={MODAL_FORM_ACTION_DIRECTION.RIGHT}
          >
            <FormattedMessage id="common.stay" defaultMessage="Stay" />
          </ModalFormAction>
          <ModalFormAction onClick={handleLeave} color={ButtonTypes.PRIMARY}>
            <FormattedMessage id="common.leave" defaultMessage="Leave" />
          </ModalFormAction>
        </ModalFormActionContainer>
      </Dialog>
      <WrappedComponent {...otherProps} onClose={onClose} />
    </Fragment>
  );
};

FormikUnsavedWarningHOC.propTypes = {
  onClose: PropTypes.func,
};

export default FormikUnsavedWarningHOC;
