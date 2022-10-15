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
import { injectIntl } from "react-intl";

import AccountModalForm from "./AccountModalForm";
import Modal from "@app/components/Modals/Modal";
import FormUnsavedWarningHOC from "@app/components/Modals/FormUnsavedWarningHOC";

import "@app/pages/HomePage/components/modals/Modal.less";

type AccountModalProps = {
  intl: { formatMessage: any };
  updateFormDirtyState: () => void;
  isOpen: () => void;
  hide: () => void;
};

const AccountModal = (props: AccountModalProps) => {
  const {
    updateFormDirtyState,
    isOpen,
    hide,
    intl: { formatMessage },
  } = props;

  return (
    <Modal
      size="large"
      title={formatMessage({ id: "SideNav.AccountSettings" })}
      isOpen={isOpen}
      hide={hide}
    >
      <AccountModalForm
        updateFormDirtyState={updateFormDirtyState}
        cancelFunc={hide}
      />
    </Modal>
  );
};

export default injectIntl(FormUnsavedWarningHOC(AccountModal));
