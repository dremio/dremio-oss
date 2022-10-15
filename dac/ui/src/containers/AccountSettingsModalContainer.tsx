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

import { connect } from "react-redux";
import { getAccountSettingsModalState } from "@app/selectors/accountSettingsSelectors";
import { hideAccountSettingsModal as closeAccountSettingsModal } from "@app/actions/modals/accountSettingsActions";
import AccountSettingsModal from "dyn-load/pages/HomePage/components/modals/AccountModal/AccountModal";

type AccountSettingsModalContainerProps = {
  showAccountSettingsModal: boolean;
  closeAccountSettingsModal: () => void;
};

export const AccountSettingsModalContainer = (
  props: AccountSettingsModalContainerProps
) => {
  const { showAccountSettingsModal, closeAccountSettingsModal } = props;

  if (!showAccountSettingsModal) {
    return <></>;
  }

  return (
    <AccountSettingsModal
      isOpen={showAccountSettingsModal}
      hide={closeAccountSettingsModal}
      {...props}
    />
  );
};

const mapStateToProps = (state: any) => ({
  showAccountSettingsModal: getAccountSettingsModalState(state),
});

const mapDispatchToProps = {
  closeAccountSettingsModal,
};

export default connect(
  mapStateToProps,
  mapDispatchToProps
)(AccountSettingsModalContainer);
