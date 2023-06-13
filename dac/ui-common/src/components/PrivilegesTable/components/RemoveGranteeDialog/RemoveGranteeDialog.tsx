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

import { getIntlContext } from "../../../../contexts/IntlContext";
import {
  ModalContainer,
  DialogContent,
  Button as NewButton,
} from "dremio-ui-lib/components";

type RemoveGranteeDialogProps = {
  open: boolean;
  closeDialog: () => void;
  className: string;
  onRemove: (id: string) => void;
  grantee?: any;
};

const RemoveGranteeDialog = ({
  open,
  closeDialog,
  className,
  grantee,
  onRemove,
}: RemoveGranteeDialogProps) => {
  const { t } = getIntlContext();
  const handleRemoveOwner = () => {
    onRemove(grantee?.granteeId);
    closeDialog();
  };

  return (
    <ModalContainer open={() => {}} isOpen={open} close={closeDialog}>
      <DialogContent
        title={t("Admin.Privileges.RemoveGranteeTitle")}
        actions={
          <>
            <NewButton
              onClick={closeDialog}
              variant="secondary"
              className="margin-right"
            >
              {t("Common.Actions.Cancel")}
            </NewButton>
            <NewButton variant="primary" onClick={handleRemoveOwner}>
              {t("Common.Actions.Yes")}
            </NewButton>
          </>
        }
        className={className}
      >
        <span className="dremio-prose">
          {t("Admin.Privileges.RemoveGranteeContent", { name: grantee?.name })}
        </span>
      </DialogContent>
    </ModalContainer>
  );
};

export default RemoveGranteeDialog;
