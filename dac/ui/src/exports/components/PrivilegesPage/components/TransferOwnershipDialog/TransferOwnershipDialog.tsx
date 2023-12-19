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

import { useMemo } from "react";
import { getIntlContext } from "dremio-ui-common/contexts/IntlContext.js";
import {
  ModalContainer,
  DialogContent,
  Button,
} from "dremio-ui-lib/components";

type TransferOwnershipDialogProps = {
  open: boolean;
  closeDialog: () => void;
  className: string;
  pageType: "organization" | "catalog" | "entity";
  newOwner: any;
  onUpdate: (id: string) => void;
  entityName: string;
};

const TransferOwnershipDialog = ({
  open,
  closeDialog,
  className,
  newOwner,
  pageType,
  onUpdate,
  entityName,
}: TransferOwnershipDialogProps) => {
  const { t } = getIntlContext();
  const handleChangeOwner = async () => {
    if (newOwner) {
      onUpdate(newOwner?.id);
    }
    closeDialog();
  };
  const dialogDescription = useMemo(() => {
    switch (pageType) {
      case "organization":
        return t("Admin.Privileges.Organization.TransferOwnerContent");
      case "catalog":
        return t("Admin.Privileges.Catalog.TransferOwnerContent", {
          newOwner: newOwner?.name,
          entity: entityName,
        });
      case "entity":
        return t("Admin.Privileges.Entity.TransferOwnerContent", {
          newOwner: newOwner?.name,
          entity: entityName,
        });
    }
  }, [pageType, t, newOwner, entityName]);
  return (
    <ModalContainer open={() => {}} isOpen={open} close={closeDialog}>
      <DialogContent
        title={t("Admin.Privileges.TransferOwnerTitle")}
        actions={
          <>
            <Button
              onClick={closeDialog}
              variant="secondary"
              className="margin-right"
            >
              {t("Common.Actions.Cancel")}
            </Button>
            <Button variant="primary" onClick={handleChangeOwner}>
              {t("Common.Transfer")}
            </Button>
          </>
        }
        className={className}
      >
        <span className="dremio-prose">{dialogDescription}</span>
      </DialogContent>
    </ModalContainer>
  );
};

export default TransferOwnershipDialog;
