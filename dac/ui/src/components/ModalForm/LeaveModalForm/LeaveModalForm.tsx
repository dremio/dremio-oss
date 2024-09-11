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

import {
  ModalContainer,
  DialogContent,
  Button,
} from "dremio-ui-lib/components";

import { getIntlContext } from "dremio-ui-common/contexts/IntlContext.js";

type LeaveModalFormProps = {
  isOpen: boolean;
  onCancel: () => void;
  onConfirm: () => void;
};
const LeaveModalForm = (props: LeaveModalFormProps): React.ReactElement => {
  const { t } = getIntlContext();
  const { isOpen, onCancel, onConfirm } = props;
  const onContinue = () => {
    onConfirm();
    onCancel();
  };
  return (
    <ModalContainer open={() => {}} isOpen={isOpen} close={() => {}}>
      <DialogContent
        icon={<dremio-icon name="interface/warning" alt="warning" />}
        title={t("Common.RouteLeaveDialog.Title")}
        toolbar={
          <dremio-icon
            name="interface/close-big"
            alt=""
            style={{ cursor: "pointer" }}
            onClick={onCancel}
          />
        }
        actions={
          <>
            <Button variant="secondary" className="mr-05" onClick={onCancel}>
              {t("Common.RouteLeaveDialog.Actions.Stay")}
            </Button>
            <Button variant="primary" onClick={onContinue}>
              {t("Common.RouteLeaveDialog.Actions.Leave")}
            </Button>
          </>
        }
      >
        <p style={{ height: 100, display: "flex", alignItems: "center" }}>
          {t("Common.RouteLeaveDialog.Message")}
        </p>
      </DialogContent>
    </ModalContainer>
  );
};
export default LeaveModalForm;
