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
  IconButton,
  CodeView,
  SyntaxHighlighter,
} from "dremio-ui-lib/components";
import { getIntlContext } from "dremio-ui-common/contexts/IntlContext.js";
import { CopyContainer } from "dremio-ui-lib/components";
import * as classes from "./SQLScriptDeletedDialog.module.less";
type SQLScriptDeletedDialogProps = {
  isOpen: boolean;
  onCancel: () => void;
  onConfirm: () => void;
  getSqlContent: () => string;
};
const SQLScriptDeletedDialog = (
  props: SQLScriptDeletedDialogProps,
): React.ReactElement => {
  const { t } = getIntlContext();
  const { isOpen, onCancel, onConfirm, getSqlContent } = props;
  const sqlContent = getSqlContent();

  return (
    <ModalContainer open={() => {}} isOpen={isOpen} close={() => {}}>
      <DialogContent
        icon={<dremio-icon name="interface/warning" alt="warning" />}
        title={t("Dialog.DeletedScript.Title")}
        className={classes["dialog"]}
        toolbar={
          <IconButton onClick={onCancel} aria-label="close dialog">
            <dremio-icon name="interface/close-big" alt="" />
          </IconButton>
        }
        actions={
          <>
            <Button variant="secondary" className="mr-05" onClick={onCancel}>
              {t("Dialog.DeletedScript.CancelAction")}
            </Button>
            <CopyContainer contents={sqlContent || ""} portal={false}>
              <Button variant="secondary" onClick={() => {}}>
                {t("Dialog.DeletedScript.CopyAction")}
              </Button>
            </CopyContainer>
            <Button variant="primary" onClick={onConfirm}>
              {t("Dialog.DeletedScript.SaveScriptAs")}
            </Button>
          </>
        }
      >
        <div className="mb-3">{t("Dialog.DeletedScript.Description")}</div>
        <CodeView title="" contentClass={classes["dialog__code"]}>
          <SyntaxHighlighter language="sql">{sqlContent}</SyntaxHighlighter>
        </CodeView>
      </DialogContent>
    </ModalContainer>
  );
};
export default SQLScriptDeletedDialog;
