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

import { useModalContainer, Button } from "dremio-ui-lib/components";
import { MessageDialog } from "dremio-ui-common/components/MessageDialog.js";
import MarkdownEditor from "@app/components/MarkdownEditor";
import ConfirmCancelFooter from "@app/components/Modals/ConfirmCancelFooter";
import ViewStateWrapper from "@app/components/ViewStateWrapper";
import { FormattedMessage, useIntl } from "react-intl";
//@ts-ignore
import ImmutablePropTypes from "react-immutable-proptypes";
import * as classes from "./WikiModal.module.less";
import { TutorialOutlet } from "@inject/tutorials/components/TutorialOutlet";
import { tutorialContext } from "dremio-ui-common/walkthrough/TutorialController";

interface WikiModalProps {
  isOpen: boolean;
  onChange: (val: string) => void;
  cancel: () => void;
  wikiValue: string;
  entityId: string;
  entityType: string;
  fullPath: any;
  isReadMode: boolean;
  save: (wikiValue: string) => void;
  wikiViewState: ImmutablePropTypes.map;
  wikiSummary: boolean;
  stay: () => void;
  leave: () => void;
  openChildModal: boolean;
}

const WikiModalView = ({
  isOpen,
  onChange,
  cancel,
  wikiValue,
  isReadMode,
  entityId,
  fullPath,
  entityType,
  save,
  wikiViewState,
  wikiSummary,
  stay,
  leave,
  openChildModal,
}: WikiModalProps) => {
  const modal = useModalContainer();
  const intl = useIntl();
  const onSave = () => {
    save(wikiValue);
  };

  const wrapperStylesFix = {
    flex: 1,
    height: "70vh",
    display: "flex",
    alignItems: "stretch",
  };

  return (
    <MessageDialog
      {...modal}
      isOpen={isOpen}
      close={cancel}
      title={intl.formatMessage({ id: "Common.Wiki" })}
      actions={
        !isReadMode && (
          <ConfirmCancelFooter
            className={classes["footer"]}
            cancelText={intl.formatMessage({ id: "Common.Cancel" })}
            cancel={cancel}
            confirm={onSave}
          />
        )
      }
    >
      <div className="modalBody">
        <div className={classes["content"]} data-qa="wikiModal">
          <ViewStateWrapper
            viewState={wikiViewState}
            hideChildrenWhenFailed={false}
            style={wrapperStylesFix}
          >
            <tutorialContext.Consumer>
              {({ setWikiEditor }) => (
                <MarkdownEditor
                  //@ts-ignore
                  showSummary={wikiSummary}
                  fullPath={fullPath}
                  value={wikiValue}
                  readMode={isReadMode}
                  entityId={entityId}
                  entityType={entityType}
                  onChange={onChange}
                  className={classes["editor"]}
                  isModal
                  fitToContainer
                  setTutorialWikiEditor={setWikiEditor}
                />
              )}
            </tutorialContext.Consumer>
          </ViewStateWrapper>
        </div>
        <MessageDialog
          isOpen={openChildModal}
          close={stay}
          actions={
            <>
              <Button variant="secondary" onClick={stay}>
                <FormattedMessage id="Common.Stay" />
              </Button>
              <Button variant="primary" onClick={leave}>
                <FormattedMessage id="Common.Leave" />
              </Button>
            </>
          }
          title={intl.formatMessage({ id: "Common.UnsavedWarning" })}
        >
          <div className={classes["childModalBody"]}>
            <FormattedMessage id="Common.ConfirmLeave" />
          </div>
        </MessageDialog>
      </div>
      {TutorialOutlet && <TutorialOutlet id="edit-wiki" />}
    </MessageDialog>
  );
};

export default WikiModalView;
