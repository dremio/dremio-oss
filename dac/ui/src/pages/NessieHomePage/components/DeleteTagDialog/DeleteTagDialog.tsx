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
import { browserHistory } from "react-router";
import { useDispatch } from "react-redux";
import { FormattedMessage, useIntl } from "react-intl";
import {
  Button,
  ModalContainer,
  DialogContent,
} from "dremio-ui-lib/components";
import { Reference } from "#oss/types/nessie";
import { useNessieContext } from "../../utils/context";
import { addNotification } from "actions/notification";
import { setReference } from "#oss/actions/nessie/nessie";
import { ReferenceType } from "#oss/services/nessie/client/index";
import {
  constructVersionedEntityUrl,
  useVersionedPageContext,
} from "#oss/exports/pages/VersionedHomePage/versioned-page-utils";
import * as classes from "./DeleteTagDialog.module.less";

type DeleteTagDialogProps = {
  open: boolean;
  closeDialog: () => void;
  forkFrom: Reference;
  refetch?: () => void;
};

function DeleteTagDialog({
  open,
  closeDialog,
  forkFrom,
  refetch,
}: DeleteTagDialogProps): JSX.Element {
  const {
    baseUrl,
    apiV2,
    stateKey,
    state: { reference, defaultReference },
  } = useNessieContext();
  const { isCatalog } = useVersionedPageContext();
  const intl = useIntl();
  const dispatch = useDispatch();

  const onCancel = () => {
    closeDialog();
  };

  const onDelete = async () => {
    try {
      await apiV2.deleteReferenceV2({
        ref: forkFrom.hash
          ? `${forkFrom.name}@${forkFrom.hash}`
          : forkFrom.name,
        type: ReferenceType.Tag,
      });
      dispatch(
        addNotification(
          intl.formatMessage(
            { id: "VersionedEntity.Tags.DeleteSuccess" },
            { tag: forkFrom.name },
          ),
          "success",
        ),
      );
      if (reference && forkFrom.name === reference.name) {
        dispatch(setReference({ reference: defaultReference }, stateKey));
        browserHistory.push(
          constructVersionedEntityUrl({
            type: isCatalog ? "catalog" : "source",
            baseUrl,
            tab: "data",
            namespace: encodeURIComponent(defaultReference.name),
          }),
        );
      }
      refetch?.();
      closeDialog();
    } catch (error: any) {
      const errorMessage = await error.json();
      dispatch(addNotification(errorMessage.message, "error"));
      closeDialog();
    }
  };

  return (
    <ModalContainer open={() => {}} isOpen={open} close={closeDialog}>
      <DialogContent
        className={classes["delete-tag-dialog"]}
        title={intl.formatMessage({
          id: "VersionedEntity.Tags.DeleteTag",
        })}
        actions={
          <>
            <Button variant="secondary" onClick={onCancel}>
              <FormattedMessage id="Common.Cancel" />
            </Button>
            <Button variant="primary-danger" onClick={onDelete}>
              <FormattedMessage id="Common.Delete" />
            </Button>
          </>
        }
      >
        <div className={classes["delete-tag-dialog-body"]}>
          <FormattedMessage
            id="VersionedEntity.Tags.DeleteTagConfirm"
            values={{ tag: forkFrom.name }}
          />
        </div>
      </DialogContent>
    </ModalContainer>
  );
}
export default DeleteTagDialog;
