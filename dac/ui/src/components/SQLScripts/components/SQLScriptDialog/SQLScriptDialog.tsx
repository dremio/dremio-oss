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

import * as React from "react";
import { injectIntl } from "react-intl";
import { connect } from "react-redux";
import Modal from "@app/components/Modals/Modal";
import ApiUtils from "utils/apiUtils/apiUtils";
import { addNotification } from "@app/actions/notification";
import SQLScriptForm from "./SQLScriptForm";

type SQLScriptDialogProps = {
  title: string;
  isOpen: boolean;
  script: any;
  intl: any;
  onCancel: () => void;
  onSubmit: (payload: any, scriptId?: string, hideFail?: boolean) => void;
  postSubmit: (payload?: any) => void;
  push?: (payload?: any) => void;
  hideFail: boolean;
  addNotification: any;
  saveAsDialogError: any;
  submit?: (payload: any) => void;
};

function SQLScriptDialog(props: SQLScriptDialogProps): React.ReactElement {
  const {
    title,
    isOpen,
    onCancel,
    push,
    script = {},
    onSubmit,
    postSubmit,
    intl,
    hideFail,
    saveAsDialogError,
    addNotification,
  } = props;
  const { content, context } = script;

  const onFormSubmit = (values: any) => {
    const payload: any = {
      name: values.name,
      content,
      context: context.toJS ? context.toJS() : context,
      description: "",
    };

    if (props.submit) {
      return ApiUtils.attachFormSubmitHandlers(props.submit(payload));
    }

    //Tabs: Remove below code after SQLRUNNER_TABS_UI is removed
    return ApiUtils.attachFormSubmitHandlers(
      onSubmit(payload, script.id, hideFail)
    ).then((res: any) => {
      if (push) {
        push(res.payload);
      }

      postSubmit(res.payload);
      addNotification(
        intl.formatMessage({ id: "NewQuery.ScriptSaved" }),
        "success"
      );
      onCancel();
      return;
    });
  };

  return (
    <Modal
      isOpen={isOpen}
      size="small"
      title={title}
      className="--newModalStyles"
      hide={onCancel}
      closeButtonType="CloseBig"
      modalHeight={"250px"}
    >
      <SQLScriptForm
        saveAsDialogError={saveAsDialogError}
        initialValues={script}
        onFormSubmit={onFormSubmit}
        intl={intl}
        onCancel={onCancel}
      />
    </Modal>
  );
}

export default connect(null, { addNotification })(injectIntl(SQLScriptDialog));
