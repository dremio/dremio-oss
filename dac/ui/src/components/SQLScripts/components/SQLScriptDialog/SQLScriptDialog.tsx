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

import React from 'react';
import { injectIntl } from 'react-intl';
import Modal from '@app/components/Modals/Modal';
import ApiUtils from 'utils/apiUtils/apiUtils';
import openPopupNotification from '@app/components/PopupNotification/PopupNotification';
import SQLScriptForm from './SQLScriptForm';

type SQLScriptDialogProps = {
  title: string;
  isOpen: boolean;
  script: any;
  intl: any;
  onCancel: () => void;
  onSubmit: (payload: any, scriptId?: string, hideFail?: boolean) => void;
  postSubmit: (payload?: any) => void;
  push?: () => void;
  hideFail: boolean;
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
    hideFail
  } = props;
  const { content, context } = script;

  const onFormSubmit = (values: any): void => {
    const newContext = context.toJS ? context.toJS() : context;
    const payload: any = { name: values.name, content, context: newContext, description: '' };
    return ApiUtils.attachFormSubmitHandlers(
      onSubmit(payload, script.id, hideFail)
    ).then((res: any) => {
      if (push) {
        push();
      }

      postSubmit(res.payload);
      openPopupNotification({ message: intl.formatMessage({ id: 'NewQuery.ScriptSaved' }), type: 'success' });
      onCancel();
    });
  };

  return (
    <Modal
      isOpen={isOpen}
      size='small'
      title={title}
      className='--newModalStyles'
      hide={onCancel}
      closeButtonType='XBig'
      modalHeight={'250px'}
    >
      <SQLScriptForm initialValues={script} onFormSubmit={onFormSubmit} intl={intl} />
    </Modal>
  );
}

export default injectIntl(SQLScriptDialog);
