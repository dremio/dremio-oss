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

import { browserHistory } from 'react-router';

export type DialogStatesType = {
  create: boolean;
  merge: boolean;
  rename: boolean;
  delete: boolean;
};

export const manageDialogs = (
  dialogStates: DialogStatesType,
  setDialogStates: React.Dispatch<React.SetStateAction<DialogStatesType>>,
  dialog: 'create' | 'merge' | 'rename' | 'delete',
  open: boolean
) => {
  switch (dialog) {
  case 'create':
    setDialogStates({ ...dialogStates, create: open });
    break;
  case 'merge':
    setDialogStates({ ...dialogStates, merge: open });
    break;
  case 'rename':
    setDialogStates({ ...dialogStates, rename: open });
    break;
  case 'delete':
    setDialogStates({ ...dialogStates, delete: open });
    break;
  default:
    return;
  }
};

export const redirectOnReferenceActions = (newName: string, baseUrl?: string) => {
  browserHistory.replace(`${baseUrl}/branches${newName ? '/' + newName : ''}`);
};
