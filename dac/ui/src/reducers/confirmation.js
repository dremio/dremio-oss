/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
import * as ActionTypes from 'actions/confirmation';

const initialState = {
  isOpen: false,
  title: null,
  confirmText: null,
  text: null,
  confirm: null
};

export default function confirmation(state = initialState, action) {
  switch (action.type) {
  // todo: add queuing
  case ActionTypes.SHOW_CONFIRMATION_DIALOG: {
    const {
      text,
      title,
      confirm,
      cancel,
      confirmText,
      cancelText,
      hideCancelButton,
      showOnlyConfirm,
      doNotAskAgainText,
      doNotAskAgainKey,
      showPrompt,
      promptFieldProps,
      dataQa
    } = action;
    // list all to be sure to reset everything
    return {
      ...state,
      isOpen: true,
      text,
      title,
      confirmText,
      cancelText,
      confirm,
      cancel,
      hideCancelButton,
      showOnlyConfirm,
      doNotAskAgainText,
      doNotAskAgainKey,
      showPrompt,
      promptFieldProps,
      dataQa
    };
  }
  case ActionTypes.HIDE_CONFIRMATION_DIALOG:
    return {
      ...state,
      ...initialState
    };
  default:
    return state;
  }
}
