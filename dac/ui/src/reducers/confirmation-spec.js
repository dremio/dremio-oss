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
import { HIDE_CONFIRMATION_DIALOG, SHOW_CONFIRMATION_DIALOG } from 'actions/confirmation';
import confirmationReducer from './confirmation';

describe('confirmation reducer', () => {
  const initialState = {
    isOpen: false,
    title: null,
    confirmText: null,
    cancelText: null,
    text: null,
    confirm: null
  };

  it('returns unaltered state by default', () => {
    const result = confirmationReducer(initialState, {type: 'bla'});
    expect(result).to.equal(initialState);
  });

  describe('SHOW_CONFIRMATION_DIALOG', () => {
    it('should set required attributes to show dialog ', () => {
      const confirmFn = () => {};
      const cancelFn = () => {};
      const result = confirmationReducer(initialState, {
        type: SHOW_CONFIRMATION_DIALOG,
        text: 'Confirmation text',
        title: 'Confirm',
        confirmText: 'Ok',
        cancelText: 'Cancel',
        confirm: confirmFn,
        cancel: cancelFn,
        doNotAskAgainKey: 'Do not ask again key',
        doNotAskAgainText: 'Do not ask again text',
        hideCancelButton: true,
        showOnlyConfirm: false,
        showPrompt: false,
        promptFieldProps: {},
        dataQa: 'test'
      });
      expect(result).to.be.eql({
        isOpen: true,
        text: 'Confirmation text',
        title: 'Confirm',
        confirmText: 'Ok',
        cancelText: 'Cancel',
        confirm: confirmFn,
        cancel: cancelFn,
        doNotAskAgainKey: 'Do not ask again key',
        doNotAskAgainText: 'Do not ask again text',
        hideCancelButton: true,
        showOnlyConfirm: false,
        showPrompt: false,
        promptFieldProps: {},
        dataQa: 'test'
      });
    });
  });

  describe('HIDE_CONFIRMATION_DIALOG', () => {
    it('should reset state with default', () => {
      const result = confirmationReducer(initialState, {
        type: HIDE_CONFIRMATION_DIALOG
      });
      expect(result).to.be.eql(initialState);
    });
  });
});
