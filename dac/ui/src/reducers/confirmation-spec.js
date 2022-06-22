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
  HIDE_CONFIRMATION_DIALOG,
  SHOW_CONFIRMATION_DIALOG,
} from "actions/confirmation";
import confirmationReducer from "./confirmation";

describe("confirmation reducer", () => {
  const initialState = {
    isOpen: false,
    title: null,
    confirmText: null,
    cancelText: null,
    text: null,
    confirm: null,
  };

  it("returns unaltered state by default", () => {
    const result = confirmationReducer(initialState, { type: "bla" });
    expect(result).to.equal(initialState);
  });

  describe("SHOW_CONFIRMATION_DIALOG", () => {
    it("should set required attributes to show dialog ", () => {
      const confirmFn = () => {};
      const cancelFn = () => {};
      const validateFn = () => {};
      const result = confirmationReducer(initialState, {
        type: SHOW_CONFIRMATION_DIALOG,
        text: "Confirmation text",
        title: "Confirm",
        confirmText: "Ok",
        cancelText: "Cancel",
        className: undefined,
        closeButtonType: undefined,
        confirm: confirmFn,
        cancel: cancelFn,
        style: {},
        doNotAskAgainKey: "Do not ask again key",
        doNotAskAgainText: "Do not ask again text",
        headerIcon: undefined,
        hideCancelButton: true,
        showOnlyConfirm: false,
        showPrompt: false,
        promptLabel: "Prompt Label",
        promptFieldProps: {},
        dataQa: "test",
        validatePromptText: validateFn,
        isCentered: true,
        size: "small",
      });
      expect(result).to.be.eql({
        isOpen: true,
        text: "Confirmation text",
        title: "Confirm",
        confirmText: "Ok",
        cancelText: "Cancel",
        className: undefined,
        closeButtonType: undefined,
        confirmButtonStyle: undefined,
        confirm: confirmFn,
        cancel: cancelFn,
        style: {},
        doNotAskAgainKey: "Do not ask again key",
        doNotAskAgainText: "Do not ask again text",
        headerIcon: undefined,
        hideCancelButton: true,
        showOnlyConfirm: false,
        showPrompt: false,
        promptLabel: "Prompt Label",
        promptFieldProps: {},
        dataQa: "test",
        validatePromptText: validateFn,
        isCentered: true,
        size: "small",
      });
    });
  });

  describe("HIDE_CONFIRMATION_DIALOG", () => {
    it("should reset state with default", () => {
      const result = confirmationReducer(initialState, {
        type: HIDE_CONFIRMATION_DIALOG,
      });
      expect(result).to.be.eql(initialState);
    });
  });
});
