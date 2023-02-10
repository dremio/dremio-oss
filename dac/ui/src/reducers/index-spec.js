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
import { LOGOUT_USER_START } from "actions/account";

import localStorageUtils from "utils/storageUtils/localStorageUtils";
import socket from "utils/socket";

import rootReducer from "./index";

describe("rootReducer", () => {
  it("returns initial state from app reducers", () => {
    const result = rootReducer(undefined, { type: "bla" });
    expect(result.confirmation.isOpen).to.be.false;
  });

  describe("LOGOUT_USER_START", () => {
    it("should reset everything except routing", () => {
      const initialState = rootReducer(undefined, {
        type: "bla",
      });
      initialState.confirmation = {
        ...initialState.confirmation,
        isOpen: true,
      };
      initialState.routing.someAttribute = true;

      const result = rootReducer(initialState, {
        type: LOGOUT_USER_START,
      });
      expect(result.confirmation.isOpen).to.be.false;
      expect(result.routing.someAttribute).to.be.true;
    });
  });

  describe("user setup", () => {
    beforeEach(() => {
      sinon.stub(localStorageUtils, "setUserData");
      sinon.stub(socket, "open");
    });
    afterEach(() => {
      localStorageUtils.setUserData.restore();
      socket.open.restore();
    });
  });
});
